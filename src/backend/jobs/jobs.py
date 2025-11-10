import requests

from celery import states
from django.utils import timezone

from kantele import settings
from jobs.models import Task, Job, JobError
from rawstatus.models import FileJob, RawFile, StoredFile, StoredFileLoc, DataSecurityClass, ShareFunction, ServerShare, FileserverShare, FileServer
from datasets import models as dm


class Jobstates:
    # FIXME make in DB integer choices
    WAITING = 'wait' # jobs in wait can be skipped
    HOLD = 'hold' # jobs in hold block other jobs
    PENDING = 'pending'
    QUEUED = 'queued' # Not currently used yet
    PROCESSING = 'processing'
    ERROR = 'error'
    DONE = 'done'
    REVOKING = 'revoking'
    CANCELED = 'canceled'


JOBSTATES_ALL = [Jobstates.WAITING, Jobstates.HOLD, Jobstates.ERROR, Jobstates.CANCELED, Jobstates.PENDING,
        Jobstates.QUEUED, Jobstates.PROCESSING, Jobstates.REVOKING, Jobstates.DONE
        ]

JOBSTATES_DONE = [Jobstates.DONE, Jobstates.CANCELED]

# Inactive jobs:
# - can (re)run analysis
JOBSTATES_JOB_INACTIVE = [Jobstates.WAITING, Jobstates.HOLD, Jobstates.ERROR, Jobstates.CANCELED]

# Active jobs
# - analysis can be stopped
JOBSTATES_JOB_ACTIVE = [Jobstates.PENDING, Jobstates.QUEUED, Jobstates.PROCESSING]

# Tasks not sent:
# - Can be canceled without revoking task
# - Cannot edit analysis
JOBSTATES_JOB_NOT_SENT = [Jobstates.WAITING, Jobstates.HOLD, Jobstates.ERROR, Jobstates.CANCELED, Jobstates.PENDING]

# Tasks not sent but job still not cancel
# - can be paused
JOBSTATES_PAUSABLE = [Jobstates.HOLD, Jobstates.ERROR, Jobstates.PENDING]

# Tasks have been sent
# - Canceling needs revoking
# - revoking (jobs/views) at all gets check
JOBSTATES_JOB_SENT = [Jobstates.QUEUED, Jobstates.PROCESSING, Jobstates.REVOKING]

# Tasks picked up
# - Nextflow can log to this analysis
JOBSTATES_TASKS_RUNNING = [Jobstates.PROCESSING, Jobstates.REVOKING]

# - showing jobs
# - determining if file is being backed up
JOBSTATES_WAIT = [Jobstates.WAITING, Jobstates.PENDING, Jobstates.QUEUED, Jobstates.PROCESSING, Jobstates.HOLD]

# Jobs retryable / startable
JOBSTATES_RETRYABLE = [Jobstates.ERROR, Jobstates.CANCELED]

# Jobrunner filters using this for get_jobs_with_single_sfloc_to_wait_for
JOBSTATES_RUNNER_WAIT = [Jobstates.HOLD, Jobstates.QUEUED, Jobstates.PROCESSING, Jobstates.ERROR, Jobstates.REVOKING]


class BaseJob:
    """Base class for jobs"""
    # Whether job can be taken out of the job queue without consequences
    # as long as we dont have proper on_delete cleanup - this is needed
    # to avoid having created file entries that are needed downstream
    can_be_canceled = False
    # FIXME in most cases, kwargs is passed as a **dict, since we dont know what is in it
    # except inside the specific job, and on instantiation. This means it's not necessary
    # to pass AND receive with ** - may as well be a dict. It's a bit dirty though,
    # better to have more explicit args where that is possible.

    def __init__(self, job):
        self.job = job
        self.run_tasks = []
        if job:
            self.state = job.state if job else False
    
    def create_db_job(self, name, state, kwargs):
        self.job = Job.objects.create(funcname=name, timestamp=timezone.now(),
            state=state, kwargs=kwargs)
        FileJob.objects.bulk_create([FileJob(rawfile_id=rf_id, job_id=self.job.pk)
            for rf_id in self.get_rf_ids_for_filejobs()])
        return {'id': self.job.id, 'kwargs': kwargs, 'error': False}

    def check_error(self, **kwargs):
        # FIXME check_error will be used differently when creating vs when running, please fix!
        return self.check_error_on_creation(**kwargs)

    def check_error_on_creation(self, **kwargs):
        return False

    def check_error_on_running(self, **kwargs):
        return False

    def oncreate_getfiles_query(self, **kwargs):
        return StoredFileLoc.objects.none()

    def getfiles_query(self, **kwargs):
        return self.oncreate_getfiles_query(**kwargs).filter(purged=False)

    def update_sourcefns_lastused(self, **kwargs):
        '''Updating last_date_used timestamps on SFLoc and DSS.
        This gets called when creating a job (to make sure files arent deleted between
        creation and running), and when running the job (to make a later timestamp in case of
        long waiting times'''
        self.oncreate_getfiles_query(**kwargs).update(last_date_used=timezone.now())
        StoredFileLoc.objects.filter(pk__in=self._get_extrafiles_to_rsync(**kwargs)).update(
                last_date_used=timezone.now())
        dm.DatasetServer.objects.filter(pk__in=self.get_dss_ids(**kwargs)).update(
                last_date_used=timezone.now())

    def on_create_addkwargs(self, **kwargs):
        # FIXME we could have a create(, **kw) function that calls addkwargs,
        # maybe sets sfloc to active, etc, and creates the actual job in Dfor ana_id in B
        return {}

    def _get_extrafiles_to_rsync(self, **kwargs):
        return []

    def _select_rsync_server(self, srcshare_id, dstshare_id):
        # Select file servers to rsync from/to - FIXME share code!
        '''Shares should already have been confirmed to be active?
        Or can we do that here?
        '''
        servers = FileserverShare.objects.filter(share_id__in=[srcshare_id, dstshare_id],
                server__active=True)
        rsync_server_q = servers.filter(server__can_rsync_remote=True)
        if singleserver := FileServer.objects.filter(fileservershare__in=servers, active=True,
                fileservershare__share=dstshare_id).filter(fileservershare__share=srcshare_id):
            # Try to get both shares from same server, rsync can skip SSH then
            srcserver = servers.filter(server__in=singleserver,
                    share=srcshare_id).values('server__fqdn', 'server__name', 'path').first()
            dstserver = servers.filter(server__in=singleserver, share=dstshare_id
                    ).values('server__fqdn', 'server__name', 'path').first()
            src_user = dst_user = rskey = False
            rsyncservername = srcserver['server__name']
        elif rsync_server_q.filter(share_id=srcshare_id).exists():
            # rsyncing server has src file, push to remote
            srcserver = rsync_server_q.filter(share_id=srcshare_id).values(
                    'server__fqdn', 'path', 'server__name').first()
            dstserver = servers.filter(share_id=dstshare_id).values('server__fqdn'
                    , 'path', 'server__rsynckeyfile', 'server__rsyncusername').first()
            rsyncservername = srcserver['server__name']
            dst_user, rskey = dstserver['server__rsyncusername'], dstserver['server__rsynckeyfile']
            src_user = False
        elif rsync_server_q.filter(share_id=dstshare_id).exists():
            # rsyncing server is the dst, pull from remote
            dstserver = rsync_server_q.values('server__fqdn', 'path', 'server__name').first()
            srcserver = servers.filter(share_id=srcshare_id).values('server__fqdn',
                    'path', 'server__rsynckeyfile', 'server__rsyncusername').first()
            rsyncservername = dstserver['server__name']
            src_user, rskey = srcserver['server__rsyncusername'], srcserver['server__rsynckeyfile']
            dst_user = False
        else:
            # FIXME error needs finding in error check already
            raise RuntimeError('Could not get file share on any rsync capable controller server')
        return srcserver, dstserver, rsyncservername, src_user, dst_user, rskey

    def on_create_prep_rsync_jobs(self, **kwargs):
        '''If needed, rsync the DB and other singlefiles which is not on the analysis share,
        this function provides a dict with the job name and kwargs for that'''
        newjobs = []
        if all_exfiles_sfpk := self._get_extrafiles_to_rsync(**kwargs):
            extra_sfl_q = StoredFileLoc.objects.filter(sfile_id__in=all_exfiles_sfpk, active=True)
            if extra_sfl_q.distinct('sfile').count() < len(all_exfiles_sfpk):
                raise RuntimeError('Not all parameter files could be found on disk, make sure they exist')
            elif 'fserver_id' in kwargs:
                # Job independent on sfloc_ids has fserver_id so we can pick raw file share here
                # and rsync extra files to there if needed
                ready_sfls = extra_sfl_q.filter(
                        servershare__fileservershare__server_id=kwargs['fserver_id']
                        ).distinct('sfile_id')
                if ready_sfls.distinct('sfile').count() < len(all_exfiles_sfpk):
                    if dstfss_q := FileserverShare.objects.filter(server_id=kwargs['fserver_id'],
                            share__function=ShareFunction.RAWDATA).values('share_id', 'share__function'):
                        # Enforce raw data share or error
                        dstfss = dstfss_q.first()
                        dstshare_id, dstsharefun = dstfss['share_id'], dstfss['share__function']
                    else:
                        raise RuntimeError('Cannot find raw data share on server to run job on, '
                                'while there are files to sync to there. Please contact admin')

            elif self.oncreate_getfiles_query(**kwargs).count():
                # Job with sfloc_ids input (e.g. analysis job). Use this sfloc which is on
                # the server to execute, as a location template to sync the extra files to.
                sfl = self.oncreate_getfiles_query(**kwargs).values('servershare_id',
                        'servershare__function').first()
                ready_sfls = extra_sfl_q.filter(servershare_id=sfl['servershare_id']
                    ).distinct('sfile_id')
                dstshare_id, dstsharefun = sfl['servershare_id'], sfl['servershare__function']

            else:
                raise RuntimeError('Cannot determine if extra files for job need syncing to target '
                        'file server. Contact admin')

            
            # Make rsync jobs for those extrafiles which there is no sfl in the servershare we want:
            # Loop over files not already in servershare
            for extra_sf in StoredFile.objects.filter(pk__in=all_exfiles_sfpk).exclude(
                        pk__in=[x['sfile_id'] for x in ready_sfls.values('sfile_id')]):
                extra_sf_sfl_q = extra_sf.storedfileloc_set.filter(active=True,
                        servershare__active=True, servershare__fileservershare__server__active=True)
                if rs_extra_sfl := extra_sf_sfl_q.filter(
                        servershare__fileservershare__server__can_rsync_remote=True):
                    # either the extra sfl is on an rsync-capable server:
                    # .first() since it can be on multiple mounts on that server
                    extra_sfl = rs_extra_sfl.values('pk', 'servershare__function', 'path').first()
                elif FileserverShare.objects.filter(share_id=dstshare_id,
                        share__active=True, server__active=True, server__can_rsync_remote=True):
                    # or the target server is rsync capable
                    extra_sfl = extra_sf_sfl_q.values('pk', 'servershare__function', 'path').first()
                else:
                    raise RuntimeError('Could not find a source file to upload to analysis server for '
                        f'{extra_sf.filename}')
                blankpathshares = [ShareFunction.LIBRARY, ShareFunction.INBOX]
                if extra_sfl['servershare__function'] in blankpathshares and dstsharefun not in blankpathshares:
                    # e.g. libfile from path '' needs a path in the dst if that is rawdata!
                    path = settings.LIBRARY_FILE_PATH_INBOX
                else:
                    path = extra_sfl['path']
                newjobs.append({'name': 'rsync_otherfiles_to_servershare',
                    'kwargs': {'sfloc_id': extra_sfl['pk'], 'dstshare_id': dstshare_id,
                        'dstpath': path}})
        return newjobs

    def get_jobs_with_single_sfloc_to_wait_for(self, **kwargs):
        '''Need to wait for non-dataset jobs on files involved in this job. One could
        add those to the get_sf_ids_for_filejobs but then you wouldnt be able to run two
        analyses in parallel if theyd use the same DB.
        However, a problem now is that such a file could be deleted mid-analysis,
        in a parallel job - make sure these files are fairly stable
        '''
        # FIXME make sure any single file job checks for these jobs as well.
        if sfpks := self._get_extrafiles_to_rsync(**kwargs):
            all_sfl = StoredFileLoc.objects.filter(sfile_id__in=sfpks, active=True).values('pk')
            return Job.objects.filter(kwargs__sfloc_id__in=[x['pk'] for x in all_sfl])
        else:
            return Job.objects.none()

    def get_rf_ids_for_filejobs(self):
        """This is run before running job, to define files used by
        the job (so it cant run if if files are in use by other job)"""
        return [x['sfile__rawfile_id'] for x in self.oncreate_getfiles_query(**self.job.kwargs
            ).values('sfile__rawfile_id')]

    def get_dsids_jobrunner(self, **kwargs):
        return []

    def get_dss_ids(self, **kwargs):
        return []

    def run(self, **kwargs):
        self.process(**kwargs)
        self.update_sourcefns_lastused(**kwargs)
        self.queue_tasks()

    def post(self):
        pass

    def set_error(self, job, *, errmsg):
        job.state = Jobstates.ERROR
        job.save()
        if errmsg:
            JobError.objects.create(job_id=job.id, message=errmsg)

    def on_pause(self, **kwargs):
        pass

    def get_server_based_queue(self, servername, queue):
        return f'{servername}__{queue}'

    def queue_tasks(self):
        '''If queue is defined on job, run that queue, otherwise, get queue
        from the task (in case of variable server-dependent queues'''
        for runtask in self.run_tasks:
            if self.queue:
                args = runtask
                queue = self.queue
            else:
                args, queue = runtask
            print(f'Queueing task for job {self.job.pk} to queue {queue}')
            tid = self.task.apply_async(args=args, queue=queue)
            self.create_db_task(tid, *args)

    def create_db_task(self, task_id, *args):
        return Task.objects.create(asyncid=task_id, job_id=self.job.pk, state=states.PENDING, args=args)


class SingleFileJob(BaseJob):
    '''Job class for any job which specifies a single file on a share (so an StoredFileLoc).
    '''

    def oncreate_getfiles_query(self, **kwargs):
        return StoredFileLoc.objects.filter(pk=kwargs['sfloc_id'], servershare__active=True)
        # As in multifile job (PurgeFiles)

    def get_dsids_jobrunner(self, **kwargs):
        ''''In case a single file has a dataset'''
        return [x['pk'] for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            datasetrawfile__rawfile__storedfile__storedfileloc__id=kwargs['sfloc_id']).values('pk')]


class MultiFileJob(BaseJob):
    '''Job class to specify any job on a number of files on a specific share.
    '''
    def oncreate_getfiles_query(self, **kwargs):
        return StoredFileLoc.objects.filter(pk__in=kwargs['sfloc_ids'], servershare__active=True)

    def get_dsids_jobrunner(self, **kwargs):
        ''''In case a single file has a dataset'''
        return [x['pk'] for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            datasetrawfile__rawfile__storedfile__storedfileloc__in=kwargs['sfloc_ids']).values('pk')]


class DatasetJob(BaseJob):
    '''Any job that changes a dataset (rename, adding/removing files, backup, reactivate).
    We include add/remove etc since the jobrunner will wait for the entire dataset file operations
    then, not only for the files-to-be-added, which is good since otherwise you could start
    an analysis on the dataset without those files, for example.
    '''

    def get_dsids_jobrunner(self, **kwargs):
        return [x.pk for x in dm.Dataset.objects.filter(datasetserver__pk=kwargs['dss_id'])]

    def get_dss_ids(self, **kwargs):
        return [kwargs['dss_id']]

    def get_rf_ids_for_filejobs(self):
        '''Let runner wait for entire dataset'''
        return [x['pk'] for x in RawFile.objects.filter(
            datasetrawfile__dataset__datasetserver__id=self.job.kwargs['dss_id']).values('pk')]

    def oncreate_getfiles_query(self, **kwargs):
        '''Get all files which had a datasetrawfile association when this job was created/retried,
        (so get the FileJob entries). Files will either be used in the
        job itself and/or post the job in e.g. re-setting their paths etc.

        FileJob records: let all files associated with dataset wait,
        this means that the job is created AFTER new datasetrawfile associations, and BEFORE
        removed datasetrawfile association.

        When e.g. check_job_error is used, there is no job yet, and this will return the files
        of a dataset'
        '''
        return StoredFileLoc.objects.filter(pk__in=kwargs['sfloc_ids'], servershare__active=True)


class MultiDatasetJob(BaseJob):
    '''For jobs on multiple datasets, currently NF search job'''

    def get_dss_ids(self, **kwargs):
        return kwargs['dss_ids']

    def get_rf_ids_for_filejobs(self):
        '''Let runner wait for entire datasets'''
        dss = dm.DatasetServer.objects.filter(pk__in=self.job.kwargs['dss_ids'])
        return [x['pk'] for x in RawFile.objects.filter(
            datasetrawfile__dataset__datasetserver__in=dss).values('pk')]

    def oncreate_getfiles_query(self, **kwargs):
        '''As for dataset job'''
        return StoredFileLoc.objects.filter(pk__in=kwargs['sfloc_ids'], servershare__active=True)
