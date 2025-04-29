import requests

from celery import states

from kantele import settings
from jobs.models import Task, Job, JobError
from rawstatus.models import StoredFile, StoredFileLoc
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
JOBSTATES_RETRYABLE = [Jobstates.WAITING, Jobstates.HOLD, Jobstates.PROCESSING, Jobstates.ERROR, Jobstates.REVOKING, Jobstates.CANCELED]

# FIXME Deprecate below line, is not used:
JOBSTATES_PRE_OK_JOB = [Jobstates.WAITING, Jobstates.ERROR, Jobstates.REVOKING, Jobstates.CANCELED, Jobstates.HOLD]


class BaseJob:
    """Base class for jobs"""
    retryable = True
    revokable = False
    # FIXME in most cases, kwargs is passed as a **dict, since we dont know what is in it
    # except inside the specific job, and on instantiation. This means it's not necessary
    # to pass AND receive with ** - may as well be a dict. It's a bit dirty though,
    # better to have more explicit args where that is possible.

    def __init__(self, job_id):
        self.job_id = job_id
        self.run_tasks = []
    
    def check_error(self, **kwargs):
        return False

    def getfiles_query(self, **kwargs):
        return []

    def get_sf_ids_for_filejobs(self, **kwargs):
        """This is run before running job, to define files used by
        the job (so it cant run if if files are in use by other job)"""
        return [x['sfile_id'] for x in self.getfiles_query(**kwargs).values('sfile_id')]

    def get_dsids_jobrunner(self, **kwargs):
        return []

    def run(self, **kwargs):
        self.process(**kwargs)
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

    def queue_tasks(self):
        '''If queue is defined on job, run that queue, otherwise, get queue
        from the task (in case of variable server-dependent queues'''
        for runtask in self.run_tasks:
            if self.queue:
                args, kwargs = runtask
                queue = self.queue
            else:
                args, kwargs, queue = runtask
            tid = self.task.apply_async(args=args, kwargs=kwargs, queue=queue)
            self.create_db_task(tid, *args, **kwargs)

    def create_db_task(self, task_id, *args, **kwargs):
        return Task.objects.create(asyncid=task_id, job_id=self.job_id, state=states.PENDING,
                args=[args, kwargs])


class SingleFileJob(BaseJob):
    '''Job class for any job which specifies a single file on a share (so an StoredFileLoc).

    QC, PDC, Rename, Classify, rsyncFileTtransfer (for http uploads)
    
    deprecate?:
    move_single
    '''

    def getfiles_query(self, **kwargs):
        return StoredFileLoc.objects.filter(pk=kwargs['sfloc_id']).select_related(
                'servershare', 'sfile__rawfile')
        # FIXME do .get and .select_related in jobs itself?
        # As in multifile job (PurgeFiles)

    def get_dsids_jobrunner(self, **kwargs):
        ''''In case a single file has a dataset'''
        return [x['pk'] for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            datasetrawfile__rawfile__storedfile__storedfileloc__id=kwargs['sfloc_id']).values('pk')]


class MultiFileJob(BaseJob):
    '''Job class to specify any job on a number of files on a specific share.
    Jobs:
    - Run NF analysis
    - Purge analysis
    - Purge files
    - Delete empty dir
    - Register external file
    '''
    def getfiles_query(self, **kwargs):
        return StoredFileLoc.objects.filter(pk__in=kwargs['sfloc_ids'])

    def get_dsids_jobrunner(self, **kwargs):
        ''''In case a single file has a dataset'''
        return [x['pk'] for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            datasetrawfile__rawfile__storedfile__storedfileloc__in=kwargs['sfloc_ids']).values('pk')]


class DatasetJob(BaseJob):
    '''Any job that changes a dataset (rename, adding/removing files, backup, reactivate).
    We include add/remove etc since the jobrunner will wait for the entire dataset file operations
    then, not only for the files-to-be-added, which is good since otherwise you could start
    an analysis on the dataset without those files, for example.

    Jobs:
    With servershare:
    - refine mzml
    - rename storloc
    - move dset servershare (rsync)
    - mzml convert
    - backup
    - retrieve

    Without servershare (all ss)
    - add/rm files 
    - delete mzml
    - delete dset
    - delete backup
    '''

    def get_dsids_jobrunner(self, **kwargs):
        return [x.pk for x in dm.Dataset.objects.filter(datasetserver__pk=kwargs['dss_id'])]

    def get_sf_ids_for_filejobs(self, **kwargs):
        '''Let runner wait for entire dataset'''
        dss = dm.DatasetServer.objects.get(pk=kwargs['dss_id'])
        return [x['pk'] for x in StoredFile.objects.filter(
            rawfile__datasetrawfile__dataset__datasetserver=dss).values('pk')]

    def getfiles_query(self, **kwargs):
        '''Get all files which had a datasetrawfile association when this job was created/retried,
        (so get the FileJob entries). Files will either be used in the
        job itself and/or post the job in e.g. re-setting their paths etc.

        FileJob records: let all files associated with dataset wait,
        this means that the job is created AFTER new datasetrawfile associations, and BEFORE
        removed datasetrawfile association.

        When e.g. check_job_error is used, there is no job yet, and this will return the files
        of a dataset'
        '''
        return StoredFileLoc.objects.filter(pk__in=kwargs['sfloc_ids'], purged=False)


class ProjectJob(BaseJob):
    '''There is only one ProjectJob and it is RenameProject - maybe change to MultiFile?
    '''
    def get_dsids_jobrunner(self, **kwargs):
        return [x.pk for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            runname__experiment__project_id=kwargs['proj_id'])]

    def getfiles_query(self, **kwargs):
        '''Get all files with same path as project_dsets.storage_locs, used to update
        path of those files post-job'''
        return StoredFileLoc.objects.filter(purged=False, sfile__deleted=False,
                sfile__rawfile__datasetrawfile__dataset__runname__experiment__project_id=kwargs['proj_id'])
