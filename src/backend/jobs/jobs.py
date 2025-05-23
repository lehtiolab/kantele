import json
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

    def get_sf_ids_jobrunner(self, **kwargs):
        """This is run before running job, to define files used by
        the job (so it cant run if if files are in use by other job)"""
        return [x['sfile_id'] for x in self.getfiles_query(**kwargs)]


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
        for task in self.run_tasks:
            args, kwargs = task[0], task[1]
            tid = self.task.delay(*args, **kwargs)
            self.create_db_task(tid, *args, **kwargs)
    
    def create_db_task(self, task_id, *args, **kwargs):
        return Task.objects.create(asyncid=task_id, job_id=self.job_id, state=states.PENDING,
                args=[args, kwargs])


class SingleFileJob(BaseJob):
    def getfiles_query(self, **kwargs):
        # FIXME do .get and .select_related in jobs itself?
        # As in multifile job (PurgeFiles)
        return StoredFileLoc.objects.filter(pk=kwargs['sfloc_id']).select_related(
                'servershare', 'sfile__rawfile').get()

    def get_sf_ids_jobrunner(self, **kwargs):
        return [self.getfiles_query(**kwargs).sfile_id]

    def get_dsids_jobrunner(self, **kwargs):
        ''''In case a single file has a dataset'''
        return [x['pk'] for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            datasetrawfile__rawfile__storedfile__storedfileloc__id=kwargs['sfloc_id']).values('pk')]


class MultiFileJob(BaseJob):
    def getfiles_query(self, **kwargs):
        return StoredFileLoc.objects.filter(pk__in=kwargs['sfloc_ids'])

    def get_dsids_jobrunner(self, **kwargs):
        ''''In case a single file has a dataset'''
        return [x['pk'] for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            datasetrawfile__rawfile__storedfile__storedfileloc__in=kwargs['sfloc_ids']).values('pk')]


class DatasetJob(BaseJob):
    '''Any job that changes a dataset (rename, adding/removing files, backup, reactivate)'''

    def get_dsids_jobrunner(self, **kwargs):
        return [kwargs['dset_id']]

    def get_sf_ids_jobrunner(self, **kwargs):
        '''Let all files associated with dataset wait, including added files on other path, and 
        removed files on dset path (will be moved to new folder before their move to tmp)'''
        dset = dm.Dataset.objects.get(pk=kwargs['dset_id'])
        dsfiles = StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=kwargs['dset_id'])
        ds_ondisk = StoredFile.objects.filter(storedfileloc__servershare=dset.storageshare, storedfileloc__path=dset.storage_loc)
        return [x.pk for x in dsfiles.union(ds_ondisk)]

    def getfiles_query(self, **kwargs):
        '''Get all files with same path as dset.storage_loc. This gets all files in the dset dir,
        not only the ones that have a datasetrawfile. If this job runs just after a user removes
        files from the dataset, the job that runs the removal comes after this, and the removed files
        will not have a datasetrawfile, so we need to take care of all the files in the dset.
        Also, files ADDED to the dataset will still be on tmp and need not to be included in this
        job, even though they will have a datasetrawfile.
        # FIXME to avoid this particular issue, create_job could create the sfids instead of dset_id when being run.
        '''
        dset = dm.Dataset.objects.get(pk=kwargs['dset_id'])
        return StoredFileLoc.objects.filter(servershare=dset.storageshare, path=dset.storage_loc)


class ProjectJob(BaseJob):
    def get_dsids_jobrunner(self, **kwargs):
        return [x.pk for x in dm.Dataset.objects.filter(deleted=False, purged=False,
            runname__experiment__project_id=kwargs['proj_id'])]

    def getfiles_query(self, **kwargs):
        '''Get all files with same path as project_dsets.storage_locs, used to update
        path of those files post-job'''
        dsets = dm.Dataset.objects.filter(runname__experiment__project_id=kwargs['proj_id'])
        return StoredFileLoc.objects.filter(
                servershare__in=[x.storageshare for x in dsets.distinct('storageshare')],
                path__in=[x.storage_loc for x in dsets.distinct('storage_loc')])

    def get_sf_ids_jobrunner(self, **kwargs):
        """Get all sf ids in project to mark them as not using pre-this-job"""
        projfiles = StoredFile.objects.filter(deleted=False, storedfileloc__purged=False,
                rawfile__datasetrawfile__dataset__runname__experiment__project_id=kwargs['proj_id'])
        dsets = dm.Dataset.objects.filter(runname__experiment__project_id=kwargs['proj_id'])
        allfiles = StoredFile.objects.filter(storedfileloc__servershare__in=[x.storageshare for x in dsets.distinct('storageshare')],
                storedfileloc__path__in=[x.storage_loc for x in dsets.distinct('storage_loc')]).union(projfiles)
        return [x.pk for x in allfiles]
