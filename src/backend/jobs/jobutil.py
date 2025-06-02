

from django.utils import timezone

from datasets import jobs as dsjobs
from rawstatus import jobs as rsjobs
from analysis import jobs as anjobs
from mstulos import jobs as mtjobs
from jobs.jobs import Jobstates
from jobs.models import Job
from rawstatus.models import FileJob


alljobs = [
        dsjobs.RenameDatasetStorageLoc,
        dsjobs.RsyncDatasetServershare,
        dsjobs.RemoveDatasetFilesFromServershare,
        dsjobs.ConvertDatasetMzml,
        dsjobs.DeleteActiveDataset,
        dsjobs.DeleteDatasetMzml,
        dsjobs.DeleteDatasetPDCBackup,
        dsjobs.RenameProject,
        dsjobs.MoveDatasetServershare,
        rsjobs.RsyncFileTransfer,
        rsjobs.RsyncFileTransferFromWeb,
        rsjobs.CreatePDCArchive,
        rsjobs.RestoreFromPDC,
        rsjobs.BackupPDCDataset,
        rsjobs.ReactivateDeletedDataset,
        rsjobs.RenameFile,
        rsjobs.MoveSingleFile,
        rsjobs.DeleteEmptyDirectory,
        rsjobs.DownloadPXProject,
        rsjobs.RegisterExternalFile,
        rsjobs.ClassifyMSRawFile,
        anjobs.RunLongitudinalQCWorkflow,
        anjobs.RunNextflowWorkflow,
        anjobs.RefineMzmls,
        anjobs.PurgeAnalysis,
        anjobs.DownloadFastaFromRepos,
        mtjobs.ProcessAnalysis,
        ]
jobmap = {job.refname: job for job in alljobs}



def check_job_error(name, **kwargs):
    jwrap = jobmap[name](False)
    return jwrap.check_error(**kwargs)


def create_job(name, state=False, **kwargs):
    '''Checks errors and then creates the job'''
    if not state:
        state = Jobstates.PENDING
    jwrap = jobmap[name](False)
    if error := check_job_error(name, **kwargs):
        jobdata = {'id': False, 'kwargs': kwargs, 'error': error}
    else:
        # addkwargs should be here, after error check, otehrwise
        # new SFL will be created at error check even when its failing
        kwargs.update(jwrap.on_create_addkwargs(**kwargs))
        for extrajob in jwrap.on_create_extrajobs(**kwargs):
            create_job(extrajob['name'], **extrajob['kwargs'])
        job = Job.objects.create(funcname=name, timestamp=timezone.now(),
            state=state, kwargs=kwargs)
        jobdata = {'id': job.id, 'kwargs': kwargs, 'error': False}
        FileJob.objects.bulk_create([FileJob(storedfile_id=sf_id, job_id=job.id) for sf_id in 
            jwrap.get_sf_ids_for_filejobs(**kwargs)])
    return jobdata


def create_job_without_check(name, state=False, **kwargs):
    '''In case you do error checking before creating jobs, you can use this
    for quicker creation without another check'''
    if not state:
        state = Jobstates.PENDING
    jwrap = jobmap[name](False)
    kwargs.update(jwrap.on_create_addkwargs(**kwargs))
    for extrajob in jwrap.on_create_extrajobs(**kwargs):
        create_job(extrajob['name'], **extrajob['kwargs'])
    job = Job.objects.create(funcname=name, timestamp=timezone.now(),
            state=state, kwargs=kwargs)
    FileJob.objects.bulk_create([FileJob(storedfile_id=sf_id, job_id=job.id) for sf_id in 
            jwrap.get_sf_ids_for_filejobs(**kwargs)])
    return {'id': job.id, 'kwargs': kwargs, 'error': False}
