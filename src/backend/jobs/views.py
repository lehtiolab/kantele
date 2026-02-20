import os
import json
import requests
import shutil
from datetime import datetime 
from urllib.parse import urljoin
from tempfile import NamedTemporaryFile

from celery import states
from celery.result import AsyncResult
from django.http import HttpResponseForbidden, HttpResponse, JsonResponse
from django.contrib.auth.decorators import login_required
from django.contrib.admin.views.decorators import staff_member_required
from django.views.decorators.http import require_POST
from django.db.models import F

from jobs import models

from rawstatus.models import (RawFile, StoredFile, StoredFileLoc, ServerShare, StoredFileType,
        SwestoreBackedupFile, PDCBackedupFile, Producer, UploadToken, FileJob)
from rawstatus import jobs as rj
from analysis import models as am
from analysis.views import write_analysis_log
from dashboard import views as dashviews
from datasets import views as dsviews
from datasets.models import DatasetRawFile, Dataset, DatasetServer
from home import models as hm
from jobs.jobs import Jobstates, JOBSTATES_PAUSABLE, JOBSTATES_JOB_SENT, JOBSTATES_JOB_NOT_SENT, JOBSTATES_RETRYABLE
from jobs.jobutil import create_job, jobmap
from kantele import settings


def set_task_done(task_id):
    task = models.Task.objects.get(asyncid=task_id)
    task.state = states.SUCCESS
    task.save()


def taskclient_authorized(client_id, possible_ids):
    """Possibly use DB in future"""
    return client_id in possible_ids


@require_POST
def set_task_status(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    # FIXME retries sometimes cause issues! Task not found
    taskq = models.Task.objects.filter(asyncid=data['task_id'])
    task_nr_found = taskq.update(state=data['state'])
    if not task_nr_found:
        # update() is 0 if no task found, 1 even if state is not actually updated
        return HttpResponseForbidden()
    if data.get('msg', False):
        task_id = taskq.values('pk').get()['pk']
        models.TaskError.objects.create(task_id=task_id, message=data['msg'])
    return HttpResponse()


@require_POST
def update_storage_loc_dset(request):
    """Updates storage_loc on dset after a dset update storage job"""
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    dss = DatasetServer.objects.filter(pk=data['dss_id'])
    dss.update(storage_loc=data['dst_path'])
    sfns = StoredFileLoc.objects.filter(pk__in=[int(x) for x in data['sfloc_ids']])
    sfns.update(path=data['dst_path'], purged=False)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@require_POST
def update_storagepath_file(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    print('Updating storage task finished')
    if 'sfloc_id' in data:
        StoredFileLoc.objects.filter(pk=data['sfloc_id']).update(path=data['dst_path'], purged=False)
        sf_q = StoredFile.objects.filter(storedfileloc__pk=data['sfloc_id'])
        if 'newname' in data:
            # FIXME newname sfile/rawfile should be done prejob, not post job? only rawfile, sfile is current!
            sf_q.update(filename=data['newname'])
            RawFile.objects.filter(storedfile__storedfileloc__pk=data['sfloc_id']).update(
                    name=data['newname'])
    elif 'sfloc_ids' in data:
        sfns = StoredFileLoc.objects.filter(pk__in=[int(x) for x in data['sfloc_ids']])
        sfns.update(path=data['dst_path'], purged=False)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@login_required
@require_POST
def pause_job(request):
    '''Puts a job in waiting state, taking it out of the job queue. Only 
    cancelable jobs can be paused, and only jobs in a certain 
    state (error, pending) can be paused.
    '''
    req = json.loads(request.body.decode('utf-8'))
    jobq = models.Job.objects.filter(pk=req['item_id'])
    if not jobq.exists():
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'}, status=403)
    job = jobq.get()
    jwrapper = jobmap[job.funcname](job) 
    ownership = get_job_ownership(job, request)
    if not ownership['owner_loggedin'] and not ownership['is_staff']:
        return JsonResponse({'error': 'Only job owners and admin can pause this job'}, status=403)
    elif not jwrapper.can_be_canceled:
        return JsonResponse({'error': 'Job type {} cannot be paused'.format(job.funcname)}, status=403)

    updated = jobq.filter(state__in=JOBSTATES_PAUSABLE).update(state=Jobstates.WAITING)
    if not updated:
        return JsonResponse({'error': 'Only jobs that are pending for queueing or errored can be paused'}, status=403)
    else:
        # Error job: delete tasks
        job.task_set.exclude(state=states.SUCCESS).delete()
        if hasattr(job, 'joberror'):
            job.joberror.delete()
        jwrapper.on_pause(**job.kwargs)
    return JsonResponse({}) 
    

@staff_member_required
@require_POST
def hold_job(request):
    '''Put a job in HOLD state, blocking the job queue until it is resumed.
    Running jobs will get their tasks revoked.
    '''
    req = json.loads(request.body.decode('utf-8'))
    jobq = models.Job.objects.filter(pk=req['item_id'])
    if not jobq.exists():
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'
            }, status=403)
    job = jobq.get()
    ownership = get_job_ownership(job, request)
    if tasks := job.task_set.exclude(state=states.SUCCESS):
        revoke_and_delete_tasks(tasks)
        updated = jobq.update(state=Jobstates.HOLD)
    else:
        updated = jobq.filter(state__in=[Jobstates.ERROR, Jobstates.DONE, Jobstates.REVOKING,
            Jobstates.CANCELED]).update(state=Jobstates.HOLD)
    if not updated:
        return JsonResponse({'error': 'Did not hold job, it was already in a state '
            'which cannot be changed to HOLD'}, status=406)
    return JsonResponse({})

    
@login_required
@require_POST
def resume_job(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        job = models.Job.objects.get(pk=req['item_id'])
    except models.Job.DoesNotExist:
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'}, status=403)
    ownership = get_job_ownership(job, request)
    if not ownership['owner_loggedin'] and not ownership['is_staff']:
        return JsonResponse({'error': 'Only job owners and admin can resume this job'}, status=403)
    elif job.state not in Jobstates.WAITING:
        return JsonResponse({'error': 'Only jobs that are paused can be resumed, this job is in state '
        '{}'.format(job.state)}, status=403)
    job.state = Jobstates.PENDING
    job.save()
    return JsonResponse({}) 
    



def cancel_or_revoke_job(jobq):
    '''Centralize revoking here, so correct check is done, and it is done in an update statement
    so there is no race condition stuff'''
    canceled = jobq.filter(state__in=JOBSTATES_JOB_NOT_SENT).update(state=Jobstates.CANCELED)
    revoked = jobq.filter(state__in=JOBSTATES_JOB_SENT).update(state=Jobstates.REVOKING)
    return canceled + revoked


@login_required
@require_POST
def delete_job(request):
    req = json.loads(request.body.decode('utf-8'))
    jobq = models.Job.objects.filter(pk=req['item_id'])
    if not jobq.exists():
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'}, status=403)
    job = jobq.get()
    ownership = get_job_ownership(job, request)
    if not ownership['owner_loggedin'] and not ownership['is_staff']:
        return JsonResponse({'error': 'Only job owners and admin can stop this job'}, status=403)
    # If job needs revoking, do it here
    cancel_or_revoke_job(jobq)
    return JsonResponse({})


@require_POST
def purge_storedfile(request):
    """Ran after a job has deleted a file from the filesystem, sets
    file DB entry to purged"""
    data = request.POST
    if 'client_id' not in data or not taskclient_authorized(data['client_id'], settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    sfile = StoredFileLoc.objects.filter(pk=data['sfloc_id']).update(purged=True)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@require_POST
def removed_emptydir(request):
    """Ran after a job has deleted an empty dir from the filesystem"""
    data = request.POST
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@require_POST
def register_external_file(request):
    """Storedfile and rawfn update proper md5 and set checked
    Creates job to add file to dset to move file to storage.
    This is for when you download files from some repository and you 
    thus do not know MD5 in advance, it is set on receival.
    """
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
        return HttpResponseForbidden()
    dataset = {'dataset_id': data['dset_id'], 'removed_files': {},
               'added_files': {1: {'id': data['raw_id']}}}
    # FIXME dont let just any job change the file state!
    # FIXME handle errors in save_or_up
    StoredFile.objects.filter(pk=data['sf_id']).update(md5=data['md5'], checked=True)
    RawFile.objects.filter(pk=data['raw_id']).update(source_md5=data['md5'])
    dsviews.save_or_update_files(dataset, f'task {data["task"]}')
    set_task_done(data['task'])
    return HttpResponse()


@require_POST
def downloaded_file(request):
    '''When files are downloaded from web upload
    in a job this can clean up afterwards
    '''
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
        return HttpResponseForbidden()
    sfloc = StoredFileLoc.objects.select_related('sfile__rawfile', 'sfile__filetype').get(
            pk=data['sfloc_id'])
    if data['do_md5check']:
        # FIXME Deprecate when no longer uploading .d files this route
        # md5 is checked for raws with stablefiles inside a .d
        sfloc.sfile.checked = sfloc.sfile.rawfile.source_md5 == data['md5']
        sfloc.purged = not sfloc.sfile.checked
    else:
        # md5 is done on upload in transfer_file
        sfloc.sfile.checked = True
        sfloc.purged = False

    if data['unzipped']:
        sfloc.sfile.filename = sfloc.sfile.filename.rstrip('.zip')
        sfloc.sfile.rawfile.name = sfloc.sfile.rawfile.name.rstrip('.zip')
        sfloc.sfile.rawfile.save()
    sfloc.sfile.save()
    sfloc.save()
    # Delete file in tmp download area
    fpath = rj.create_upload_dst_web(sfloc.sfile.rawfile.pk, sfloc.sfile.filetype.filetype)
    os.unlink(fpath)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@require_POST
def created_pdc_archive(request):
    data = request.POST
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
        return HttpResponseForbidden()
    backup = PDCBackedupFile.objects.filter(storedfile_id=data['sfid'])
    backup.update(pdcpath=data['pdcpath'], deleted=False, success=True)
    if 'task' in request.POST:
        set_task_done(request.POST['task'])
    return HttpResponse()


@require_POST
def restored_archive_file(request):
    data = request.POST
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
        return HttpResponseForbidden()
    StoredFileLoc.objects.filter(pk=data['sflocid']).update(purged=False)
    if 'task' in request.POST:
        set_task_done(request.POST['task'])
    return HttpResponse()


@require_POST
def analysis_run_done(request):
    data = json.loads(request.body.decode('utf-8'))
    if ('client_id' not in data or
            data['client_id'] not in settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    if 'log' in data:
        write_analysis_log(data['log'], data['analysis_id'])
    if 'task' in data:
        set_task_done(data['task'])
    # Defensively, since analysis should already be not editable upon launch:
    am.Analysis.objects.filter(pk=data['analysis_id']).update(editable=False)
    ana = am.Analysis.objects.values('user', 'name').get(pk=data['analysis_id'])
    hm.UserMessage.create_message(ana['user'], msgtype=hm.AnalysisMsgTypes.COMPLETED,
            analysis_id=data['analysis_id'])
    return HttpResponse()


@require_POST
def store_longitudinal_qc(request):
    data = json.loads(request.body.decode('utf-8'))
    if ('client_id' not in data or
            data['client_id'] not in settings.CLIENT_APIKEYS):
        return HttpResponseForbidden()
    else:
        storeresult = dashviews.store_longitudinal_qc(data)
        if storeresult['error']:
            return JsonResponse(storeresult, status=400)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


def get_job_analysis(job):
    try:
        analysis = job.nextflowsearch.analysis
    except am.NextflowSearch.DoesNotExist:
        analysis = False 
    return analysis


def get_job_ownership(job, request):
    """returns {'ownertype': user/admin, 'usernames': [], 'owner_loggedin': T/F}
    """
    owner_loggedin = False
    ownertype = 'user'
    ana = get_job_analysis(job)
    if ana:
        usernames = [ana.user.username]
        owner_loggedin = request.user.id == ana.user.id
    else:
        fjs = job.filejob_set.select_related('rawfile__datasetrawfile__dataset')
        try:
            users = list({y.user for x in fjs for y in x.rawfile.datasetrawfile.dataset.datasetowner_set.all()})
        except DatasetRawFile.DoesNotExist:
            usernames = list({x.rawfile.producer.name for x in fjs})
            ownertype = 'admin'
        else:
            usernames = [x.username for x in users]
            owner_loggedin = request.user.id in [x.id for x in users]
    return {'usernames': usernames, 'owner_loggedin': owner_loggedin, 'type': ownertype,
             'is_staff': request.user.is_staff}
    

@login_required
def retry_job(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    try:
        job = models.Job.objects.get(pk=req['item_id'])
    except models.Job.DoesNotExist:
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'}, status=403)
    ownership = get_job_ownership(job, request)
    if ownership['is_staff']:
        do_retry_job(job, force=True)
    elif ownership['owner_loggedin'] and job.state in JOBSTATES_RETRYABLE:
        do_retry_job(job)
    else:
        return JsonResponse({'error': 'You are not allowed to retry this job'}, status=403)
    return JsonResponse({})


def revoke_and_delete_tasks(task_q):
    task_q.update(state=states.REVOKED)
    for task in task_q:
        AsyncResult(task.asyncid).revoke(terminate=True, signal='SIGUSR1')
    task_q.delete()


def do_retry_job(job, force=False):
    '''Jobs that are not DONE, CANCELED, PENDING, can be retried'''
    jwrap = jobmap[job.funcname](job)
    tasks = models.Task.objects.filter(job=job)
    if {t.state for t in tasks}.difference(states.READY_STATES) and not force:
        print('Tasks not all ready yet, will not retry, try again later')
        return
    # revoke tasks in case they are still running (force retry)
    revoke_and_delete_tasks(tasks.exclude(state=states.SUCCESS))
    # Redo file jobs (for runner) in case the job has been in WAITING state and other jobs 
    # have changed files associated to a dset or such.
    FileJob.objects.filter(job_id=job.pk).delete()
    FileJob.objects.bulk_create([FileJob(rawfile_id=rf_id, job_id=job.id) for rf_id in 
        jwrap.get_rf_ids_for_filejobs()])
    jobq = models.Job.objects.filter(pk=job.pk).update(state=Jobstates.PENDING)
    try:
        job.joberror.delete()
    except models.JobError.DoesNotExist:
        pass
