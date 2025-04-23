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
from datasets.models import DatasetRawFile, Dataset
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
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
        return HttpResponseForbidden()
    dset = Dataset.objects.filter(pk=data['dset_id'])
    dset.update(storage_loc=data['storage_loc'])
    if data['newsharename']:
        newshare = ServerShare.objects.get(name=data['newsharename'])
        dset.update(storageshare=newshare)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@require_POST
def update_storagepath_file(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY, settings.ANALYSISCLIENT_APIKEY]):
        return HttpResponseForbidden()
    print('Updating storage task finished')
    print(data)
    if 'sfloc_id' in data:
        sfloc = StoredFileLoc.objects.get(pk=data['sfloc_id'])
        print(sfloc)
        sfloc.servershare = ServerShare.objects.get(name=data['servershare'])
        sfloc.path = data['dst_path']
        if 'newname' in data:
            sfloc.sfile.filename = data['newname']
            sfloc.sfile.rawfile.name = data['newname']
            sfloc.sfile.rawfile.save()
        sfloc.sfile.save()
        sfloc.save()
    elif 'sfloc_ids' in data:
        sfns = StoredFileLoc.objects.filter(pk__in=[int(x) for x in data['sfloc_ids']])
        sfns.update(path=data['dst_path'])
        if 'servershare' in data:
            sshare = ServerShare.objects.get(name=data['servershare'])
            sfns.update(servershare=sshare)
    if 'task' in data:
        set_task_done(data['task'])
    return HttpResponse()


@require_POST
def renamed_project(request):
    """
    After project renaming on disk, we need to set all paths of the storedfile objects
    to the new path name, and rename the project in the DB as well.
    """
    data = json.loads(request.body.decode('utf-8'))
    print('Updating storage task finished')
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY, settings.ANALYSISCLIENT_APIKEY]):
        return HttpResponseForbidden()
    # this updates also deleted files. Good in case restoring backup etc
    proj_sfiles = StoredFileLoc.objects.filter(pk__in=data['sfloc_ids'])
    for dset in Dataset.objects.filter(runname__experiment__project_id=data['proj_id']):
        newstorloc = dsviews.rename_storage_loc_toplvl(data['newname'], dset.storage_loc)
        dset.storage_loc = newstorloc
        dset.save()
        proj_sfiles.filter(sfile__rawfile__datasetrawfile__dataset=dset).update(path=newstorloc)
    set_task_done(data['task'])
    return HttpResponse()


@login_required
@require_POST
def pause_job(request):
    req = json.loads(request.body.decode('utf-8'))
    jobq = models.Job.objects.filter(pk=req['item_id'])
    if not jobq.exists():
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'}, status=403)
    job = jobq.get()
    ownership = get_job_ownership(job, request)
    if not ownership['owner_loggedin'] and not ownership['is_staff']:
        return JsonResponse({'error': 'Only job owners and admin can pause this job'}, status=403)
    elif not is_job_retryable(job):
        return JsonResponse({'error': 'Job type {} cannot be paused/resumed'.format(job.funcname)}, status=403)

    updated = jobq.filter(state__in=JOBSTATES_PAUSABLE).update(state=Jobstates.WAITING)
    if not updated:
        return JsonResponse({'error': 'Only jobs that are pending for queueing or errored can be paused'}, status=403)
    else:
        # Error job: delete tasks
        job.task_set.exclude(state=states.SUCCESS).delete()
        if hasattr(job, 'joberror'):
            job.joberror.delete()
        jwrapper = jobmap[job.funcname](job.id) 
        jwrapper.on_pause(**job.kwargs)
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
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
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
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
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
    '''When files are downloaded in a job this can clean up afterwards
    '''
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or not taskclient_authorized(
            data['client_id'], [settings.STORAGECLIENT_APIKEY]):
        return HttpResponseForbidden()
    sfile = StoredFile.objects.select_related('rawfile').get(pk=data['sf_id'])
    # Delete file in tmp download area
    if data['do_md5check']:
        sfile.checked = sfile.rawfile.source_md5 == data['md5']
    else:
        # rsync checks integrity so we should not have problems here
        sfile.checked = True
    if data['unzipped']:
        sfile.filename = sfile.filename.rstrip('.zip')
        sfile.rawfile.name = sfile.rawfile.name.rstrip('.zip')
        sfile.rawfile.save()
    sfile.save()
    fpath = rj.create_upload_dst_web(sfile.rawfile.pk, sfile.filetype.filetype)
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
    StoredFileLoc.objects.filter(pk=data['sflocid']).update(purged=False,
            servershare_id=ServerShare.objects.get(name=data['serversharename']))
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
    am.Analysis.objects.filter(pk=data['analysis_id']).update(editable=False)
    return HttpResponse()


@require_POST
def mzml_convert_or_refine_file_done(request):
    """Converted/Refined mzML file already copied to analysis result storage 
    server with MD5, fn, path. This function then queues a job to move it to 
    dataset directory from the analysis dir"""
    # FIXME need to remove the empty dir after moving all the files, how?
    # create cleaning task queue on ana server
    data = json.loads(request.POST['json'])
    if ('client_id' not in data or
            data['client_id'] != settings.ANALYSISCLIENT_APIKEY):
        return HttpResponseForbidden()
    sfloc = StoredFileLoc.objects.select_related('sfile__rawfile__datasetrawfile__dataset').get(pk=data['fn_id'])
    sfloc.sfile.md5 = data['md5']
    sfloc.sfile.checked = True
    sfloc.sfile.save()
    # FIXME buggy - if you remove fns from dataset, they will not have a datasetrawfile!
    # do not use that here! instead, direct pass the storage loc from the job!
    sfloc.save()
    create_job('move_single_file', sfloc_id=sfloc.id, dstsharename=settings.PRIMARY_STORAGESHARENAME,
            dst_path=sfloc.sfile.rawfile.datasetrawfile.dataset.storage_loc)
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


@require_POST
def confirm_internal_file(request):
    """Stores the reporting of a transferred analysis result file,
    and of downloaded uniprot files, checks its md5"""
    data =  json.loads(request.POST['json'])
    upload = UploadToken.validate_token(data['token'], [])
    if not upload:
        return HttpResponseForbidden()
    dstshare = ServerShare.objects.get(name=data['dstsharename'])

    # Reruns lead to trying to store files multiple times, avoid that here:
    sfile, created = StoredFile.objects.get_or_create(rawfile_id=data['fn_id'], md5=data['md5'],
            defaults={'filetype_id': upload.filetype_id, 'checked': True, 'filename': data['filename']})
    sfloc, _ = StoredFileLoc.objects.get_or_create(sfile=sfile, servershare=dstshare, path=data['outdir'])
    if data['analysis_id'] and created:
        am.AnalysisResultFile.objects.create(analysis_id=data['analysis_id'], sfile=sfile)
    elif data['is_fasta'] and created:
        fa = data['is_fasta']
        # set fasta download files
        libfile = am.LibraryFile.objects.create(sfile=sfile, description=fa['desc'])
        dbmodel = {'uniprot': am.UniProtFasta, 'ensembl': am.EnsemblFasta}[fa['dbname']]
        kwargs = {'version': fa['version'], 'libfile_id': libfile.id, 'organism': fa['organism']}
        subtype = False
        if fa['dbname'] == 'uniprot':
            subtype = am.UniProtFasta.UniprotClass[fa['dbtype']]
            kwargs['dbtype'] = subtype
        dbmodel.objects.create(**kwargs)

    # Also store any potential servable file on share on web server
    if data['filename'] in settings.SERVABLE_FILENAMES and request.FILES:
        # FIXME web files are not currently tracked by an sfloc (or previously by an sf)
        #webshare = ServerShare.objects.get(name=settings.WEBSHARENAME)
        #srvfile, _cr = StoredFileLoc.objects.get_or_create(sfile=sfile, servershare=webshare, path=data['outdir'])
        srvpath = os.path.join(settings.WEBSHARE, sfloc.path)
        srvdst = os.path.join(srvpath, sfile.filename)
        try:
            os.makedirs(srvpath, exist_ok=True)
        except FileExistsError:
            pass
        with NamedTemporaryFile(mode='wb+') as fp:
            for chunk in request.FILES['ana_file']:
                fp.write(chunk)
            fp.flush()
            os.fsync(fp.fileno())
            shutil.copy(fp.name, srvdst)
            os.chmod(srvdst, 0o644)
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
        fjs = job.filejob_set.select_related('storedfile__rawfile__datasetrawfile__dataset')
        try:
            users = list({y.user for x in fjs for y in x.storedfile.rawfile.datasetrawfile.dataset.datasetowner_set.all()})
        except DatasetRawFile.DoesNotExist:
            usernames = list({x.storedfile.rawfile.producer.name for x in fjs})
            ownertype = 'admin'
        else:
            usernames = [x.username for x in users]
            owner_loggedin = request.user.id in [x.id for x in users]
    return {'usernames': usernames, 'owner_loggedin': owner_loggedin, 'type': ownertype,
             'is_staff': request.user.is_staff}
    

def is_job_retryable_ready(job, tasks=False):
    return is_job_retryable(job) and is_job_ready(job)


def is_job_retryable(job, tasks=False):
    return job.funcname in jobmap and jobmap[job.funcname].retryable


def is_job_ready(job=False, tasks=False):
    if tasks is False:
        tasks = models.Task.objects.filter(job_id=job.id)
    if {t.state for t in tasks}.difference(states.READY_STATES):
        return False
    return True


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
    if ownership['is_staff'] and is_job_retryable(job):
        do_retry_job(job, force=True)
    elif ownership['owner_loggedin'] and is_job_retryable_ready(job):
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
    tasks = models.Task.objects.filter(job=job)
    if not is_job_retryable(job) and not force:
        print('Cannot retry job, is not retryable')
        return
    if not is_job_ready(job=job, tasks=tasks) and not force:
        print('Tasks not all ready yet, will not retry, try again later')
        return
    # revoke tasks in case they are still running (force retry)
    revoke_and_delete_tasks(tasks.exclude(state=states.SUCCESS))
    # Redo file jobs (for runner) in case the job has been in WAITING state and other jobs 
    # have changed files associated to a dset or such.
    jwrap = jobmap[job.name](job.pk)
    FileJob.objects.filter(job_id=job.pk).delete()
    FileJob.objects.bulk_create([FileJob(storedfile_id=sf_id, job_id=job.id) for sf_id in 
        jwrap.get_sf_ids_for_filejobs(**job.kwargs)])
    jobq = models.Job.objects.filter(pk=job.pk, state__in=JOBSTATES_RETRYABLE).update(
            state=Jobstates.PENDING)
    try:
        job.joberror.delete()
    except models.JobError.DoesNotExist:
        pass
