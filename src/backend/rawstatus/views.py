from django.http import (JsonResponse, HttpResponseForbidden, FileResponse,
                         HttpResponse, HttpResponseBadRequest)
from django.shortcuts import render
from django.template import loader
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.models import User
from django.utils import timezone
from django.contrib.admin.views.decorators import staff_member_required

from datetime import timedelta, datetime
from functools import wraps
import os
import re
import json
import shutil
import zipfile
from tempfile import NamedTemporaryFile, mkstemp
from uuid import uuid4
import requests
from hashlib import md5
from urllib.parse import urlsplit
from Bio import SeqIO
from celery import states as taskstates

from kantele import settings
from rawstatus.models import (RawFile, Producer, StoredFile, ServerShare,
                              SwestoreBackedupFile, StoredFileType, UserFile,
                              PDCBackedupFile, UploadToken)
from rawstatus import jobs as rsjobs
from rawstatus.tasks import search_raws_downloaded
from analysis.models import (Analysis, LibraryFile, AnalysisResultFile)
from datasets import views as dsviews
from datasets import models as dsmodels
from dashboard import models as dashmodels
from jobs import models as jm
from jobs import jobs as jobutil
from jobs.jobutil import create_job, check_job_error


def inflow_page(request):
    return render(request, 'rawstatus/inflow.html', {
        'userfile_id': UploadToken.UploadFileType.USERFILE,
        'rawfile_id': UploadToken.UploadFileType.RAWFILE,
        'library_id': UploadToken.UploadFileType.LIBRARY,
        'producers': {x.id: x.name for x in Producer.objects.filter(msinstrument__active=True,
            internal=True)},
        'filetypes': [{'id': x.id, 'name': x.name, 'israw': x.is_rawdata, 'isfolder': x.is_folder}
            for x in StoredFileType.objects.filter(user_uploadable=True)]})


@staff_member_required
@require_POST
def import_external_data(request):
    # Input like so: {share_id: int, dirname: top_lvl_dir, dsets: [{'instrument_id': int, 'name': str, 'files': [(path/to/file.raw', ],
    # FIXME thermo files are .raw, but how do we handle bruker raws? they are folders!
    req = json.loads(request.body.decode('utf-8'))
    share = ServerShare.objects.get(pk=req['share_id'])
    proj = dsmodels.Project.objects.get(pk=settings.PX_PROJECT_ID)
    exp, created = dsmodels.Experiment.objects.get_or_create(name=req['dirname'], project_id=settings.PX_PROJECT_ID)
    dscreatedata = {'datatype_id': dsviews.get_quantprot_id(), 'prefrac_id': False,
            'ptype_id': settings.LOCAL_PTYPE_ID}
    date = timezone.now()
    for indset in req['dsets']:
        extprod = Producer.objects.get(pk=indset['instrument_id'])
        run, created = dsmodels.RunName.objects.get_or_create(name=indset['name'], experiment=exp)
        dset = dsmodels.Dataset.objects.filter(runname=run)
        # save_new_dset is complex enough to not use .get_or_create
        if not dset.exists():
            dset = dsviews.save_new_dataset(dscreatedata, proj, exp, run, request.user.id)
        else:
            dset = dset.get()
        sf_ids = []
        for fpath, size in indset['files']:
            path, fn = os.path.split(fpath)
            fakemd5 = md5()
            fakemd5.update(fn.encode('utf-8'))
            fakemd5 = fakemd5.hexdigest()
            rawfn, _ = RawFile.objects.get_or_create(source_md5=fakemd5, defaults={
                'name': fn, 'producer': extprod, 'size': size, 'date': date, 'claimed': True})
            sfile = StoredFile.objects.get_or_create(rawfile_id=rawfn.pk,
                    filetype_id=extprod.msinstrument.filetype_id, filename=fn,
                    defaults={'servershare_id': share.id,
                        'path': os.path.join(req['dirname'], path), 'md5': fakemd5})
            sf_ids.append(sfile.pk)
        # Jobs to get MD5 etc
        create_job('register_external_raw', dset_id=dset.id, sf_ids=sf_ids, sharename=share.name)
    return JsonResponse({})


@staff_member_required
@require_GET
def scan_raws_tmp(request):
    if 'dirname' not in request.GET:
        return JsonResponse({'shares': [{'id': x.id, 'name': x.name} 
            for x in ServerShare.objects.filter(name='tmp')]})
    dirname = request.GET['dirname']
    serversharename = 'tmp'
    res = search_raws_downloaded.delay(serversharename, dirname)
    # TODO make async to allow large time diff if we have network or other
    # problems, or are busy on backend file server
    exprods = Producer.objects.filter(pk__in=settings.EXTERNAL_PRODUCER_IDS)
    result = res.get()
    return JsonResponse({'dirsfound': result, 'instruments': [(ep.id, ep.name) for ep in exprods]})


@login_required
def browser_userupload(request):
    # needs test
    # FIXME make sure this job is similar to transfer_file then?
    data = request.POST
    try:
        ftype = StoredFileType.objects.get(user_uploadable=True, pk=int(data['ftype_id']))
    except ValueError:
        return JsonResponse({'success': False, 'msg': 'Please select a file type '
        f'{data["ftype_id"]}'}, status=400)
    except StoredFileType.DoesNotExist:
        return JsonResponse({'success': False, 'msg': 'Illegal file type to upload'}, status=403)
    try:
        archive_only = bool(int(data['archive_only']))
        uploadtype = int(data['uploadtype'])
    except (ValueError, KeyError):
        return JsonResponse({'success': False, 'msg': 'Bad request, contact admin'}, status=400)
    uft = UploadToken.UploadFileType
    desc = str(data.get('desc', '').strip())
    if desc == '':
        desc = False
        if uploadtype in [uft.LIBRARY, uft.USERFILE]:
            return JsonResponse({'success': False, 'msg': 'A description for this file is required'}, status=400)
    if ftype.is_folder:
        return JsonResponse({'success': False, 'msg': 'Cannot upload folder datatypes through browser'}, status=403)

    # create userfileupload model (incl. fake token)
    # FIXME hardcoded admin name!
    producer = Producer.objects.get(shortname='admin')
    upload = create_upload_token(ftype.pk, request.user.id, producer, uploadtype, archive_only)
    # tmp write file 
    upfile = request.FILES['file']
    dighash = md5()
    # Fasta must be text mode for checking with SeqIO, other files can be binary
    fpmode = 'wb+' if not ftype.filetype == 'fasta' else 'w+'
    notfa_err_resp = {'msg': 'File is not correct FASTA', 'success': False}
    with NamedTemporaryFile(mode=fpmode) as fp:
        for chunk in upfile.chunks():
            if fpmode == 'w+':
                try:
                    fp.write(chunk.decode('utf-8'))
                except UnicodeDecodeError:
                    return JsonResponse(notfa_err_resp, status=403)
            else:
                fp.write(chunk)
            dighash.update(chunk)
        # stay in context until copied, else tempfile is deleted
        fp.seek(0)
        if ftype.filetype == 'fasta' and not any(SeqIO.parse(fp, 'fasta')):
            return JsonResponse(notfa_err_resp, status=403)
        dighash = dighash.hexdigest() 
        raw, _ = RawFile.objects.get_or_create(source_md5=dighash, defaults={
                'name': upfile.name, 'producer': producer, 'size': upfile.size,
                'date': timezone.now(), 'claimed': True})
        dst = rsjobs.create_upload_dst_web(raw.pk, upload.filetype.filetype)
        # Copy file to target uploadpath, after Tempfile context is gone, it is deleted
        shutil.copy(fp.name, dst)
        os.chmod(dst, 0o644)

    # Unfortunately have to do checking after upload as we need the MD5 of the file
    sfns = StoredFile.objects.filter(rawfile_id=raw.pk)
    if sfns.count() == 1:
        os.unlink(dst)
        return JsonResponse({'success': False, 'msg': 'This file is already in the '
            f'system: {sfns.get().filename}'}, status=403)
    elif sfns.count():
        os.unlink(dst)
        return JsonResponse({'success': False, 'msg': 'Multiple files already found, this '
            'should not happen, please inform your administrator'}, status=403)

    # Get the file path and share dependent on the upload type
    ufiletypes = UploadToken.UploadFileType
    if upload.uploadtype == ufiletypes.RAWFILE:
        dstpath = settings.TMPPATH
        dstsharename = settings.TMPSHARENAME
        fname = upfile.name
    elif upload.uploadtype == ufiletypes.LIBRARY:
        dstsharename = settings.PRIMARY_STORAGESHARENAME
        fname = f'{raw.pk}_{upfile.name}'
        dstpath = settings.LIBRARY_FILE_PATH
    elif upload.uploadtype == ufiletypes.USERFILE:
        dstsharename = settings.PRIMARY_STORAGESHARENAME
        fname = f'{raw.pk}_{upfile.name}'
        dstpath = settings.USERFILEDIR
    else:
        return JsonResponse({'success': False, 'msg': 'Can only upload files of raw, library, '
            'or user type'}, status=403)

    dstshare = ServerShare.objects.get(name=dstsharename)
    if upload.uploadtype == ufiletypes.RAWFILE and StoredFile.objects.filter(filename=fname, path=dstpath,
            servershare=dstshare, deleted=False).exclude(rawfile__source_md5=raw.source_md5).exists():
        return JsonResponse({'error': 'Another file in the system has the same name '
            f'and is stored in the same path ({dstshare.name} - {dstpath}/{fname}). '
            'Please investigate, possibly change the file name or location of this or the other '
            'file to enable transfer without overwriting.', 'problem': 'DUPLICATE_EXISTS'},
            status=403)

    # All good, get the file to storage
    sfile = StoredFile.objects.create(rawfile_id=raw.pk, filename=fname, checked=True,
            filetype=upload.filetype, md5=dighash, path=dstpath, servershare=dstshare)
    create_job('rsync_transfer', sf_id=sfile.pk, src_path=dst)
    dstfn = process_file_confirmed_ready(sfile.rawfile, sfile, upload, desc)
    return JsonResponse({'success': True, 'msg': 'Succesfully uploaded file to '
        f'become {dstfn} File will be accessible on storage soon.'})

    
# TODO store heartbeat of instrument, deploy config, message from backend, etc

@require_POST
def instrument_check_in(request):
    '''Returns 200 at correct token or expiring token, in which case a new token
    will be issued'''
    # FIXME need unit test
    # auto update producer would be nice, when it calls server at intervals, then downloads_automaticlly
    # a new version of itself?
    data = json.loads(request.body.decode('utf-8'))
    token = data.get('token', False)
    client_id = data.get('client_id', False)
    # analysis transfer client checks in with taskid
    taskid = data.get('task_id', False)
    if not any([token, taskid]):
        return JsonResponse({'error': 'Bad request'}, status=400)
    elif taskid and not data.get('ftype', False):
        return JsonResponse({'error': 'Bad request'}, status=400)

    upload = UploadToken.validate_token(token, ['producer']) if token else False
    task = jm.Task.objects.filter(asyncid=taskid).exclude(state__in=jobutil.JOBSTATES_DONE)

    response = {'newtoken': False}
    uploadtype = UploadToken.UploadFileType.RAWFILE
    manual_producers = [settings.PRODUCER_ADMIN_NAME, settings.PRODUCER_ANALYSIS_NAME]
    if upload:
        day_window = timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS)
        if (upload.producer.client_id != client_id and 
                upload.producer.shortname not in manual_producers):
            # producer is admin if there is no client id
            return JsonResponse({'error': 'Token/client ID invalid or non-existing'}, status=403)
        elif client_id and upload.expires - day_window < timezone.now() < upload.expires:
            # Keep the token bound to a client instrument
            upload.expired = True
            upload.save()
            newtoken = create_upload_token(upload.filetype_id, upload.user_id, upload.producer, uploadtype)
            response.update({'newtoken': newtoken.token, 'expires': datetime.strftime(newtoken.expires, '%Y-%m-%d, %H:%M')})
        else:
            response.update({'newtoken': False, 'expires': datetime.strftime(upload.expires, '%Y-%m-%d, %H:%M')})
        response['stablefiles'] = upload.filetype.stablefiles

    elif task.count():
        # Token for a client on a controlled system like analysis server:
        # auth by client ID and task ID knowledge
        producer = Producer.objects.get(client_id=data['client_id'])
        try:
            ftype = StoredFileType.objects.get(name=data['ftype'])
        except StoredFileType.DoesNotExist:
            return JsonResponse({'error': 'File type does not exist'}, status=403)
        print('New token issued for a valid task ID without a token')
        staff_ops = dsmodels.Operator.objects.filter(user__is_staff=True)
        if staff_ops.exists():
            user_op = staff_ops.first()
        else:
            user_op = dsmodels.Operator.objects.first()
        newtoken = create_upload_token(ftype.pk, user_op.user_id, producer, uploadtype)
        response.update({'newtoken': newtoken.token, 'expires': datetime.strftime(newtoken.expires, '%Y-%m-%d, %H:%M')})

    else:
        return JsonResponse({'error': 'Token / task ID invalid or not existing'}, status=403)
    return JsonResponse(response)

 
def login_required_403_json(view_func):
    """
    Modified django's login_required to return a JsonResponse with
    403 instead of a redirect to the log-in page.
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if request.user.is_authenticated:
            return view_func(request, *args, **kwargs)
        return JsonResponse({'error': 'Permission denied'}, status=403)
    return _wrapped_view



@login_required_403_json
@require_POST
def request_upload_token(request):
    '''This view is ony for the instrument check-in, and the manual upload. It is not
    used by the analysis upload, and will not work with that uploadtype if tested'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        producer = Producer.objects.get(client_id=data['producer_id'])
    except Producer.DoesNotExist:
        return JsonResponse({'error': True, 'error': 'Cannot use that file producer'}, status=403)
    except KeyError:
        producer = Producer.objects.get(shortname=settings.PRODUCER_ADMIN_NAME)
        try:
            uploadtype = UploadToken.UploadFileType(data['uploadtype'])
        except KeyError:
            return JsonResponse({'error': True, 'error': 'Need to specify upload type, contact '
                'admin'}, status=403)
    else:
        uploadtype = UploadToken.UploadFileType.RAWFILE
    try:
        selected_ft = StoredFileType.objects.get(pk=data['ftype_id'])
    except StoredFileType.DoesNotExist:
        return JsonResponse({'error': True, 'error': 'Cannot use that file type'}, status=403)
    if uploadtype not in [UploadToken.UploadFileType.RAWFILE,
            UploadToken.UploadFileType.USERFILE, UploadToken.UploadFileType.LIBRARY]:
        return JsonResponse({'success': False, 'msg': 'Can only upload raw, library, user files '})

    ufu = create_upload_token(data['ftype_id'], request.user.id, producer, uploadtype, data['archive_only'])
    host = settings.KANTELEHOST or request.build_absolute_uri('/')
    return JsonResponse(ufu.parse_token_for_frontend(host))


def create_upload_token(ftype_id, user_id, producer, uploadtype, archive_only=False):
    '''Generates a new UploadToken for a producer and stores it in DB'''
    token = str(uuid4())
    expi_sec = settings.MAX_TIME_PROD_TOKEN if producer.internal else settings.MAX_TIME_UPLOADTOKEN
    expiry = timezone.now() + timedelta(seconds=expi_sec)
    return UploadToken.objects.create(token=token, user_id=user_id, expired=False,
            expires=expiry, filetype_id=ftype_id, producer=producer,
            archive_only=archive_only, uploadtype=uploadtype)


# /files/transferstate
@require_POST
def get_files_transferstate(request):
    data =  json.loads(request.body.decode('utf-8'))
    try:
        token = data['token']
    except KeyError as error:
        return JsonResponse({'error': 'No token, cannot authenticate'}, status=403)
    
    if fnid := data.get('fnid', False):
        desc = data.get('desc')
        fn = size = md5 = file_date = False
    else:
        desc = True # Do not error on missing description
        try:
            fn, size, md5, filedate_raw = data['fn'], data['size'], data['md5'], data['date']
            file_date = datetime.strftime(
                datetime.fromtimestamp(float(filedate_raw)), '%Y-%m-%d %H:%M')
        except ValueError as error:
            return JsonResponse({'error': 'Date passed to registration incorrectly formatted'}, status=400)
        except KeyError as error:
            print(f'Request to get transferstate with missing parameter, {error}')
            return JsonResponse({'error': 'Bad request'}, status=400)

    upload = UploadToken.validate_token(token, ['producer'])
    if not upload:
        return JsonResponse({'error': 'Token invalid or expired'}, status=403)
    elif upload.uploadtype in [UploadToken.UploadFileType.LIBRARY, UploadToken.UploadFileType.USERFILE] and not desc:
        return JsonResponse({'error': 'Library or user files need a description'}, status=403)
    elif upload.uploadtype == UploadToken.UploadFileType.ANALYSIS and not hasattr(upload, 'externalanalysis'):
        # FIXME can we upload proper analysis files here too??? In theory, yes! At a speed cost
        return JsonResponse({'error': 'Analysis result uploads need an analysis_id to put them in'}, status=403)

    if not fnid:
        if upload.uploadtype == UploadToken.UploadFileType.ANALYSIS:
            claimed = True
        else:
            claimed = False
        rfn, _ = RawFile.objects.get_or_create(source_md5=md5, defaults={
            'name': fn, 'producer': upload.producer, 'size': size, 'date': file_date,
            'claimed': claimed})
    else:
        rfn = RawFile.objects.filter(pk=fnid).select_related('producer')
        if not rfn.count():
            return JsonResponse({'error': f'File with ID {fnid} cannot be found in system'}, status=404)
        rfn = rfn.get()
    if rfn.producer != upload.producer:
        # In case the file has been moved to another instrument or the instrument API key
        # is wrong here (unlikely right?)
        return JsonResponse({'error': f'File with ID {rfn.id} is not from producer  {upload.producer.name}'}, status=403)
    # FIXME if somehow really bad timing, there will be multiple sfns?
    sfns = rfn.storedfile_set.filter(filetype_id=upload.filetype_id)
    if not sfns.count():
        # has not been reported as transferred,
        tstate = 'transfer'
    elif sfns.filter(mzmlfile__isnull=True).count() > 1:
        # Now behaviour specifies there can only be one copy of a raw file
        # What happens if there is a copy e.g. on a different server?
        errmsg = 'Problem, there are multiple stored files with that raw file ID'
        return JsonResponse({'error': errmsg}, status=409)
    else:
        # File in system, should be transferred and being rsynced/unzipped, or
        # errored, or done.
        sfn = sfns.select_related('filetype', 'userfile', 'libraryfile').filter(
                mzmlfile__isnull=True).get()
        up_dst = rsjobs.create_upload_dst_web(rfn.pk, sfn.filetype.filetype)
        rsync_jobs = jm.Job.objects.filter(funcname='rsync_transfer',
                kwargs__sf_id=sfn.pk, kwargs__src_path=up_dst).order_by('timestamp')
        # fetching from DB here to avoid race condition in if/else block
        try:
            last_rsjob = rsync_jobs.last()
        except jm.Job.DoesNotExist:
            last_rsjob = False
        # Refresh to make sure we dont get race condition where it is checked
        # while we fetching jobs above and the non-checked/done job will result
        # in a retransfer
        sfn.refresh_from_db()

        if sfn.checked:
            # File transfer and check finished
            tstate = 'done'
            has_backupjob = jm.Job.objects.filter(funcname='create_pdc_archive',

                    kwargs__sf_id=sfn.pk, state=jobutil.JOBSTATES_WAIT).exists()
            if not has_backupjob and not PDCBackedupFile.objects.filter(storedfile_id=sfn.id):
                # No already-backedup PDC file, then do some processing work
                process_file_confirmed_ready(rfn, sfn, upload, desc)
        # FIXME this is too hardcoded data model which will be changed one day,
        # needs to be in Job class abstraction!

        elif not last_rsjob:
            # There is no rsync job for this file, means it's old or somehow
            # errored # TODO how to report to user? File is also not OK checked
            tstate = 'wait'
        # FIXME elif last_rsjob.state == jobutil.Jobstates.ERROR: tstate = 'skip' ??
        elif last_rsjob.state not in jobutil.JOBSTATES_DONE:
            # File being rsynced and optionally md5checked (or it crashed, job
            # errored, revoked, wait for system or admin to catch job)
            # WARNING: this did not work when doing sfn.filejob_set.filter ?
            # A second call to this route would fire the rsync/md5 job again,
            # until the file was checked. But in theory it'd work, and by hand too.
            # Maybe a DB or cache thing, however 3seconds between calls should be enough?
            # Maybe NGINX caches stuff, add some sort of no-cache into the header of request in client producer.py
            tstate = 'wait'

        elif last_rsjob.state == jobutil.Jobstates.DONE:
            # MD5 on disk is not same as registered MD5, corrupted transfer
            # reset MD5 on stored file to make sure no NEW stored files are created
            # basically setting its state to pre-transfer state
            sfn.md5 = rfn.source_md5
            sfn.save()
            tstate = 'transfer'

        else:
            # There is an unlikely rsync job which is canceled, requeue it
            create_job('rsync_transfer', sf_id=sfn.pk, src_path=up_dst)
            tstate = 'wait'
    response = {'transferstate': tstate, 'fn_id': rfn.pk}
    return JsonResponse(response)


def classified_rawfile_treatment(request):
    '''Task calls this after reading a raw file for classification'''
    data = json.loads(request.body.decode('utf-8'))
    tasks = jm.Task.objects.filter(asyncid=data['task_id'], state=taskstates.PENDING)
    # If task is force-retried, and there was another task running, that other task will
    # get 403 here
    if tasks.count() != 1:
        return HttpResponseForbidden()
    try:
        token, fnid, is_qc, dsid = data['token'], data['fnid'], data['qc'], data['dset_id']
    except KeyError as error:
        return JsonResponse({'error': 'Bad request'}, status=400)
    upload = UploadToken.validate_token(token, [])
    if not upload:
        return JsonResponse({'error': 'Token invalid or expired'}, status=403)
    ufts = UploadToken.UploadFileType
    sfn = StoredFile.objects.filter(pk=fnid).select_related('rawfile__producer').get()
    if sfn.rawfile.claimed:
        # This file has already been classified or otherwise picked up
        return HttpResponse()
    if is_qc:
        sfn.rawfile.claimed = True
        sfn.rawfile.save()
        create_job('move_single_file', sf_id=sfn.pk,
                dstsharename=settings.PRIMARY_STORAGESHARENAME,
                dst_path=os.path.join(settings.QC_STORAGE_DIR, sfn.rawfile.producer.name))
        staff_ops = dsmodels.Operator.objects.filter(user__is_staff=True)
        if staff_ops.exists():
            user_op = staff_ops.first()
        else:
            user_op = dsmodels.Operator.objects.first()
        run_singlefile_qc(sfn.rawfile, sfn, user_op)
    elif dsid:
        # Make sure dataset exists
        if not dsmodels.Dataset.objects.filter(pk=dsid).exists():
            # TODO this needs logging
            return HttpResponse()
        # Make sure users cant use this file for something else:
        sfn.rawfile.claimed = True
        sfn.rawfile.save()
        # Now make job
        mvjob_kw = {'dset_id': dsid, 'rawfn_ids': [sfn.rawfile_id]}
        if error := check_job_error('move_files_storage', **mvjob_kw):
            # TODO this needs logging
            print(error)
            return HttpResponse()
        job, _cr = jm.Job.objects.get_or_create(state=jobutil.Jobstates.HOLD,
                funcname='move_files_storage', kwargs=mvjob_kw, timestamp=timezone.now())
        if not _cr:
            # Somehow script has already run!
            return HttpResponse()

    # For all files, even those not assoc to QC/Dset
    create_job('create_pdc_archive', sf_id=sfn.pk, isdir=sfn.filetype.is_folder)
    if upload.archive_only:
        # This archive_only is for sens data but we should probably have a completely
        # different track for that TODO
        # This purge job only runs when the PDC job is confirmed, w need_archive
        sfn.deleted = True
        sfn.save()
        create_job('purge_files', sf_ids=[sfn.pk], need_archive=True)
    updated = jm.Task.objects.filter(asyncid=data['task_id']).update(state=taskstates.SUCCESS)
    return HttpResponse()


def process_file_confirmed_ready(rfn, sfn, upload, desc):
    """Processing of backup, QC, library/userfile after transfer has succeeded
    (MD5 checked) for newly arrived MS other raw data files (not for analysis etc)
    Files that are for archiving only are also deleted from the archive share after
    backing up.
    """
    is_ms = hasattr(rfn.producer, 'msinstrument')
    is_active_ms = is_ms and rfn.producer.internal and rfn.producer.msinstrument.active
    newname = sfn.filename
    if is_active_ms and upload.uploadtype == UploadToken.UploadFileType.RAWFILE:
        create_job('classify_msrawfile', sf_id=sfn.pk, token=upload.token)
        # No backup before the classify job etc
    else:
        if upload.uploadtype == UploadToken.UploadFileType.LIBRARY:
            LibraryFile.objects.create(sfile=sfn, description=desc)
            newname = f'libfile_{sfn.libraryfile.id}_{rfn.name}'
            create_job('move_single_file', sf_id=sfn.id, dst_path=settings.LIBRARY_FILE_PATH,
                    newname=newname)
        elif upload.uploadtype == UploadToken.UploadFileType.USERFILE:
            UserFile.objects.create(sfile=sfn, description=desc, upload=upload)
            newname = f'userfile_{rfn.id}_{rfn.name}'
            # FIXME can we move a folder!?
            create_job('move_single_file', sf_id=sfn.id, dst_path=settings.USERFILEDIR,
                    newname=newname)
        elif upload.uploadtype == UploadToken.UploadFileType.ANALYSIS:
            AnalysisResultFile.objects.create(sfile=sfn, analysis=upload.externalanalysis.analysis)
        create_job('create_pdc_archive', sf_id=sfn.id, isdir=sfn.filetype.is_folder)
        if upload.archive_only:
            # This archive_only is for sens data but we should probably have a completely
            # different track for that TODO
            # This purge job only runs when the PDC job is confirmed, w need_archive
            sfn.deleted = True
            sfn.save()
            create_job('purge_files', sf_ids=[sfn.pk], need_archive=True)
    return newname


@require_POST
def transfer_file(request):
    # FIXME HTTP need long view time 
    # FIXME add share name to upload to and path
    '''HTTP based file upload'''
    data = request.POST
    try:
        token = data['token']
        fn_id = int(data['fn_id'])
        fname = data['filename']
    except KeyError as error:
        print(f'POST request to transfer_file with missing parameter, {error}')
        return JsonResponse({'error': 'Bad request'}, status=400)
    except ValueError:
        print(f'POST request to transfer_file with incorrect fn_id, {error}')
        return JsonResponse({'error': 'Bad request'}, status=400)
    upload = UploadToken.validate_token(token, ['filetype', 'externalanalysis__analysis'])
    if not upload:
        return JsonResponse({'error': 'Token invalid or expired'}, status=403)
    # First check if everything is OK wrt rawfile/storedfiles
    try:
        rawfn = RawFile.objects.get(pk=fn_id)
    except RawFile.DoesNotExist:
        errmsg = 'File with ID {} has not been registered yet, cannot transfer'.format(fn_id)
        return JsonResponse({'state': 'error', 'problem': 'NOT_REGISTERED', 'error': errmsg}, status=403)
    sfns = StoredFile.objects.filter(rawfile_id=fn_id)
    if sfns.filter(checked=True).count():
        # By default do not overwrite, although deleted files could trigger this
        # as well. In that case, have admin remove the files from DB.
        # TODO create exception for that if ever needed? data['overwrite'] = True?
        # Also look at below get_or_create call and checking created
        return JsonResponse({'error': 'This file is already in the '
            f'system: {sfns.first().filename}, if you are re-uploading a previously '
            'deleted file, consider reactivating from backup, or contact admin',
            'problem': 'ALREADY_EXISTS'}, status=409)

    elif sfns.filter(checked=False).count() > 1:
        return JsonResponse({'error': 'This file is already in the '
            f'system: {sfns.first().filename} and it has multiple DB entries. That '
            'should not happen, please contact admin',
            'problem': 'MULTIPLE_ENTRIES'}, status=409)

    elif sfns.filter(checked=False).count() == 1:
        # Re-transferring a failed file
        sfn = sfns.get()
        up_dst = rsjobs.create_upload_dst_web(rawfn.pk, sfn.filetype.filetype)
        rsync_jobs = jm.Job.objects.filter(funcname='rsync_transfer',
                kwargs__sf_id=sfn.pk, kwargs__src_path=up_dst).order_by('timestamp')
        # fetching from DB here to avoid race condition in if/else block
        try:
            last_rsjob = rsync_jobs.last()
        except jm.Job.DoesNotExist:
            last_rsjob = False
        if not last_rsjob:
            return JsonResponse({'error': 'This file is already in the '
                f'system: {sfns.first().filename}, but there is no job to put it in the '
                'storage. Please contact admin', 'problem': 'NO_RSYNC'}, status=409)
        elif last_rsjob.state not in jobutil.JOBSTATES_DONE:
            return JsonResponse({'error': 'This file is already in the '
                f'system: {sfns.first().filename}, and it is queued for transfer to storage '
                'If this is taking too long, please contact admin',
                'problem': 'RSYNC_PENDING'}, status=403)
        else:
            # Overwrite sf with rsync done and checked=False, corrupt -> retransfer
            pass

    # Has the filename changed between register and transfer? Assume user has stopped the upload,
    # corrected the name, and also change the rawname
    if upload.filetype.is_folder and len(upload.filetype.stablefiles) > 0:
        nonzip_fname = fname.rstrip('.zip')
    else:
        nonzip_fname = fname
    if nonzip_fname != rawfn.name:
        rawfn.name = nonzip_fname
        rawfn.save()
    ufiletypes = UploadToken.UploadFileType
    # Now prepare file system info, check if duplicate name exists:
    check_dup = False
    if upload.archive_only:
        dstsharename = settings.ARCHIVESHARENAME
        dstpath = settings.ARCHIVEPATH
        check_dup = True
    elif upload.uploadtype == ufiletypes.RAWFILE:
        dstpath = settings.TMPPATH
        dstsharename = settings.TMPSHARENAME
        check_dup = True
    elif upload.uploadtype == ufiletypes.ANALYSIS:
        dstpath = upload.externalanalysis.analysis.storage_dir
        dstsharename = settings.ANALYSISSHARENAME
    elif upload.uploadtype == ufiletypes.LIBRARY:
        # Make file names unique because harder to control external files
        fname = f'{rawfn.pk}_{fname}'
        dstpath = settings.LIBRARY_FILE_PATH
        dstsharename = settings.PRIMARY_STORAGESHARENAME
    elif upload.uploadtype == ufiletypes.USERFILE:
        # Make file names unique because harder to control external files
        fname = f'{rawfn.pk}_{fname}'
        dstpath = settings.USERFILEDIR
        dstsharename = settings.PRIMARY_STORAGESHARENAME
    else:
        return JsonResponse({'error': f'Upload has an invalid uploadtype ID ({upload.uploadtype}). '
            'This should not happen, contact admin'}, status=403)

    dstshare = ServerShare.objects.get(name=dstsharename)
    if check_dup and StoredFile.objects.filter(filename=nonzip_fname, path=dstpath, servershare=dstshare,
            deleted=False).exclude(rawfile__source_md5=rawfn.source_md5).exists():
        return JsonResponse({'error': 'Another file in the system has the same name '
            f'and is stored in the same path ({dstshare.name} - {dstpath}/{nonzip_fname}). '
            'Please investigate, possibly change the file name or location of this or the other '
            'file to enable transfer without overwriting.', 'problem': 'DUPLICATE_EXISTS'},
            status=403)

    # All clear, do the upload storing:
    upfile = request.FILES['file']
    dighash = md5()
    upload_dst = rsjobs.create_upload_dst_web(rawfn.pk, upload.filetype.filetype)
    # Write file from /tmp (or in memory if small) to its destination in upload folder
    # We could do shutil.move() if /tmp file, for faster performance, but on docker
    # with bound host folders this is a read/write operation and not a simple atomic mv
    # That means we can do MD5 check at hardly an extra cost, it is hardly slower than
    # not doing it if we're r/w anyway. Thus we can skip using an extra bg job
    if upload.filetype.is_folder and len(upload.filetype.stablefiles) > 0:
        # folder data is uploaded zipped and will be unzipped after rsync
        # contains a stablefile to MD5 check on, post-unzip
        with open(upload_dst, 'wb+') as fp:
            for chunk in upfile.chunks():
                fp.write(chunk)
    else:
        # No predictable file inside zipped folder if any, so we instead do MD5 on
        # entire zipped folder or raw file which is uploaded.
        with open(upload_dst, 'wb+') as fp:
            for chunk in upfile.chunks():
                fp.write(chunk)
                dighash.update(chunk)
        dighash = dighash.hexdigest() 
        if dighash != rawfn.source_md5:
            os.unlink(upload_dst)
            return JsonResponse({'error': 'Failed to upload file, checksum differs from reported MD5, possibly corrupted in transfer or changed on local disk', 'state': 'error'}, status=409)
    os.chmod(upload_dst, 0o644)
    file_trf, created = StoredFile.objects.get_or_create(
            rawfile=rawfn, filetype=upload.filetype, md5=rawfn.source_md5,
            defaults={'servershare': dstshare, 'path': dstpath, 'filename': fname})
    if not created:
        # Is this possible? Above checking with sfns.count() for both checked and non-checekd
        print('File already registered as transferred')
    create_job('rsync_transfer', sf_id=file_trf.pk, src_path=upload_dst)
    return JsonResponse({'fn_id': fn_id, 'state': 'ok'})


def query_all_qc_files():
    '''QC files are defined as not having a dataset, being claimed, and stored on the
    QC storage dir'''
    return StoredFile.objects.filter(rawfile__datasetrawfile__isnull=True, rawfile__claimed=True,
            path__startswith=settings.QC_STORAGE_DIR)


def run_singlefile_qc(rawfile, storedfile, user_op):
    """This method is only run for detecting new incoming QC files"""
    params = ['--instrument', rawfile.producer.msinstrument.instrumenttype.name]
    analysis = Analysis.objects.create(user_id=user_op.user_id,
            name=f'{rawfile.producer.name}_{rawfile.name}_{rawfile.date}')
    create_job('run_longit_qc_workflow', sf_id=storedfile.id, analysis_id=analysis.id, params=params)


def get_file_owners(sfile):
    owners = {x.id for x in User.objects.filter(is_superuser=True)}
    if hasattr(sfile.rawfile, 'datasetrawfile'):
        owners.update(dsviews.get_dataset_owners_ids(sfile.rawfile.datasetrawfile.dataset))
    elif hasattr(sfile, 'analysisresultfile'):
        owners.add(sfile.analysisresultfile.analysis.user.id)
    return owners
 

@login_required
def rename_file(request):
    """Renames a single file. This checks if characters are correct, launches job
    with bare filename (no extension), since job determines if mutliple files including
    mzML have to be renamed."""
    if not request.method == 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    data =  json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.filter(pk=data['sf_id']).select_related(
            'rawfile', 'mzmlfile').get()
        newfilename = os.path.splitext(data['newname'])[0]
        #mv_mzml = data['mvmzml']  # TODO optional mzml renaming too? Now it is default
    except (StoredFile.DoesNotExist, KeyError):
        print('Stored file to rename does not exist')
        return JsonResponse({'error': 'File does not exist'}, status=403)
    if request.user.id not in get_file_owners(sfile):
        return JsonResponse({'error': 'Not authorized to rename this file'}, status=403)
    elif hasattr(sfile, 'mzmlfile'):
        return JsonResponse({'error': 'Files of this type cannot be renamed'}, status=403)
    elif re.match('^[a-zA-Z_0-9\-]*$', newfilename) is None:
        return JsonResponse({'error': 'Illegal characteres in new file name'}, status=403)
    job = create_job('rename_file', sf_id=sfile.id, newname=newfilename)
    if job['error']:
        return JsonResponse({'error': job['error']}, status=403)
    else:
        return JsonResponse({})


def zip_instrument_upload_pkg(prod, runtransferfile):
    tmpfp, zipfilename = mkstemp()
    shutil.copy('/assets/producer.zip', zipfilename)
    with zipfile.ZipFile(zipfilename, 'a') as zipfp:
        zipfp.write('rawstatus/file_inputs/upload.py', 'upload.py')
        zipfp.write('rawstatus/file_inputs/producer.bat', 'transfer.bat')
        zipfp.writestr('transfer_config.json', runtransferfile)
    return zipfilename


def zip_user_upload_pkg(windows):
    tmpfp, zipfilename = mkstemp()
    with zipfile.ZipFile(zipfilename, 'w') as zipfp:
        zipfp.write('rawstatus/file_inputs/upload.py', 'upload.py')
        if windows:
            zipfp.write('rawstatus/file_inputs/kantele_upload.bat', 'kantele_upload.bat')
        else:
            zipfp.write('rawstatus/file_inputs/kantele_upload.sh', 'kantele_upload.sh')
    return zipfilename


@login_required
@require_GET
def download_instrument_package(request):
    # TODO instrument page with all instruments, which can have configs to be saved
    # make new app for this in django
    # configs will then be auto-downloaded when changing datadisk, outbox name,
    # instrument name, etc
    # and staff can create new instruments when they like
    try:
        client = request.GET['client']
    except KeyError:
        return HttpResponseForbidden()
    if client == 'instrument':
        try:
            prod_id = request.GET['prod_id']
            datadisk = request.GET['datadisk'][0]
        except (KeyError, IndexError):
            return HttpResponseForbidden()
        try:
            prod = Producer.objects.select_related('msinstrument').get(pk=prod_id)
        except Producer.DoesNotExist:
            return HttpResponseForbidden()
        fname_prefix = prod.name
        # strip datadisk so only get first letter
        runtransferfile = json.dumps({
            # FIXME some of these should go to instrument_checkin! So users can dynamically change it
            'outbox': f'{datadisk}:\outbox',
            'zipbox': f'{datadisk}:\zipbox',
            'donebox': f'{datadisk}:\donebox',
            'skipbox': f'{datadisk}:\skipbox',
            'client_id': prod.client_id,
            'filetype_id': prod.msinstrument.filetype_id,
            'acq_process_names': settings.PROCNAMES[prod.msinstrument.filetype.name],
            'injection_waittime': int(settings.INJ_WAITTIMES[prod.msinstrument.filetype.name]),
            'raw_is_folder': 1 if prod.msinstrument.filetype.is_folder else 0,
            'host': settings.KANTELEHOST,
            })
        if 'configonly' in request.GET and request.GET['configonly'] == 'true':
            resp = HttpResponse(runtransferfile, content_type='application/json')
            resp['Content-Disposition'] = 'attachment; filename="transfer_config.json"'
            return resp
        zipfn = zip_instrument_upload_pkg(prod, runtransferfile)
    elif client == 'user':
        fname_prefix = 'kantele'
        try:
            zipfn = zip_user_upload_pkg(int(request.GET['windows']))
        except (KeyError, ValueError):
            return HttpResponseBadRequest()
    else:
        return HttpResponseForbidden()
    resp = FileResponse(open(zipfn, 'rb'))
    resp['Content-Disposition'] = f'attachment; filename="{fname_prefix}_filetransfer.zip"'
    return resp


def show_old_new_projects(request):
    maxtime_nonint = timezone.now() - timedelta(settings.MAX_MZML_STORAGE_TIME_POST_ANALYSIS)
    allp = dsmodels.Project.objects.filter(active=True)
    # make an aggregate for gt / lt  maxtime, or just filter stuff?
    pass


def getxbytes(bytes, op=50):
    if bytes is None:
        return '0B'
    if bytes >> op:
        return '{}{}B'.format(bytes >> op, {0: '', 10: 'K', 20: 'M', 30: 'G', 40: 'T', 50: 'P'}[op])
    else:
        return getxbytes(bytes, op-10)


@login_required
@require_POST
def cleanup_old_files(request):
    if not request.user.is_staff:
        return JsonResponse({'state': 'error', 'error': 'User has no permission to retire this project, does not own all datasets in project'}, status=403)
    data = json.loads(request.body.decode('utf-8'))
    try:
        queue_job = data['queue_job']
    except KeyError as error:
        return HttpResponseForbidden()
    mzmls = StoredFile.objects.filter(mzmlfile__isnull=False, purged=False)
    maxtime_nonint = timezone.now() - timedelta(settings.MAX_MZML_STORAGE_TIME_POST_ANALYSIS)
    # Filtering gotchas here:
    # filter multiple excludes so they get done in serie, and exclude date__gt rather than
    # filter date__lt because then you get all rows where date is lt, but it does not check
    # if there are ALSO joins where date is gt...
    # old normal mzmls from searches
    old_searched_mzmls = mzmls.exclude(
            rawfile__datasetrawfile__dataset__datatype_id__in=settings.LC_DTYPE_IDS).exclude(
            rawfile__datasetrawfile__dataset__datasetanalysis__isnull=True).exclude(
            rawfile__datasetrawfile__dataset__datasetanalysis__analysis__date__gt=maxtime_nonint)
    # old LC mzmls
    lcmzmls = mzmls.filter(
            rawfile__datasetrawfile__dataset__datatype_id__in=settings.LC_DTYPE_IDS,
            rawfile__datasetrawfile__dataset__datasetanalysis__isnull=False).exclude(
            rawfile__datasetrawfile__dataset__datasetanalysis__analysis__date__gt=timezone.now() - timedelta(settings.MAX_MZML_LC_STORAGE_TIME))
    # old mzmls without searches
    old_nonsearched_mzml = mzmls.filter(
            rawfile__datasetrawfile__dataset__datasetanalysis__isnull=True,
            regdate__lt=maxtime_nonint)
    all_old_mzmls = old_searched_mzmls.union(lcmzmls, old_nonsearched_mzml)
    # TODO auto remove QC raw files of certain age? make sure you can get them back when needed though.

    def chunk_iter(qset, chunk_size):
        chunk = []
        # Django iterator has chunk_size but that only affects the database caching level
        # and not the chunked output. Here we use this to output chunks to create jobs from
        # otherwise the job params become very large
        for item in qset.iterator():
            chunk.append(item)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        yield chunk

    if queue_job:
        for chunk in chunk_iter(all_old_mzmls, 500):
            create_job('purge_files', sf_ids=[x.id for x in chunk])
        return JsonResponse({'ok': True})
    else:
        # cannot aggregate Sum on UNION
        totalsize_raw = getxbytes(sum((x['rawfile__size'] for x in all_old_mzmls.values('rawfile__size'))))
        return JsonResponse({'ok': True, 'mzml_cleanupsize_raws': totalsize_raw})


@login_required
def download_px_project(request):
    # FIXME check if pxacc exists on pride and here, before creating dset
    # FIXME View checks project and returns maybe as a nicety how many files it will download.
    # FIXME if already exist, update experiment name in view
    try:
        expname = request.POST['exp']
        pxacc = request.POST['px_acc']
    except KeyError:
        return JsonResponse({'error': 'Invalid request'}, status=403)
    # First check if we can get the dataset from PX at all
    try:
        px_files = rsjobs.call_proteomexchange(pxacc)
    except RuntimeError as error:
        return JsonResponse({'error': error}, status=500)
    except requests.exceptions.ConnectionError:
        return JsonResponse({'error': 'Could not connect to ProteomeXchange server, timed out'}, status=500)

    # Now go through the files
    date = datetime.strftime(timezone.now(), '%Y-%m-%d %H:%M')
    tmpshare = ServerShare.objects.get(name=settings.TMPSHARENAME)
    dset = dsviews.get_or_create_px_dset(expname, pxacc, request.POST['user_id'])
    raw_ids, shasums = [], {}
    extproducers = {x.msinstrument.instrumenttype.name: x for x in Producer.objects.filter(name__startswith='External')}
    for fn in px_files:
        ftpurl = urlsplit(fn['downloadLink'])
        filename = os.path.split(ftpurl.path)[1]
        fakemd5 = md5()
        fakemd5.update(filename.encode('utf-8'))
        fakemd5 = fakemd5.hexdigest()
        rawfn, _ = RawFile.objects.get_or_create(source_md5=fakemd5, defaults={
            'name': filename, 'producer': extproducers[fn['instr_type']],
            'size': fn['fileSize'], 'date': date, 'claimed': True})
        shasums[rawfn.pk] = fn['sha1sum']
        if not StoredFile.objects.filter(md5=fakemd5, checked=True).count():
            # FIXME thermo only
            ftid = StoredFileType.objects.get(name='thermo_raw_file', filetype='raw').id
            StoredFile.objects.get_or_create(rawfile=rawfn, filetype_id=ftid,
                    filename=fn, defaults={'servershare_id': tmpshare, 'path': '',
                        'md5': fakemd5})
    create_job(
        'download_px_data', dset_id=dset.id, pxacc=request.POST['px_acc'], sharename=settings.TMPSHARENAME, shasums=shasums)
    return HttpResponse()


@login_required
@require_POST
def restore_file_from_cold(request):
    '''Single file function for restoring archived files, for cases where files are not in dataset,
    e.g. on tmp storage only'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.select_related('rawfile__datasetrawfile', 'mzmlfile', 'pdcbackedupfile').get(pk=data['item_id'])
    except StoredFile.DoesNotExist:
        return JsonResponse({'error': 'File does not exist'}, status=403)
    if not sfile.purged or not sfile.deleted:
        return JsonResponse({'error': 'File is not currently marked as deleted, will not undelete'}, status=403)
    elif hasattr(sfile.rawfile, 'datasetrawfile'):
        return JsonResponse({'error': 'File is in a dataset, please restore entire set'}, status=403)
    elif not hasattr(sfile, 'pdcbackedupfile'):
        return JsonResponse({'error': 'File has no archived copy in PDC backup registered in Kantele, can not restore'}, status=403)
    elif not sfile.pdcbackedupfile.success or sfile.pdcbackedupfile.deleted:
        return JsonResponse({'error': 'Archived copy exists but cannot be restored from, check with admin'}, status=403)
    elif hasattr(sfile, 'mzmlfile'):
        return JsonResponse({'error': 'mzML derived files are not archived, please regenerate it from RAW data'}, status=403)
    # File is set to deleted, purged = False, False in the post-job-view
    create_job('restore_from_pdc_archive', sf_id=sfile.pk)
    return JsonResponse({'state': 'ok'})


@login_required
@require_POST
def archive_file(request):
    '''Single file function for archiving files, for cases where files are not in dataset,
    e.g. on tmp storage only'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.select_related('rawfile__datasetrawfile', 'filetype', 'rawfile__producer').get(pk=data['item_id'])
    except StoredFile.DoesNotExist:
        return JsonResponse({'error': 'File does not exist'}, status=404)
    except KeyError:
        return JsonResponse({'error': 'Bad request'}, status=400)
    if sfile.purged or sfile.deleted:
        return JsonResponse({'error': 'File is currently marked as deleted, can not archive'}, status=403)
    elif sfile.rawfile.claimed or hasattr(sfile.rawfile, 'datasetrawfile'):
        return JsonResponse({'error': 'File is in a dataset, please archive entire set or remove it from dataset first'}, status=403)
    elif hasattr(sfile, 'pdcbackedupfile') and sfile.pdcbackedupfile.success == True and sfile.pdcbackedupfile.deleted == False:
        return JsonResponse({'error': 'File is already archived'}, status=403)
    elif hasattr(sfile, 'mzmlfile'):
        return JsonResponse({'error': 'Derived mzML files are not archived, they can be regenerated from RAW data'}, status=403)
    create_job('create_pdc_archive', sf_id=sfile.pk, isdir=sfile.filetype.is_folder)
    # File is set to deleted,purged=True,True in the post-job-view for purge
    create_job('purge_files', sf_ids=[sfile.pk], need_archive=True)
    return JsonResponse({'state': 'ok'})
