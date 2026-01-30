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
from rawstatus.models import (RawFile, Producer, StoredFile, FileServer, ServerShare, StoredFileLoc,
        ShareFunction, FileserverShare, StoredFileType, UserFile, MSFileData, PDCBackedupFile, 
        UploadToken, UploadFileType, DataSecurityClass, FileJob)
from rawstatus import jobs as rsjobs
from rawstatus.tasks import search_raws_downloaded
from analysis.models import (Analysis, LibraryFile, AnalysisResultFile, UniProtFasta, EnsemblFasta,
        UserWorkflow)
from datasets import views as dsviews
from datasets import models as dsmodels
from dashboard import models as dashmodels
from jobs import models as jm
from jobs import jobs as jj
from jobs.jobutil import create_job, create_job_without_check, check_job_error, jobmap


UPLOAD_DESTINATIONS = {
        # FIXME when new upload script finished, remove rawfile from here (only for manual via web)
        # also remove raw uploads fom rawstatus/tests then
        UploadFileType.RAWFILE: (ShareFunction.INBOX, settings.TMPPATH),
        UploadFileType.ANALYSIS: (ShareFunction.ANALYSIS_DELIVERY, False),
        UploadFileType.LIBRARY: (ShareFunction.LIBRARY, settings.LIBRARY_FILE_PATH),
        UploadFileType.USERFILE: (ShareFunction.LIBRARY, settings.LIBRARY_FILE_PATH),
        }


def inflow_page(request):
    return render(request, 'rawstatus/inflow.html', {
        'userfile_id': UploadFileType.USERFILE,
        'rawfile_id': UploadFileType.RAWFILE,
        'library_id': UploadFileType.LIBRARY,
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
            sfile, cr = StoredFile.objects.get_or_create(rawfile_id=rawfn.pk,
                    filetype_id=extprod.msinstrument.filetype_id, filename=fn,
                    defaults={'md5': fakemd5})
            if cr:
                StoredFileLoc.objects.create(sfile=sfile, servershare_id=share.id, 
                        path=os.path.join(req['dirname'], path))
            sf_ids.append(sfile.pk)
        # Jobs to get MD5 etc
        create_job('register_external_raw', dset_id=dset.id, sf_ids=sf_ids, sharename=share.name, user_id=request.user.id)
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
    data = request.POST
    try:
        ftype = StoredFileType.objects.get(user_uploadable=True, pk=int(data['ftype_id']))
    except ValueError:
        return JsonResponse({'success': False, 'msg': 'Please select a file type '
        f'{data["ftype_id"]}'}, status=400)
    except StoredFileType.DoesNotExist:
        return JsonResponse({'success': False, 'msg': 'Illegal file type to upload'}, status=403)
    try:
        uploadtype = int(data['uploadtype'])
    except (ValueError, KeyError):
        return JsonResponse({'success': False, 'msg': 'Bad request, contact admin'}, status=400)
    desc = str(data.get('desc', '').strip())
    if desc == '':
        desc = False
        if uploadtype in [UploadFileType.LIBRARY, UploadFileType.USERFILE]:
            return JsonResponse({'success': False, 'msg': 'A description for this file is required'}, status=400)
    if ftype.is_folder:
        return JsonResponse({'success': False, 'msg': 'Cannot upload folder datatypes through browser'}, status=403)

    # create userfileupload model (incl. fake token)
    # FIXME hardcoded admin name!
    producer = Producer.objects.get(shortname='admin')
    upload = create_upload_token(ftype.pk, request.user.id, producer, uploadtype)
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
                'date': timezone.now(), 'claimed': True, 'usetype': uploadtype})
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
    _, dstpath = UPLOAD_DESTINATIONS[upload.uploadtype]
    if upload.uploadtype == UploadFileType.RAWFILE:
        fname = upfile.name
        # inbox is so broad so we upload to each raw file's own path
        dstpath = os.path.join(dstpath, raw.pk)
    elif upload.uploadtype == UploadFileType.LIBRARY:
        fname = f'{raw.pk}_{upfile.name}'
    elif upload.uploadtype == UploadFileType.USERFILE:
        fname = f'{raw.pk}_{upfile.name}'
    else:
        return JsonResponse({'success': False, 'msg': 'Can only upload files of raw, library, '
            'or user type'}, status=403)

    dstshare = ServerShare.objects.get(function=ShareFunction.INBOX)
    if upload.uploadtype == UploadFileType.RAWFILE and StoredFileLoc.objects.filter(
            sfile__filename=fname, path=dstpath, servershare=dstshare, sfile__deleted=False).exclude(
                    sfile__rawfile__source_md5=raw.source_md5).exists():
        return JsonResponse({'error': 'Another file in the system has the same name '
            f'and is stored in the same path ({dstshare.name} - {dstpath}/{fname}). '
            'Please investigate, possibly change the file name or location of this or the other '
            'file to enable transfer without overwriting.', 'problem': 'DUPLICATE_EXISTS'},
            status=403)

    # All good, get the file to storage
    sfile = StoredFile.objects.create(rawfile_id=raw.pk, filename=fname, checked=True,
            filetype=upload.filetype, md5=dighash)
    sfloc = StoredFileLoc.objects.create(sfile=sfile, servershare=dstshare, path=dstpath)
    create_job('rsync_transfer_fromweb', sfloc_id=sfloc.pk, src_path=dst)
    dstfn = process_file_confirmed_ready(sfile.rawfile, sfile, sfloc, upload, desc)
    return JsonResponse({'success': True, 'msg': 'Succesfully uploaded file to '
        f'become {dstfn} File will be accessible on storage soon.'})

    
# TODO store heartbeat of instrument, deploy config, message from backend, etc

@require_POST
def instrument_check_in(request):
    '''Returns 200 at correct token or expiring token, in which case a new token
    will be issued. Used when uploading by user'''
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
    task = jm.Task.objects.filter(asyncid=taskid).exclude(state__in=jj.JOBSTATES_DONE)

    response = {'newtoken': False}
    uploadtype = UploadFileType.RAWFILE
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
        user_op = get_operator_user()
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
            uploadtype = UploadFileType(data['uploadtype'])
        except KeyError:
            return JsonResponse({'error': True, 'error': 'Need to specify upload type, contact '
                'admin'}, status=403)
    else:
        uploadtype = UploadFileType.RAWFILE
    try:
        selected_ft = StoredFileType.objects.get(pk=data['ftype_id'])
    except StoredFileType.DoesNotExist:
        return JsonResponse({'error': True, 'error': 'Cannot use that file type'}, status=403)
    if uploadtype not in [UploadFileType.RAWFILE, UploadFileType.USERFILE, UploadFileType.LIBRARY]:
        return JsonResponse({'success': False, 'msg': 'Can only upload raw, library, user files '})

    ufu = create_upload_token(data['ftype_id'], request.user.id, producer, uploadtype)
    host = settings.KANTELEHOST or request.build_absolute_uri('/')
    return JsonResponse(ufu.parse_token_for_frontend(host))


def create_upload_token(ftype_id, user_id, producer, uploadtype):
    '''Generates a new UploadToken for a producer and stores it in DB'''
    token = str(uuid4())
    expi_sec = settings.MAX_TIME_PROD_TOKEN if producer.internal else settings.MAX_TIME_UPLOADTOKEN
    expiry = timezone.now() + timedelta(seconds=expi_sec)
    return UploadToken.objects.create(token=token, user_id=user_id, expired=False,
            expires=expiry, filetype_id=ftype_id, producer=producer,
            uploadtype=uploadtype)


# /files/transferstate
@require_POST
def get_files_transferstate(request):
    data = json.loads(request.body.decode('utf-8'))
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
    elif upload.uploadtype in [UploadFileType.LIBRARY, UploadFileType.USERFILE] and not desc:
        return JsonResponse({'error': 'Library or user files need a description'}, status=403)
    elif upload.uploadtype == UploadFileType.ANALYSIS and not hasattr(upload, 'externalanalysis'):
        # FIXME can we upload proper analysis files here too??? In theory, yes! At a speed cost
        return JsonResponse({'error': 'Analysis result uploads need an analysis_id to put them in'}, status=403)

    if not fnid:
        if upload.uploadtype == UploadFileType.ANALYSIS:
            claimed = True
        else:
            claimed = False
        rfn, _ = RawFile.objects.get_or_create(source_md5=md5, defaults={
            'name': fn, 'producer': upload.producer, 'size': size, 'date': file_date,
            'claimed': claimed, 'usetype': upload.uploadtype})
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
        # FIXME not ideal to get upload destination from code, it can change at deploy and
        # then you will not get correct behaviour - better in DB?
        up_dst = rsjobs.create_upload_dst_web(rfn.pk, sfn.filetype.filetype)
        dstsharefun, dstpath = UPLOAD_DESTINATIONS[upload.uploadtype]
        sfnss = sfn.storedfileloc_set.filter(servershare__function=dstsharefun).first()
        rsync_jobs = jm.Job.objects.filter(funcname='rsync_transfer_fromweb',
                kwargs__sfloc_id=sfnss.pk, kwargs__src_path=up_dst).order_by('timestamp')
        # fetching from DB here to avoid race condition in if/else block
        try:
            last_rsjob = rsync_jobs.last()
        except jm.Job.DoesNotExist:
            last_rsjob = False
        # Refresh to make sure we dont get race condition where it is checked
        # while we fetching jobs above and the non-checked/done job will result
        # in a retransfer
        sfn.refresh_from_db()

        sfnss = StoredFileLoc.objects.filter(sfile=sfn, servershare__function=dstsharefun).first()
        if sfn.checked:
            # File transfer and check finished
            tstate = 'done'
            has_backupjob = jm.Job.objects.filter(funcname='create_pdc_archive',
                    kwargs__sfloc_id=sfnss.pk, state__in=jj.JOBSTATES_WAIT).exists()
            if not has_backupjob and not PDCBackedupFile.objects.filter(storedfile_id=sfn.id):
                # No already-backedup PDC file, then do some processing work
                process_file_confirmed_ready(rfn, sfn, sfnss, upload, desc)
        # FIXME this is too hardcoded data model which will be changed one day,
        # needs to be in Job class abstraction!

        elif not last_rsjob:
            # There is no rsync job for this file, means it's old or somehow
            # errored # TODO how to report to user? File is also not OK checked
            tstate = 'wait'
        # FIXME elif last_rsjob.state == jj.Jobstates.ERROR: tstate = 'skip' ??
        elif last_rsjob.state not in jj.JOBSTATES_DONE:
            # File being rsynced and optionally md5checked (or it crashed, job
            # errored, revoked, wait for system or admin to catch job)
            # WARNING: this did not work when doing sfn.filejob_set.filter ?
            # A second call to this route would fire the rsync/md5 job again,
            # until the file was checked. But in theory it'd work, and by hand too.
            # Maybe a DB or cache thing, however 3seconds between calls should be enough?
            # Maybe NGINX caches stuff, add some sort of no-cache into the header of request in client producer.py
            tstate = 'wait'

        elif last_rsjob.state == jj.Jobstates.DONE:
            # MD5 on disk is not same as registered MD5, corrupted transfer
            # reset MD5 on stored file to make sure no NEW stored files are created
            # basically setting its state to pre-transfer state
            sfn.md5 = rfn.source_md5
            sfn.save()
            tstate = 'transfer'

        else:
            # There is an unlikely rsync job which is canceled, requeue it
            create_job('rsync_transfer_fromweb', sfloc_id=sfnss.pk, src_path=up_dst)
            tstate = 'wait'

    response = {'transferstate': tstate, 'fn_id': rfn.pk}
    return JsonResponse(response)


def classified_rawfile_treatment(request):
    '''Task calls this after reading a raw file for classification, rawfile being
    on inflow share'''
    data = json.loads(request.body.decode('utf-8'))
    tasks = jm.Task.objects.filter(asyncid=data['task_id'], state=taskstates.PENDING)
    # If task is force-retried, and there was another task running, that other task will
    # get 403 here
    if tasks.count() != 1:
        return HttpResponseForbidden()
    try:
        token, fnid, is_qc_acqtype, dsid = data['token'], data['fnid'], data['qc'], data['dset_id']
        mstime, error = data['mstime'], data['error']
    except KeyError as error:
        return JsonResponse({'error': 'Bad request'}, status=400)
    upload = UploadToken.validate_token(token, [])
    if not upload:
        return JsonResponse({'error': 'Token invalid or expired'}, status=403)
    sfloc = StoredFileLoc.objects.select_related('sfile__rawfile__producer',
            'sfile__filetype').get(pk=fnid)

    mstime = mstime if mstime else 0
    success = not error
    msf, cr = MSFileData.objects.get_or_create(rawfile_id=sfloc.sfile.rawfile_id,
            defaults={'mstime': mstime, 'errmsg': error, 'success': success})
    if not cr:
        # Somehow script has already run! Maybe retried classify job
        print(f'File {sfloc.sfile.filename} already classified')

    if sfloc.sfile.rawfile.claimed:
        # This file has already been classified or otherwise picked up by a fast user
        pass
    elif is_qc_acqtype:
        sfloc.sfile.rawfile.claimed = True
        sfloc.sfile.rawfile.usetype = UploadFileType.QC
        sfloc.sfile.rawfile.save()
        user_op = get_operator_user()
        if errmsg := run_singlefile_qc(StoredFileLoc.objects.filter(pk=sfloc.pk), user_op,
                dsmodels.AcquisistionMode[is_qc_acqtype]):
            return JsonResponse({'error': f'Problem transferring QC file to analysis: {errmsg}'},
                    status=400)
    elif dsid:
        # Make sure dataset exists
        dsq = dsmodels.Dataset.objects.filter(pk=dsid, locked=False)
        if not dsq.exists():
            # TODO this needs error logging? For now this is fine
            # File will not be classified and kept on upload
            print(f'Classify task error for task {data["task_id"]} - dsid {dsid} doesnt exist '
                    'or is locked')
        elif dsq.filter(datasetcomponentstate__dtcomp__component=dsmodels.DatasetUIComponent.FILES,
                datasetcomponentstate__state=dsmodels.DCStates.NEW).exists():
            # Only accept files if file component state is NEW
            # Make sure users cant use this file for something else:
            sfloc.sfile.rawfile.claimed = True
            sfloc.sfile.rawfile.save()
            # Now make job
            # FIXME return errors 
            dss_mvjobs = []
            for dss in dsq.get().datasetserver_set.filter(active=True).values('pk', 'storage_loc',
                    'storageshare_id'):
                mvjob_kw = {'dss_id': dss['pk'], 'sfloc_ids': [sfloc.pk],
                        'dstshare_id': dss['storageshare_id']}
                        
                if rsjob_error := check_job_error('rsync_dset_files_to_servershare', **mvjob_kw):
                    # TODO this needs logging
                    print(f'Classify task error for task {data["task_id"]} trying to queue '
                            f'rsync_dset_files_to_servershare - {rsjob_error}')
                else:
                    dss_mvjobs.append(mvjob_kw)

            if not rsjob_error:
                jname = 'rsync_dset_files_to_servershare'
                for mvkw in dss_mvjobs:
                    # Create job here instead of in jobs.py, because we should not create this
                    # job multiple times, esp not if user manages to put another job in 
                    # between, so filter first to see if exists
                    # get_or_create is harder since you also need to do addkwargs before
                    if not jm.Job.objects.filter(funcname=jname,
                            kwargs__sfloc_ids=mvkw['sfloc_ids'], kwargs__dss_id=mvkw['dss_id'],
                            kwargs__dstshare_id=mvkw['dstshare_id']):
                        jwrap = jobmap['rsync_dset_files_to_servershare'](False)
                        mvkw.update(jwrap.on_create_addkwargs(**mvkw))
                        jwrap.update_sourcefns_lastused(**mvkw)
                        job = jm.Job.objects.create(funcname=jname, timestamp=timezone.now(),
                                state=jj.Jobstates.HOLD, kwargs=mvkw)
                        FileJob.objects.create(rawfile_id=sfloc.sfile.rawfile.pk,
                                job_id=job.pk)
        else:
            print(f'Classify task error for task {data["task_id"]} - dataset {dsid} already has '
                    'files, more files cannot be added automatically via rawfile classification')

    if not error:
        # Errored classify-file task has possibly corrupt file, manual check required so no
        # backup yet
        create_job('create_pdc_archive', sfloc_id=sfloc.pk, isdir=sfloc.sfile.filetype.is_folder)
    updated = jm.Task.objects.filter(asyncid=data['task_id']).update(state=taskstates.SUCCESS)
    return HttpResponse()


# /files/uploaded
@require_POST
def file_uploaded(request):
    '''This is for uploading files from an instrument, so not from a user using the
    upload script. We dont use a token since the scripts run on our own server.
    '''
    data =  json.loads(request.body.decode('utf-8'))
    try:
        fn, size, md5, filedate_raw = data['fn'], data['size'], data['md5'], data['date']
        instrument_id = data['client_id']
        file_date = datetime.strftime(
            datetime.fromtimestamp(float(filedate_raw)), '%Y-%m-%d %H:%M')
        sfl_path = data['path']
    except ValueError as error:
        return JsonResponse({'error': 'Date passed to registration incorrectly formatted'}, status=400)
    except KeyError as error:
        print(f'Request to upload instrument file with missing parameter, {error}')
        return JsonResponse({'error': 'Bad request'}, status=400)

    claimed = data.get('claimed')
    # For analysis we have the server in the run, otherwise this ID is from env var 
    # in MS prod upload script:
    analysis_id = data.get('analysis_id')

    if server_id := data.get('server_id', False):
        sfl_sharepath = data['sharepath']
        try:
            fss = FileserverShare.objects.values('share__name', 'share_id').get(
                share__active=True, server__active=True, server_id=server_id, path=sfl_sharepath)
        except FileserverShare.DoesNotExist:
            return JsonResponse({'error': 'Cannot find active server to upload to by that server ID'},
                    status=400)
        share_name, share_id = fss['share__name'], fss['share_id']
    elif share_id := data.get('share_id', False):
        share_name = ServerShare.objects.values('name').get(pk=share_id)['name']
    else:
        return JsonResponse({'error': 'Bad request'}, status=400)
    producer = Producer.objects.values('pk', 'internal', 'msinstrument__filetype',
            'msinstrument__active').get(client_id=instrument_id)
    is_active_ms, analysis = False, False
    if producer['msinstrument__filetype'] is not None:
        filetype = producer['msinstrument__filetype']
        is_active_ms = producer['internal'] and producer['msinstrument__active']
        uploadtype = UploadFileType.RAWFILE
    elif instrument_id == settings.ANALYSISCLIENT_APIKEY:
        analysis = Analysis.objects.get(pk=analysis_id)
        filetype = StoredFileType.objects.get(name=settings.ANALYSIS_FT_NAME)
        uploadtype = UploadFileType.ANALYSIS
    elif data['is_library']:
        filetype = StoredFileType.objects.get(pk=data['filetype_id'])
        uploadtype = UploadFileType.LIBRARY
    else:
        return JsonResponse({'error': f'Could not identify uploading client for file {fn} on '
            'server {share__name}'}, status=403)

    # Create file entries
    rfn, raw_created = RawFile.objects.get_or_create(source_md5=md5, defaults={
        'name': fn, 'producer_id': producer['pk'], 'size': size, 'date': file_date,
        'claimed': claimed, 'usetype': uploadtype})
    sf, _ = StoredFile.objects.get_or_create(rawfile=rfn, filetype=filetype, md5=rfn.source_md5,
            defaults={'filename': fn, 'checked': True})
    sfl, _ = StoredFileLoc.objects.get_or_create(sfile=sf, servershare_id=share_id,
            defaults={'path': sfl_path, 'purged': False})
    if not raw_created:
        # If there is a raw, there will also be an SF and SFL if it has been done in this
        # method - so we can safely get_or_create those. The file exists on disk somehwere,
        # or in backup. For analysis this is accepted, but error for raw inflow
        if instrument_id != settings.ANALYSISCLIENT_APIKEY:
            return JsonResponse({'error': 'This file is already in the '
                f'system: {rfn.name}, if you are re-uploading a previously '
                'deleted file, consider reactivating from backup, or contact admin',
                'problem': 'ALREADY_EXISTS'}, status=409)

    if is_active_ms:
        # FIXME do more things here!
        create_job('classify_msrawfile', sfloc_id=sfl.pk)#, token=upload.token)
        # backup is done after the classify job (remove failing files)
    elif instrument_id == settings.ANALYSISCLIENT_APIKEY:
        AnalysisResultFile.objects.get_or_create(sfile=sf, analysis=analysis)
        rsjob, sensitive_data = False, False
        if not raw_created:
            # update timestamp on sfl: file can be from this or an older analysis 
            # result (in which case they share it)
            sfl.save()
        if sensitive_data:
            # Only dump in sens OK data storage, if that is set up, to do backups from
            # and in the delivery storage if needed
            # TODO
            #rsjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=sfl.pk,
            #    dstshare_id=dstshare['pk'], dstpath=analysis.get_public_output_dir())
            #create_job('create_pdc_archive', sfloc_id=rsjob['kwargs']['dstsfloc_id'],
            #        isdir=sf.filetype.is_folder)
            pass
        else:
            # Rsync non-sensitive data to the public data storage
            dstshare = ServerShare.objects.values('pk').get(active=True,
                    function=ShareFunction.ANALYSIS_DELIVERY)
            # Only rsync new files (most cases) or multi-analyses-shared files which are
            # not existing in dstshare yet
            if raw_created:
                rsjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=sfl.pk,
                    dstshare_id=dstshare['pk'], dstpath=analysis.get_public_output_dir())
                create_job('create_pdc_archive', sfloc_id=rsjob['kwargs']['dstsfloc_id'],
                        isdir=sf.filetype.is_folder)
    elif data.get('is_library', False):
        # Library files at this URL arrive in inbox, go to their respective shares
        # These are always fasta files from auto downloads for this view
        fa = data['is_fasta']
        # set fasta download files
        libfile = LibraryFile.objects.create(sfile=sf, description=fa['desc'])
        dbmodel = {'uniprot': UniProtFasta, 'ensembl': EnsemblFasta}[fa['dbname']]
        kwargs = {'version': fa['version'], 'libfile_id': libfile.id, 'organism': fa['organism']}
        subtype = False
        if fa['dbname'] == 'uniprot':
            subtype = UniProtFasta.UniprotClass[fa['dbtype']]
            kwargs['dbtype'] = subtype
        dbmodel.objects.create(**kwargs)
        rs_kwargs = []
        for dstshare in ServerShare.objects.filter(function=ShareFunction.LIBRARY).values('pk'):
            kwargs = {'name': 'rsync_otherfiles_to_servershare', 'sfloc_id': sfl.pk,
                    'dstshare_id': dstshare['pk'], 'dstpath': settings.LIBRARY_FILE_PATH}
            if rsjoberr := check_job_error(**kwargs):
                return JsonResponse({'error': rsjoberr}, status=401)
            rs_kwargs.append(kwargs)
        [create_job(**kwargs) for args in rs_kwargs]
        create_job('create_pdc_archive', sfloc_id=sfl.pk, isdir=sf.filetype.is_folder)
    return JsonResponse({'error': False, 'rfid': rfn.pk, 'sfid': sf.pk, 'sflid': sfl.pk,
        'path': sfl.path})


@login_required
@require_POST
def archive_file(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfid = data['item_id']
    except KeyError:
        return JsonResponse({'error': 'Parameters not passed'}, status=400)
    if sf := StoredFile.objects.filter(pk=sfid, mzmlfile__isnull=True):
        if sf.filter(pdcbackedupfile__success=True):
            return JsonResponse({'error': 'File is already in archive'}, status=400)
        elif sfl := StoredFileLoc.objects.filter(sfile_id=sfid, active=True).values('pk'):
            isdir = sf.values('filetype__is_folder').get()['filetype__is_folder']
            create_job('create_pdc_archive', sfloc_id=sfl.first()['pk'], isdir=isdir)
            if msferr := MSFileData.objects.filter(rawfile__storedfile__id=sfid, success=False):
                msferr.update(success=True)
            return JsonResponse({})
        else:
            return JsonResponse({'error': 'Cannot find copy of file on disk to archive'}, status=404)
    else:
        return JsonResponse({'error': 'File does not exist'}, status=404)


def process_file_confirmed_ready(rfn, sfn, sfloc, upload, desc):
    """Processing of backup, QC, library/userfile after transfer has succeeded
    (MD5 checked) for newly arrived MS other raw data files (not for analysis etc)
    Files that are for archiving only are also deleted from the archive share after
    backing up.
    """
    is_ms = hasattr(rfn.producer, 'msinstrument')
    is_active_ms = is_ms and rfn.producer.internal and rfn.producer.msinstrument.active
    newname = sfn.filename
    # No more RAWFILE upload via HTTP, Deprecate
    if is_active_ms and upload.uploadtype == UploadFileType.RAWFILE:
        create_job('classify_msrawfile', sfloc_id=sfloc.pk, token=upload.token)
        # No backup before the classify job etc
    else:
        if upload.uploadtype in [UploadFileType.LIBRARY, UploadFileType.USERFILE]:
            if upload.uploadtype == UploadFileType.LIBRARY:
                LibraryFile.objects.create(sfile=sfn, description=desc)
                newname = f'libfile_{sfn.libraryfile.id}_{rfn.name}'
            elif upload.uploadtype == UploadFileType.USERFILE:
                UserFile.objects.create(sfile=sfn, description=desc, upload=upload)
                newname = f'userfile_{rfn.id}_{rfn.name}'
            create_job('rename_file', sfloc_id=sfloc.pk, newname=newname)
            for ss in ServerShare.objects.exclude(pk=sfloc.servershare_id).filter(
                    function=ShareFunction.LIBRARY):
                create_job('rsync_otherfiles_to_servershare', sfloc_id=sfloc.id, dstshare_id=ss.pk,
                        dstpath=settings.LIBRARY_FILE_PATH)
        elif upload.uploadtype == UploadFileType.ANALYSIS:
            # TODO which analysis uploads go to Kantele? Skip any sens data
            # results, so possibly only reports (aggregates)
            # Currently this is for external analysis data , which will also be 
            # uploaded via sens track at times - maybe make that the only way?
            AnalysisResultFile.objects.create(sfile=sfn, analysis=upload.externalanalysis.analysis)
            # FIXME PDC is already done in transfer_file, so dont do twice
        create_job('create_pdc_archive', sfloc_id=sfloc.pk, isdir=sfn.filetype.is_folder)
    return newname


# /files/newmzml/
@require_POST
def mzml_uploaded(request):
    '''This is for uploading mzML files after conversion, so not from a user using the
    upload script. Not so much data to set since mzMLs are pre-made in DB.
    We dont use a token since the scripts run on our own server.
    '''
    data =  json.loads(request.body.decode('utf-8'))
    try:
        sflpk, md5, uploader_id = data['sflpk'], data['md5'], data['client_id']
    except ValueError as error:
        return JsonResponse({'error': 'Date passed to registration incorrectly formatted'}, status=400)
    except KeyError as error:
        print(f'Request to upload instrument file with missing parameter, {error}')
        return JsonResponse({'error': 'Bad request'}, status=400)
    if not Producer.objects.filter(client_id=uploader_id).exists():
        return JsonResponse({'error': 'Upload producer not authorized'}, status=403)
    try:
        sfl = StoredFileLoc.objects.get(pk=sflpk)
    except StoredFileLoc.DoesNotExist:
        return JsonResponse({'error': 'Cannot find pre-registered mzML file to update'}, status=404)
    sfl.sfile.md5 = md5
    sfl.sfile.checked = True
    sfl.sfile.save()
    sfl.purged = False
    sfl.save()
    return JsonResponse({'error': False})


@require_POST
def transfer_file(request):
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
    dstsharefun, dstpath = UPLOAD_DESTINATIONS[upload.uploadtype]
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
        sfnss = StoredFileLoc.objects.filter(sfile=sfn, servershare__function=dstsharefun).first()
        up_dst = rsjobs.create_upload_dst_web(rawfn.pk, sfn.filetype.filetype)
        rsync_jobs = jm.Job.objects.filter(funcname='rsync_transfer_fromweb',
                kwargs__sfloc_id=sfnss.pk, kwargs__src_path=up_dst).order_by('timestamp')
        # fetching from DB here to avoid race condition in if/else block
        try:
            last_rsjob = rsync_jobs.last()
        except jm.Job.DoesNotExist:
            last_rsjob = False
        if not last_rsjob:
            return JsonResponse({'error': 'This file is already in the '
                f'system: {sfns.first().filename}, but there is no job to put it in the '
                'storage. Please contact admin', 'problem': 'NO_RSYNC'}, status=409)
        elif last_rsjob.state not in jj.JOBSTATES_DONE:
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
    # Now prepare file system info, check if duplicate name exists:
    check_dup = False
    if upload.uploadtype == UploadFileType.RAWFILE:
        check_dup = True
    elif upload.uploadtype == UploadFileType.ANALYSIS:
        # FIXME what if sens data -> only go to specific shares, not delivery
        dstpath = upload.externalanalysis.analysis.get_public_output_dir()
    elif upload.uploadtype == UploadFileType.LIBRARY:
        # Make file names unique because harder to control external files
        fname = f'{rawfn.pk}_{fname}'
    elif upload.uploadtype == UploadFileType.USERFILE:
        # Make file names unique because harder to control external files
        fname = f'{rawfn.pk}_{fname}'
    else:
        return JsonResponse({'error': f'Upload has an invalid uploadtype ID ({upload.uploadtype}). '
            'This should not happen, contact admin'}, status=403)

    if check_dup and StoredFileLoc.objects.filter(sfile__filename=nonzip_fname, purged=False,
            path=dstpath, servershare__function=dstsharefun).exclude(
                    sfile__rawfile__source_md5=rawfn.source_md5).exists():
        return JsonResponse({'error': 'Another file in the system has the same name '
            f'and is stored in the same path ({dstpath}/{nonzip_fname}). '
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
            return JsonResponse({'error': f'Failed to upload file, checksum {dighash} differs from '
                f'expected {rawfn.source_md5}, possibly corrupted in transfer or changed on local disk',
                'state': 'error'}, status=409)
    os.chmod(upload_dst, 0o644)
    # FIXME will need to pass share!
    file_trf, created = StoredFile.objects.update_or_create(rawfile=rawfn, filetype=upload.filetype,
            md5=rawfn.source_md5, defaults={'filename': fname})
    if not created:
        # Is this possible? Above checking with sfns.count() for both checked and non-checekd
        print('File already registered as transferred')
        dstsss = StoredFileLoc.objects.get(sfile=file_trf, servershare__function=dstsharefun)
    else:
        # Now transfer to dst share (library share on controller)
        dstshare = ServerShare.objects.filter(function=dstsharefun).first()
        dstsss = StoredFileLoc.objects.create(sfile=file_trf, servershare=dstshare, path=dstpath)
    create_job('rsync_transfer_fromweb', sfloc_id=dstsss.pk, src_path=upload_dst)
    return JsonResponse({'fn_id': fn_id, 'state': 'ok'})


def rsync_qc_to_analysis(sfl_q, nfwfvid):
    '''Select sfl to run QC on, if not on analysis server, queue it to one.
    Returns either (sfl, server_id, False)
    OR (False, False 'error message')
    '''
    fss_q = FileserverShare.objects.filter(share__function=ShareFunction.RAWDATA,
            server__analysisserverprofile__nfconfigfile__nfpipe_id=nfwfvid, server__active=True,
            share__active=True)
    if ana_sfl := sfl_q.filter(servershare__fileservershare__in=fss_q):
        # File is on analysis server with QC pipe 
        sfloc = ana_sfl.first()
        fss = fss_q.filter(share_id=sfloc.servershare_id).values('server_id').first()

    elif ana_rs_fssq := fss_q.filter(server__can_rsync_remote=True):
        # We can pull file to analysis server with QC pipe
        fss = ana_rs_fssq.values('share_id', 'server_id').first()
        srcsfl = sfl_q.first()
        dstpath = os.path.join(settings.QC_STORAGE_DIR, srcsfl.sfile.rawfile.producer.name)
        # FIXME path!
        qc_mvjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=srcsfl.pk,
            dstshare_id=fss['share_id'], dstpath=dstpath)
        sfloc = StoredFileLoc.objects.get(pk=qc_mvjob['kwargs']['dstsfloc_id'])

    elif srcsfloc := sfl_q.filter(servershare__fileservershare__server__can_rsync_remote=True):
        # We can push file to analysis server with QC
        fss = fss_q.values('share_id', 'server_id').first()
        srcsfl = srcsfloc.first()
        dstpath = os.path.join(settings.QC_STORAGE_DIR, srcsfl.sfile.rawfile.producer.name)
        qc_mvjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=srcsfl.pk,
            dstshare_id=fss['share_id'], dstpath=dstpath)
        sfloc = StoredFileLoc.objects.get(pk=qc_mvjob['kwargs']['dstsfloc_id'])
    else:
        msg = (f'Queued file {sfl_q.values("storedfile__filename").get()["storedfile__filename"]} '
                'for QC run could not be found on any share with either analysis or rsync transfer '
                'capabilities')
        return False, False, msg
    return sfloc, fss['server_id'], False


def run_singlefile_qc(sfloc_q, user_op, acqtype):
    """This method is only run for detecting new incoming QC files"""
    wf = UserWorkflow.objects.filter(wftype=UserWorkflow.WFTypeChoices.QC).last()
    nfwfvid = wf.nfwfversionparamsets.values('pk').last()['pk']
    sfloc, server_id, rsync_errmsg = rsync_qc_to_analysis(sfloc_q, nfwfvid)
    if rsync_errmsg and not sfloc:
        return rsync_errmsg

    rawfile = sfloc.sfile.rawfile
    params = ['--instrument', rawfile.producer.msinstrument.instrumenttype.name,
            f'--{acqtype.name.lower()}']
    analysis, _ = Analysis.objects.update_or_create(user_id=user_op.user_id,
            name=f'{rawfile.producer.name}_{rawfile.name}_{rawfile.date}', defaults={
                'log': [], 'deleted': False, 'purged': False, 'base_rundir': '', 'editable': False,
                'securityclass': DataSecurityClass.NOSECURITY})
    qcrun, _ = dashmodels.QCRun.objects.update_or_create(rawfile=rawfile, defaults={'is_ok': False,
        'message': '', 'runtype': acqtype, 'analysis': analysis})
    tps = dashmodels.TrackedPeptideSet.objects.filter(active=True, acqmode=acqtype).order_by('-date').first()
    trackpeps = [[x['peptide__pk'], x['peptide__sequence'], x['peptide__charge']] for x in
            dashmodels.PeptideInSet.objects.filter(peptideset=tps).values('peptide__pk',
            'peptide__sequence', 'peptide__charge')]
    nfconfig = LibraryFile.objects.filter(nfconfigfile__serverprofile__server_id=server_id,
            nfconfigfile__nfpipe_id=nfwfvid).values('sfile_id').get()['sfile_id']
    create_job('run_longit_qc_workflow', sfloc_id=sfloc.id, analysis_id=analysis.id, wf_id=wf.pk,
            nfwfvid=nfwfvid, fserver_id=server_id, nfconfig_id=nfconfig, qcrun_id=qcrun.pk,
            params=params, trackpeptides=trackpeps)
    return False


def get_file_owners(sfile):
    owners = {x.id for x in User.objects.filter(is_superuser=True)}
    if hasattr(sfile.rawfile, 'datasetrawfile'):
        owners.update(dsviews.get_dataset_owners_ids(sfile.rawfile.datasetrawfile.dataset))
    elif hasattr(sfile, 'analysisresultfile'):
        owners.add(sfile.analysisresultfile.analysis.user_id)
    elif hasattr(sfile, 'userfile'):
        owners.add(sfile.userfile.upload.user_id)
    return owners
 

@login_required
def rename_file(request):
    """Renames a single file. This checks if characters are correct, launches job
    with bare filename (no extension), since job determines if mutliple files including
    mzML have to be renamed. The file needs renaming in ALL shares"""
    if not request.method == 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    data =  json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.filter(pk=data['sf_id'], deleted=False).select_related(
            'mzmlfile', 'rawfile').get()
        newfilename = os.path.splitext(data['newname'])[0]
    except (StoredFile.DoesNotExist, KeyError):
        print('Stored file to rename does not exist')
        return JsonResponse({'error': 'File does not exist'}, status=403)
    if request.user.id not in get_file_owners(sfile):
        return JsonResponse({'error': 'Not authorized to rename this file'}, status=403)
    elif hasattr(sfile, 'mzmlfile'):
        return JsonResponse({'error': 'Files of this type cannot be renamed'}, status=403)
    elif re.match('^[a-zA-Z_0-9\-]*$', newfilename) is None:
        return JsonResponse({'error': 'Illegal characteres in new file name'}, status=403)
    jobkws = []
    for sfloc in StoredFileLoc.objects.filter(sfile__rawfile__storedfile=sfile, active=True
            ).select_related('sfile__mzmlfile'):
        # go through rawfile to also rename mzml files
        refined = '_refined' if hasattr(sfloc.sfile, 'mzmlfile') and sfloc.sfile.mzmlfile.refined else ''
        fn_ext = os.path.splitext(sfloc.sfile.filename)[1]
        newname = f'{newfilename}{refined}{fn_ext}'
        jobkw = {'sfloc_id': sfloc.pk, 'newname': newname}
        joberr = check_job_error('rename_file', **jobkw)
        if joberr:
            return JsonResponse({'error': joberr}, status=403)
        jobkws.append(jobkw)
    for jobkw in jobkws:
        create_job_without_check('rename_file', **jobkw)
    rawfn_ext = os.path.splitext(sfile.rawfile.name)[1]
    newrawname = f'{newfilename}{rawfn_ext}'
    RawFile.objects.filter(storedfile=sfile).update(name=newname)
    return JsonResponse({})


@login_required
def update_sharedfile_description(request):
    """Changes description of a single library or user file. This checks if 
    characters are correct, rights ok"""
    if not request.method == 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    data =  json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.filter(pk=data['sf_id'], deleted=False).get()
        desc = data['desc']
    except StoredFile.DoesNotExist:
        return JsonResponse({'error': 'File does not exist'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Incorrectly called description update'}, status=400)

    if request.user.id not in get_file_owners(sfile):
        return JsonResponse({'error': 'Not authorized to update description of this file'}, status=403)

    if lf := LibraryFile.objects.filter(sfile=sfile):
        lf.update(description=desc)
    elif uf := UserFile.objects.filter(sfile=sfile):
        uf.update(description=desc)
    else:
        return JsonResponse({'error': 'File is not a shared file'}, status=403)
    return JsonResponse({'msg': 'Updated file description'})


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
            'filetype_ext': prod.msinstrument.filetype.filetype,
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
        return JsonResponse({'error': str(error)}, status=500)
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
            sfile, cr = StoredFile.objects.get_or_create(rawfile=rawfn, filetype_id=ftid,
                    filename=fn, defaults={'md5': fakemd5})
            if cr:
                StoredFileLoc.objects.create(sfile=sfile, servershare_id=tmpshare, path='')
    create_job(
        'download_px_data', dset_id=dset.id, pxacc=request.POST['px_acc'], sharename=settings.TMPSHARENAME, shasums=shasums, user_id=request.user.id)
    return HttpResponse()


@login_required
@require_POST
def update_sfile_storage(request):
    '''Storage update including restoring from archive) for individual files
    Restores to 'share_ids': [1,2,3]
    '''
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.select_related('pdcbackedupfile', 'rawfile').get(pk=data['item_id'])
        share_ids = data['share_ids']
    except StoredFile.DoesNotExist:
        return JsonResponse({'error': 'File does not exist'}, status=404)
    except KeyError:
        return JsonResponse({'error': 'Parameters not passed'}, status=400)
    if not share_ids:
        return JsonResponse({'error': 'Must select shares to update to, please select '
            'at least one or press delete instead'}, status=403)

    existing_sfl = sfile.storedfileloc_set.filter(active=True).values('pk', 'servershare_id')
    existing_shares = [(x['pk'], x['servershare_id']) for x in existing_sfl]
    if not set(x[1] for x in existing_shares).symmetric_difference(share_ids):
        return JsonResponse({'error': f'File with ID {data["item_id"]} is already '
            'on the server shares you want'}, status=400)


    def get_path(sfile, dstshare_id, dss, analysis):
        # Get dst path depending on file and share
        if dss:
            # For raw files, and mzML, and refined mzML
            dstpath = dss.filter(storageshare_id=dstshare_id).values(
                    'storage_loc_ui').get()['storage_loc_ui']
        elif sfile.rawfile.usetype == UploadFileType.QC:
            # QC raw files
            dstpath = settings.QC_STORAGE_DIR
        elif sfile.rawfile.usetype in [UploadFileType.LIBRARY, UploadFileType.USERFILE]:
            # Library/user files, if they go to raw file storage (e.g. on remote server)
            # make sure they get special __library path
            dstsharefun = ServerShare.objects.values('function').get(pk=dstshare_id)['function']
            if dstsharefun == ShareFunction.LIBRARY:
                dstpath = settings.LIBRARY_FILE_PATH
            else:
                dstpath = settings.LIBRARY_FILE_PATH_INBOX
        elif analysis:
            # Analysis results but not those from mzML converter/refiner
            dstshare = ServerShare.objects.values('function').get(pk=dstshare_id)
            if dstshare['function'] == ShareFunction.ANALYSIS_DELIVERY:
                dstpath = analysis.get_public_output_dir()
            else:
                dstpath = analysis.base_rundir
        elif (ServerShare.objects.values('function').get(pk=dstshare_id)['function'] 
                == ShareFunction.INBOX and sfile.rawfile.usetype == UploadFileType.RAWFILE):
            # file without dataset, e.g. from backup to tmp
            dstpath = ServerShare.get_inbox_path()
        else:
            raise RuntimeError('Cannot determine path to update file to')
        return dstpath

    if sfile.get_allowed_shares().filter(pk__in=share_ids).count() < len(share_ids):
        return JsonResponse({'error': 'Invalid share requested to move file to'}, status=400)

    if rsync_server := FileServer.objects.filter(can_rsync_remote=True, active=True):
        rsync_server = rsync_server.values('pk').first()
    else:
        return JsonResponse({'error': 'No server available to transfer file, likely '
            'an incorrect configuration, please contact admin'}, status=400)

    deleted = sfile.deleted and not len(existing_shares)
    dset_id, analysis, dss = False, False, False
    if deleted and not PDCBackedupFile.objects.filter(storedfile=sfile, success=True,
            deleted=False).exists():
        return JsonResponse({'error': 'File has no archived copy in PDC backup registered in '
            'Kantele, can not restore'}, status=403)

    elif not deleted and (sfile.deleted or not len(existing_shares)):
        return JsonResponse({'error': 'File is not marked as deleted but no copies can be found '
            'in DB to exist on filesystem. Please contact admin.'}, status=400)

    elif analysis_q := Analysis.objects.filter(analysisresultfile__sfile=sfile):
        analysis = analysis_q.first()
        # FIXME securityclass check when we have that!

    elif hasattr(sfile.rawfile, 'datasetrawfile'):
        if not deleted or not (dss := dsmodels.DatasetServer.objects.filter(active=True,
                    dataset__datasetrawfile__rawfile=sfile.rawfile)):
            # in dset -> only if file cold and there is dset on disk
            return JsonResponse({'error': 'File is in a dataset, please update entire set'}, status=403)
        if len(share_ids) > dsmodels.DatasetServer.objects.filter(storageshare_id__in=share_ids,
                dataset__datasetrawfile__rawfile=sfile.rawfile, active=True).count():
            # cannot move to location where there is no dss
            # already done the max_security check)
            return JsonResponse({'error': 'You cannot move dataset-associated files to where their '
                'dataset is not, move the dataset instead'}, status=403)

        dset_id = dss.values('dataset_id').first()['dataset_id']

    retrieve_count = 0
    if deleted:
        # Select a place to do restore to, then start restore job:
        sfloc_q = sfile.storedfileloc_set.filter(servershare__fileservershare__server__can_backup=True,
                servershare__active=True, servershare__fileservershare__server__active=True)
        if sfl_bup_q := sfloc_q.filter(servershare_id__in=share_ids):
            # determine where to back up to, preferably a place the user wants to
            src_sfl = sfl_bup_q.first()
            retrieve_count = 1
        elif sfl_inbox_q := sfloc_q.filter(servershare__function=ShareFunction.INBOX):
            # if that is not avail, pick inbox
            src_sfl = sfl_inbox_q.first()
        elif sfloc_q.exists():
            # Otherwise take first historical available share with backup capabilities
            src_sfl = sfloc_q.first()
        else:
            return JsonResponse({'error': 'There is no copy of a file on a storage server with backup '
                'function, or maybe the servers are not correctly registered'}, status=400)
        create_job('restore_from_pdc_archive', sfloc_id=src_sfl.pk)
        src_sfl.active = True
        src_sfl.save()
        src_sfl_id = src_sfl.pk
        remaining_share_ids = [x for x in share_ids if x != src_sfl.servershare_id]
        sfile.deleted = False
        sfile.save()

    elif src_sfl_q := sfile.storedfileloc_set.filter(active=True, servershare__active=True, 
            servershare__fileservershare__server__can_rsync_remote=True,
            servershare__fileservershare__server__active=True):
        # We have an accessible file to distribute on rsync-capable server
        src_sfl = src_sfl_q.values('pk', 'servershare_id').first()
        src_sfl_id = src_sfl['pk']
        remaining_share_ids = [x for x in share_ids if x != src_sfl['servershare_id']]

    else:
        # File only on remote, rsync to first rsync capable server
        src_sfl = sfile.storedfileloc_set.filter(active=True, servershare__active=True, 
            servershare__fileservershare__server__active=True).values('servershare_id').first()
        srcserver = FileserverShare.objects.filter(share_id=src_sfl['servershare_id'],
                ).values('server_id').first()
        dstshare_classes = ServerShare.classify_shares_by_rsync_reach([x for x in share_ids],
                srcserver['server_id'])
        if dstshare_classes['rsync_sourcable']:
            rs_src_dstshare_id = dstshare_classes['rsync_sourcable'][0]
            rs_src_dstpath = get_path(sfile, rs_src_dstshare_id, dss, analysis)

        elif dstshare_classes['remote']:
            # need to rsync to INBOX (as it is guaranteed to be on an rsyncing capable
            # controller (and sensitive classed)
            rs_src_dstshare_id = ServerShare.objects.filter(function=ShareFunction.INBOX
                    ).values('pk').first()['pk']
            rs_src_dstpath = get_path(sfile, rs_src_dstshare_id, dss, analysis)

        else:
            return JsonResponse({'error': 'There is no copy of a file on a storage server which '
                'can be reached, or maybe the servers are not correctly registered'}, status=400)

        rs_src_job = create_job('rsync_otherfiles_to_servershare', sfloc_id=existing_shares[0][0],
                    dstshare_id=rs_src_dstshare_id, dstpath=rs_src_dstpath)
        src_sfl_id = rs_src_job['kwargs']['dstsfloc_id']
        remaining_share_ids = [x for x in share_ids if x != rs_src_job['kwargs']['dstshare_id']]

    # Now distribute to remaining dst shares
    rsync_shares =  [x for x in remaining_share_ids if x not in [x[1] for x in existing_shares]]
    for share_id in rsync_shares:
        dstpath = get_path(sfile, share_id, dss, analysis)
        create_job('rsync_otherfiles_to_servershare', sfloc_id=src_sfl_id, dstshare_id=share_id,
                dstpath=dstpath)

    # Delete files from excluded shares
    if rm_sfls := sfile.storedfileloc_set.filter(active=True).exclude(servershare__in=share_ids):
        create_job('purge_files', sfloc_ids=[x['pk'] for x in rm_sfls.values('pk')])
    inbox_rm_count = rm_sfls.filter(servershare__function=ShareFunction.INBOX).count()
    del_count = rm_sfls.update(active=False) - inbox_rm_count
    # del_count is for user, do subtract the "deleted from inbox after retrieve from backup"

    return JsonResponse({'state': 'ok', 'msg': f'File with ID {data["item_id"]} queued for syncing '
        f'to {len(rsync_shares) + retrieve_count} shares, and deletion from {del_count} shares'})


@login_required
@require_POST
def delete_file(request):
    '''Single file function for deleting files from disk, backing them up if that has not
    been done yet'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfile = StoredFile.objects.select_related('rawfile__datasetrawfile', 'filetype', 'rawfile__producer').get(pk=data['item_id'], deleted=False)
    except StoredFile.DoesNotExist:
        return JsonResponse({'error': 'File does not exist, maybe it is deleted?'}, status=404)
    except KeyError:
        return JsonResponse({'error': 'Bad request'}, status=400)
    sfloc_q = StoredFileLoc.objects.filter(sfile=sfile, active=True)
    analysis_id, dset_id = False, False
    if not sfloc_q.exists():
        return JsonResponse({'error': 'File is possibly deleted, but not marked as such, please '
            'inform admin -- can not archive'}, status=403)

    elif hasattr(sfile.rawfile, 'datasetrawfile'):
        dset = dsmodels.Dataset.objects.filter(datasetrawfile__rawfile=sfile.rawfile).values('pk',
                'runname__experiment__project__ptype_id', 'deleted').get()
        if not data.get('force'):
            return JsonResponse({'error': 'File is in a dataset, force delete, archive entire set, '
                'or remove it from dataset first'}, status=402)
        elif dsviews.check_ownership(request.user, dset['runname__experiment__project__ptype_id'],
                dset['pk'], dset['deleted']):
            dset_id = dset['pk']
        else:
            return JsonResponse({'error': 'File is in a dataset which you are not authorized to '
                'change'}, status=403)

    elif sfile.analysisresultfile_set.exists():
        analysis = sfile.analysisresultfile_set.first().analysis
        analysis_id = analysis.pk
        if not analysis.user == request.user and not request.user.is_staff:
            return JsonResponse({'error': 'You are not authorized to delete this analysis file'},
                    status=403)

    elif sfile.rawfile.usetype == UploadFileType.USERFILE and not UserFile.objects.filter(
            upload__user=request.user, sfile=sfile).exists() and not request.user.is_staff:
        return JsonResponse({'error': 'File is a userfile which you are not authorized to '
                'remove'}, status=403)

    elif not request.user.is_staff:
        # if not analysis, dset, userfile: library, QC, loose rawfile, need staff status
        ft = UploadFileType(sfile.rawfile.usetype).label
        return JsonResponse({'error': f'You are not authorized to remove files of type {ft}'},
                status=403)

    missing_backup = (not hasattr(sfile, 'pdcbackedupfile') or not sfile.pdcbackedupfile.success or 
            sfile.pdcbackedupfile.deleted) and not data.get('noarchive', False)
    if not hasattr(sfile, 'mzmlfile') and missing_backup:
        if sfl_q_bup := sfloc_q.filter(servershare__fileservershare__server__can_backup=True,
                servershare__active=True, servershare__fileservershare__server__active=True):
            # File on backup-available share, archive
            sflocid = sfl_q_bup.values('pk').first()['pk']
        else:
            # First move file to inbox before doing a backup as it is not available
            sflocid = sfloc_q.values('pk').first()['pk']
            inbox = ServerShare.objects.filter(function=ShareFunction.INBOX, active=True).values(
                    'pk').first()
            inboxpath = ServerShare.get_inbox_path(dset_id=dset_id, analysis_id=analysis_id)
            rsjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=sflocid,
                    dstpath=inboxpath, dstshare_id=inbox['pk'])
            sflocid = rsjob['kwargs']['dstsfloc_id']
        create_job('create_pdc_archive', sfloc_id=sflocid, isdir=sfile.filetype.is_folder)
    # sfloc is set to purged=True in the post-job-view for purge
    sfile.deleted = True
    sfile.save()
    create_job('purge_files', sfloc_ids=[x['pk'] for x in sfloc_q.values('pk')])
    sfloc_q.update(active=False)
    return JsonResponse({'state': 'ok'})


def get_operator_user():
    '''Return Operator who is a staff user'''
    staff_ops = dsmodels.Operator.objects.filter(user__is_staff=True)
    if staff_ops.exists():
        user_op = staff_ops.first()
    else:
        user_op = dsmodels.Operator.objects.first()
    return user_op
