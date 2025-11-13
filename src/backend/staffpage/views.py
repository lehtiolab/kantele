import os
import json
from collections import defaultdict
from datetime import datetime

from django.utils import timezone
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.decorators import login_required
from django.contrib.admin.views.decorators import staff_member_required
from django.db.models import Q
from django.db import IntegrityError

from kantele import settings
from rawstatus import models as rm
from rawstatus.views import run_singlefile_qc, get_operator_user
from datasets import models as dm
from dashboard import models as dam
from jobs.jobutil import create_job
from analysis import models as am
from jobs import models as jm
from jobs import jobs as jj
from jobs import views as jv


@staff_member_required
@require_GET
def show_staffpage(request):
    set_peps = defaultdict(list)
    for pins in dam.PeptideInSet.objects.values('peptide__sequence', 'peptide__charge', 'peptideset_id'):
        set_peps[pins['peptideset_id']].append({'seq': pins['peptide__sequence'],
            'charge': str(pins['peptide__charge'])})
    else:
        last_peptide_set = {}
    for pset_id, peps in set_peps.items():
        set_peps[pset_id] = {ix: {'ix': ix, **pep} for ix, pep in enumerate(peps)}
    if last_tps := dam.TrackedPeptideSet.objects.filter(active=True).values('pk'):
        last_tps = last_tps.get()['pk']
    else:
        last_tps = False

    # Populate servers
    servers = {x.pk: {f: getattr(x, f) for f in ['pk', 'name', 'uri', 'fqdn', 'active',
        'can_backup', 'can_rsync_remote', 'rsyncusername', 'rsynckeyfile']}
        for x in rm.FileServer.objects.all()}
    [servers[x].update({'show_analysis_profile': False, 'mounted': []}) for x in servers.keys()]
    for asp in rm.AnalysisServerProfile.objects.all():
        servers[asp.server_id].update({'queue_name': asp.queue_name, 'scratchdir': asp.scratchdir,
            'nfprofiles': json.dumps(asp.nfprofiles), 'show_analysis_profile': True})
    for fss in rm.FileserverShare.objects.all():
        servers[fss.server_id]['mounted'].append({'share': fss.share.pk, 'path': fss.path})

    # Populate shares
    shares = [{f: getattr(x, f) for f in ['pk', 'name', 'max_security', 'description', 'active',
        'function', 'maxdays_data']} for x in rm.ServerShare.objects.all()]
    context = {'peptide_sets': {tps['pk']: {'id': tps['pk'], 'frozen': tps['frozen'],
            'date': datetime.strftime(tps['date'], '%a %b %d %Y'), 'name': tps['name'],
            'active': tps['active'], 'peptides': set_peps[tps['pk']]} for tps in
            dam.TrackedPeptideSet.objects.values('pk', 'date', 'name', 'frozen', 'active')},
        'selected_pepset': last_tps, 'sharefuns': [(x, x.label) for x in rm.ShareFunction],
        'secclass': [[x, x.label] for x in rm.DataSecurityClass],
        'servers': [x for x in servers.values()], 'shares': shares,
        }
    return render(request, 'staffpage/staffpage.html', context)


@staff_member_required
@require_POST
def remove_qcfiles(request):
    '''To remove a file from the longitudinal QC as it is there by mistake and should be
    in a dataset instead: claimed=F, analysis.delete, mv_to_tmp, more?
    - sets usetype on rawfile to RAW
    - claimed = False
    - revoke any ongoing task
    - delete analysis
    - delete sfloc from instrument QC
    '''
    data = json.loads(request.body.decode('utf-8'))
    try:
        presfnq = rm.StoredFile.objects.filter(pk__in=data['sfids'])
    except KeyError:
        return JsonResponse({'state': 'error', 'msg': 'Something went wrong, contact admin'},
                status=400)

    sfnq = presfnq.filter(rawfile__usetype=rm.UploadFileType.QC)
    if sfnq.count() == len(set(data['sfids'])):
        rm.RawFile.objects.filter(storedfile__pk__in=data['sfids']).update(claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        # FIXME path getting should probably be centralized
        if sflocs := rm.StoredFileLoc.objects.filter(sfile_id__in=data['sfids'],
                path__startswith=f'{settings.QC_STORAGE_DIR}/', active=True):
            create_job('purge_files', sfloc_ids=[x['pk'] for x in sflocs.values('pk')])
            sflocs.update(active=False)
        if jobq := jm.Job.objects.filter(
                nextflowsearch__analysis__qcrun__rawfile__storedfile__id__in=data['sfids']):
            jv.cancel_or_revoke_job(jobq)
        am.Analysis.objects.filter(qcrun__rawfile__storedfile__id__in=data['sfids']).delete()
        state = 'ok'
        msg = f'Removed {len(set(data["sfids"]))} file from QC'
    else:
        msg = 'Problem, some files you selected are not QC'
        state = 'error'
    status = 200 if state == 'ok' else 400
    return JsonResponse({'msg': msg, 'state': state}, status=status)


def retrieve_backup_to_ana_or_rsyncstor(sfid):
    errmsg = False
    bup_sfl = rm.StoredFileLoc.objects.filter(sfile_id=sfid,
            servershare__fileservershare__server__can_backup=True)
    if ana_sfl := bup_sfl.filter(servershare__fileservershare__server__analysisserverprofile__isnull=False):
        # retrieve straight to analysis share
        bupjob = create_job('restore_from_pdc_archive', sfloc_id=ana_sfl.get().pk)
        ana_sfl.update(active=True)
    elif rssfl := bup_sfl.filter(servershare__fileservershare__server__can_rsync_remote=True):
        # retrieve to rsync share and put in analysis after
        bup_sflocid = rssfl.values('pk').get()['pk']
        bupjob = create_job('restore_from_pdc_archive', sfloc_id=bup_sflocid)
        rssfl.update(active=True)
    else:
        errmsg = 'File is in backup and could not find a storage to retrieve it to'
    return errmsg


def rsync_qc_to_analysis(sfl_q):
    '''Select sfl to run QC on, if not on analysis server, queue it to one
    returns either sfl, server_id, False
    or False, False 'error message'
    '''
    fss_q = rm.FileserverShare.objects.filter(server__analysisserverprofile__isnull=False,
            server__active=True, share__function=rm.ShareFunction.RAWDATA)
    if ana_sfl := sfl_q.filter(servershare__fileservershare__server__analysisserverprofile__isnull=False,
            servershare__fileservershare__server__active=True):
        sfloc = ana_sfl.get()
        fss = fss_q.filter(share_id=sfloc.servershare_id).values('server_id').first()

    elif ana_rs_fssq := fss_q.filter(server__can_rsync_remote=True):
        fss = ana_rs_fssq.values('share_id', 'server_id').first()
        srcsfl = sfl_q.first()
        # FIXME path!
        qc_mvjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=srcsfl.pk,
            dstshare_id=fss['share_id'])
        sfloc = rm.StoredFileLoc.objects.get(pk=qc_mvjob['kwargs']['dstsfloc_id'])

    elif srcsfloc := sfl_q.filter(servershare__fileservershare__server__can_rsync_remote=True):
        fss = fss_q.values('share_id', 'server_id').first()
        qc_mvjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=srcsfloc.first().pk,
            dstshare_id=fss['share_id'])
        sfloc = rm.StoredFileLoc.objects.get(pk=qc_mvjob['kwargs']['dstsfloc_id'])

    else:
        msg = (f'Queued file {rfnq.values("name").get()["name"]} for QC run could not be found '
        'on any share with either analysis or rsync transfer capabilities')
        return False, False, msg
    return sfloc, fss['server_id'], False


@staff_member_required
@require_POST
def new_qcfiles(request):
    '''For files without QC data, where it is not known of what type they are,
    we can use this to queue new QC files'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        presfnq = rm.StoredFile.objects.filter(pk__in=data['sfids'], checked=True,
                rawfile__claimed=False)
        acqtype = dm.AcquisistionMode[data['acqtype']]
    except (KeyError, AttributeError, TypeError):
        # TypeError is for actype =list
        # AttributeError is for acqtype is not in the AcquisistionMode
        return JsonResponse({'state': 'error', 'msg': 'Something went wrong, contact admin'},
                status=400)

    # Only allow raw files to become QC
    sfnq = presfnq.filter(rawfile__usetype=rm.UploadFileType.RAWFILE)
    if sfnq.count() == len(set(data['sfids'])):
        rm.RawFile.objects.filter(storedfile__pk__in=data['sfids']).update(claimed=True,
                usetype=rm.UploadFileType.QC)
        for sfid in set(data['sfids']):
            sfl_q = rm.StoredFileLoc.objects.filter(sfile_id=sfid, active=True)
            sfloc, server_id, errmsg = rsync_qc_to_analysis(sfl_q)
            if errmsg and not sfloc:
                return JsonResponse({'msg': errmsg, 'state': 'error'}, status=400)
        user_op = get_operator_user()
        run_singlefile_qc(sfloc, server_id, user_op, acqtype)
        state = 'ok'
        msg = f'Added {len(set(data["sfids"]))} to QC'
    else:
        msg = ('Problem, some files you selected cannot be added to QC. They may already be '
                'QC type, or in a dataset')
        state = 'error'
    status = 200 if state == 'ok' else 400
    return JsonResponse({'msg': msg, 'state': state}, status=status)


@staff_member_required
@require_POST
def rerun_qcs(request):
    '''Rerun multiple QCs
    '''
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfids = data['sfids']
        confirm_ok = data['doConfirm']
    except (KeyError, TypeError, ValueError, AssertionError):
        return JsonResponse({'state': 'error', 'msg': 'Something went wrong, contact admin'}, status=400)
    # Filter QC files w no dataset, with QCdata
    sfs = rm.StoredFile.objects.filter(pk__in=sfids, rawfile__usetype=rm.UploadFileType.QC,
            rawfile__qcrun__isnull=False)
    if sfs.count() < len(sfids):
        return JsonResponse({'state': 'error', 'msg': 'You have selected files which are possibly '
            'not a QC file or have no , those cannot be rerun'}, status=400)

    if confirm_ok:
        # FIXME revoke running jobs
        user_op = get_operator_user()
        deleted_files = sfs.filter(deleted=True)
        retr_msg = ''
        retrieve_files = sfs.filter(deleted=True, pdcbackedupfile__success=True,
                pdcbackedupfile__deleted=False)
        sfs = sfs.filter(deleted=False)
        for sf in retrieve_files.values('pk'):
            if retr_msg := retrieve_backup_to_ana_or_rsyncstor(sf['pk']):
                return JsonResponse({'state': 'error', 'msg': retr_msg})
        # Not just keep sfs, some files may not be possible to retrieve
        sfs = sfs.union(retrieve_files)
        retr_msg = f' - Queued {retrieve_files.count()} QC raw files for retrieval from archive'
        msg = f'Queued {sfs.count()} QC raw files for running{retr_msg}'
        for sf in sfs.values('pk', 'rawfile__qcrun__runtype'):
            # here home
            sfloc, server_id, errmsg = rsync_qc_to_analysis(
                    rm.StoredFileLoc.objects.filter(sfile_id=sf['pk'], active=True))
            run_singlefile_qc(sfloc, server_id, user_op, dm.AcquisistionMode(sf['rawfile__qcrun__runtype']))
        state = 'ok'

    else:
        job_sflids = [x.pk for y in sfs for x in y.storedfileloc_set.all()]
        latest_qcwf = am.NextflowWfVersionParamset.objects.filter(
                userworkflow__wftype=am.UserWorkflow.WFTypeChoices.QC).last()
        qcjobs = [x.kwargs['sfloc_id'] for x in jm.Job.objects.filter(funcname='run_longit_qc_workflow',
            state__in=jj.JOBSTATES_WAIT, kwargs__sfloc_id__in=job_sflids)]
        without_duplicates = sfs.exclude(storedfileloc__pk__in=qcjobs).exclude(
                rawfile__qcrun__analysis__nextflowsearch__nfwfversionparamset=latest_qcwf)
        msg = f'You have selected {sfs.count()} QC raw files.'
        if nr_duplicates := sfs.count() - without_duplicates.count():
            msg = (f'{msg} Of these, {nr_duplicates} seem to'
            ' be obsolete reruns ran with the same workflow version as the current latest')
        msg = f'{msg} Press confirm to start the run(s)'  
        state = 'confirm'
    return JsonResponse({'msg': msg, 'state': state})


@staff_member_required
@require_POST
def save_tracked_peptides(request):
    data = json.loads(request.body.decode('utf-8'))
    acqmode = dm.AcquisistionMode.DIA
    if not data.get('tpsname'):
        return JsonResponse({'state': 'error', 'msg': 'Name is mandatory for peptide set'}, status=400)
    for pep in data['peptides']:
        if not pep.get('seq') or not pep.get('charge'):
            return JsonResponse({'state': 'error', 'msg': 'Sequence and charge are mandatory for '
                'peptides'}, status=400)
    # Will freeze current TPS in case that is asked and the data has peptides
    freeze_tps = data.get('publish', False) and len(data.get('peptides', [])) > 0
    if tpsid := data.get('tpsid', False):
        tpsq = dam.TrackedPeptideSet.objects.filter(pk=tpsid)
        if tpsq.filter(frozen=False).exists():
            if freeze_tps:
                # Deactivate any other active peptide set on publishing this one
                dam.TrackedPeptideSet.objects.filter(active=True).update(active=False)
            # Publishing sets frozen and active, old sets are frozen but not active
            try:
                tpsq.update(name=data['tpsname'], frozen=freeze_tps, active=freeze_tps)
            except IntegrityError:
                return JsonResponse({'state': 'error', 'msg': 'Name for peptide set already exist, '
                    'choose another'}, status=403)
            tps = tpsq.get()
        else:
            return JsonResponse({'state': 'error', 'msg': 'Cannot update that tracked peptide set '
                'it has likely been published already. Create a new or select a non-published set'},
                status=403)
    else:
        if freeze_tps:
            # Deactivate any other active peptide set on publishing this one
            dam.TrackedPeptideSet.objects.filter(active=True).update(active=False)
        try:
            tps = dam.TrackedPeptideSet.objects.create(name=data['tpsname'], acqmode=acqmode,
                    frozen=freeze_tps, active=freeze_tps)
        except IntegrityError:
            return JsonResponse({'state': 'error', 'msg': 'Name for peptide set already exists, '
                'choose another'}, status=403)
    dam.PeptideInSet.objects.filter(peptideset=tps).delete()
    for pep in data['peptides']:
        tpep, _ = dam.TrackedPeptide.objects.get_or_create(sequence=pep['seq'].upper(),
                charge=pep['charge'])
        dam.PeptideInSet.objects.create(peptideset=tps, peptide=tpep)
    return JsonResponse({'state': 'ok', 'data': {'id': tps.pk, 'date': tps.date,
        'frozen': tps.frozen, 'name': tps.name, 'active': tps.active}})


@staff_member_required
@require_POST
def delete_tracked_peptide_set(request):
    data = json.loads(request.body.decode('utf-8'))
    acqmode = dm.AcquisistionMode.DIA
    tpsid = data.get('tpsid')
    if not tpsid:
        return JsonResponse({'state': 'error', 'msg': 'Need to specify ID for deleting tracked '
            'peptide set'}, status=400)
    tpsq = dam.TrackedPeptideSet.objects.filter(pk=tpsid, frozen=False)
    if tpsq.exists():
        tpsq.delete()
    else:
        return JsonResponse({'state': 'error', 'msg': 'Tracked peptide set does not exist or is '
            'frozen, not possible to delete'}, status=403)
    return JsonResponse({'state': 'ok'})


@staff_member_required
@require_POST
def save_server(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        data['show_analysis_profile']
        data['pk']
    except KeyError:
        return JsonResponse({'state': 'error', 'msg': 'Invalid params passed'}, status=400)
    if data['show_analysis_profile']:
        try:
            # nfprofiles is passed as a string from the form, '["profile1", "profile2", ...]'
            data['nfprofiles'] = json.loads(data['nfprofiles'])
            data['queue_name']
        except json.decoder.JSONDecodeError:
            return JsonResponse({'state': 'error', 'msg': 'Not valid JSON for nextflow profiles'},
                    status=400)
        except KeyError:
            return JsonResponse({'state': 'error', 'msg': 'Need to enter analysis server information'},
                    status=400)
    
    if data['pk']:
        # Deactivation does not save other attributes
        if not (fs := rm.FileServer.objects.filter(pk=data['pk'])):
            return JsonResponse({'state': 'error', 'msg': 'No server for that query exists'}, status=404)
        if not data['active'] and fs.filter(active=False):
            return JsonResponse({'state': 'error', 'msg': f'Cannot update deactivated server'}, status=403)
        if not data['active'] and fs.filter(active=True):
            fs.update(active=data['active'])
            return JsonResponse({'state': 'ok', 'msg': f'Deactivated server with ID {data["pk"]}'})
        else:
            fs.update(**{f: data[f] for f in ['name', 'uri', 'fqdn', 'active', 'can_backup',
                'can_rsync_remote', 'rsyncusername', 'rsynckeyfile']})
            fs = fs.get()
    else:
        fs = rm.FileServer.objects.create(**{f: data[f] for f in ['name', 'uri', 'fqdn', 'active',
            'can_backup', 'can_rsync_remote', 'rsyncusername', 'rsynckeyfile']})

    for mount in data['mounted']:
        rm.FileserverShare.objects.update_or_create(server=fs, share_id=mount['share'],
                defaults={'path': mount['path']})
    rm.FileserverShare.objects.filter(server=fs).exclude(
            share_id__in=[x['share'] for x in data['mounted']]).delete()

    if data['show_analysis_profile']:
        rm.AnalysisServerProfile.objects.update_or_create(server=fs, defaults={
            f: data[f] for f in ['nfprofiles', 'scratchdir', 'queue_name']})
    else:
        rm.AnalysisServerProfile.objects.filter(server=fs).delete()
    return JsonResponse({'state': 'ok', 'msg': f'Saved server {fs.name} with ID {fs.pk}'})


@staff_member_required
@require_POST
def save_share(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        data['pk']
    except KeyError:
        return JsonResponse({'state': 'error', 'msg': 'Invalid params passed'}, status=400)
    if data['pk']:
        # Deactivation does not save other attributes
        if not (ss := rm.ServerShare.objects.filter(pk=data['pk'])):
            return JsonResponse({'state': 'error', 'msg': 'No share for that query exists'}, status=404)
        if not data['active'] and ss.filter(active=True):
            ss.update(active=False)
            return JsonResponse({'state': 'ok', 'msg': f'Deactivated share with ID {data["pk"]}'})
        elif not data['active'] and ss.filter(active=False):
            return JsonResponse({'state': 'error', 'msg': f'Cannot update deactivated share'})
        else:
            ss, _ = rm.ServerShare.objects.update_or_create(pk=data['pk'], defaults={f: data[f]
                for f in ['name', 'max_security', 'description', 'active', 'function',
                    'maxdays_data']})
            new = ''

    else:
        # new 
        ss  = rm.ServerShare.objects.create(**{f: data[f] for f in
            ['name', 'max_security', 'description', 'active', 'function', 'maxdays_data']})
        new = 'new '
    return JsonResponse({'state': 'ok', 'msg': f'Saved {new}share {data["name"]} with ID {ss.pk}'})
