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


def query_all_qc_files():
    '''QC files are defined as not having a dataset, being claimed, and stored on the
    QC storage dir'''
    return rm.StoredFile.objects.filter(rawfile__datasetrawfile__isnull=True, rawfile__claimed=True,
            storedfileloc__path__startswith=settings.QC_STORAGE_DIR, rawfile__qcrun__isnull=False)


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

    context = {'qc_instruments': {x['producer__pk']: x['producer__name'] for x in 
        rm.MSInstrument.objects.filter(producer__internal=True, active=True).values(
            'producer__pk', 'producer__name')},
        'peptide_sets': {tps['pk']: {'id': tps['pk'], 'frozen': tps['frozen'],
            'date': datetime.strftime(tps['date'], '%a %b %d %Y'), 'name': tps['name'],
            'active': tps['active'], 'peptides': set_peps[tps['pk']]} for tps in
            dam.TrackedPeptideSet.objects.values('pk', 'date', 'name', 'frozen', 'active')},
        'selected_pepset': last_tps,
        }
    return render(request, 'staffpage/staffpage.html', context)


@staff_member_required
@require_POST
def remove_qcfile(request):
    '''To remove a file from the longitudinal QC as it is there by mistake and should be
    in a dataset instead: claimed=F, analysis.delete, mv_to_tmp, more?
    '''
    pass


@staff_member_required
@require_POST
def new_qcfile(request):
    '''For files without QC data, where it is not known of what type they are,
    we can use this to queue new QC files'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        presfnq = rm.StoredFile.objects.filter(pk=data['sfid'])
        acqtype = dm.AcquisistionMode[data['acqtype']]
    except (KeyError, AttributeError, TypeError):
        # TypeError is for actype =list
        # AttributeError is for acqtype is not in the AcquisistionMode
        return JsonResponse({'state': 'error', 'msg': 'Something went wrong, contact admin'},
                status=400)
    # I tried with Q(claimed) | Q(filejob__job__funcname) 
    # But that got as many records as there are filejobs due to the JOIN
    # And somehow not filtering the way I wanted it (claimed OR filejob=run_longit) was used
    # so all three filejobs had claim filtering pass, even if only one had jobname filter pass.
    presfnq = presfnq.filter(checked=True, rawfile__qcrun__isnull=True)
    sfnqa = presfnq.filter(rawfile__claimed=False)
    sfnqb = presfnq.filter(filejob__job__funcname= 'run_longit_qc_workflow')
    sfnq = sfnqa.union(sfnqb)
    if sfnq.count() == 1:
        tmpshare = rm.ServerShare.objects.get(name=settings.TMPSHARENAME)
        sfn = sfnq.get()
        rm.RawFile.objects.filter(storedfile__pk=data['sfid']).update(claimed=True)
        # FIXME first is probably not a good idea here!
        sfloc = rm.StoredFileLoc.objects.filter(sfile=sfn).first()
        if sfloc.servershare == tmpshare:
            create_job('move_single_file', sfloc_id=sfloc.pk,
                    dstsharename=settings.PRIMARY_STORAGESHARENAME,
                    dst_path=os.path.join(settings.QC_STORAGE_DIR, sfn.rawfile.producer.name))
        user_op = get_operator_user()
        run_singlefile_qc(sfn.rawfile, sfloc, user_op, acqtype)
        msg = f'Queued file {sfn.filename} for QC run'
        state = 'ok'
    else:
        msg = 'Something went wrong, could not get file to run QC on, contact admin'
        state = 'error'
    if state != 'ok':
        status = 400
    else:
        status = 200
    return JsonResponse({'msg': msg, 'state': state}, status=status)


@staff_member_required
@require_POST
def rerun_singleqc(request):
    '''Reruns a single QC file. This doesnt care if there is already an analysis,
    and it will get retrieve the backed up file if needed'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        sfid = int(data['sfid'])
    except (KeyError, ValueError):
        return JsonResponse({'state': 'error', 'msg': 'Something went wrong, contact admin'}, status=400)
    sfs = query_all_qc_files().filter(pk=sfid).select_related('rawfile__qcrun',
            'rawfile__producer__msinstrument__instrumenttype')
    if sfs.count() == 1:
        user_op = get_operator_user()
        sf = sfs.get()
        # FIXME first call prob not right
        sfloc = sf.storedfileloc_set.first()
        if sf.deleted:
            # retrieve if needed
            if hasattr(sf, 'pdcbackedupfile') and sf.pdcbackedupfile.success and not sf.pdcbackedupfile.deleted:
                sfs.update(deleted=False)
                create_job('restore_from_pdc_archive', sfloc_id=sfloc.pk)
                run_singlefile_qc(sf.rawfile, sfloc, user_op,
                        dm.AcquisistionMode(sf.rawfile.qcrun.runtype))
                msg = f'Queued {sf.filename} QC raw for retrieval from archive and rerun'
                state = 'ok'
            else:
                msg = (f'QC file {sf.filename} is marked as deleted, but cannot be restored, '
                        'contact admin')
                state = 'error'
        else:
            run_singlefile_qc(sf.rawfile, sfloc, user_op,
                    dm.AcquisistionMode(sf.rawfile.qcrun.runtype))
            msg = f'Queued {sf.filename} QC raw for rerun'
            state = 'ok'
    else:
        msg = 'Something went wrong, could not get file to run QC on, contact admin'
        state = 'error'
    return JsonResponse({'msg': msg, 'state': state})


@staff_member_required
@require_POST
def rerun_qcs(request):
    '''Rerun multiple QCs, in two steps:
    1. Report on the amount and state of files
    2. Run them when confirm=true
    If there are deleted files, ask if they should be retrieved from archive,
    if there are duplicates, ask if they should be run as well
    '''
    data = json.loads(request.body.decode('utf-8'))
    try:
        assert type(data['instruments']) == list
        days_back = int(data['days'])
        instruments = [int(x) for x in data['instruments']]
        confirm_ok = data['confirm']
        ignore_dups = data['ignore_obsolete']
        retrieve_archive = data['retrieve_archive']
    except (KeyError, TypeError, ValueError, AssertionError):
        return JsonResponse({'state': 'error', 'msg': 'Something went wrong, contact admin'}, status=400)
    lastdate = (timezone.now() - timezone.timedelta(days_back)).date()
    # Filter QC files (in path, no dataset, with QCdata, claimed, date)
    sfs = query_all_qc_files().filter(rawfile__producer__pk__in=instruments,
            rawfile__date__gte=lastdate).select_related('rawfile__qcrun',
                'rawfile__producer__msinstrument__instrumenttype')
    latest_qcwf = am.NextflowWfVersionParamset.objects.filter(
            userworkflow__wftype=am.UserWorkflow.WFTypeChoices.QC).last()
    # FIXME Again, using first()
    qcjobs = [x.kwargs['sfloc_id'] for x in jm.Job.objects.filter(funcname='run_longit_qc_workflow',
        state__in=jj.JOBSTATES_WAIT, kwargs__sfloc_id__in=[x.storedfileloc_set.first().pk for x in sfs])]
    duprun_q = Q(rawfile__qcrun__analysis__nextflowsearch__nfwfversionparamset=latest_qcwf)
    retrieve_q = Q(deleted=True, pdcbackedupfile__success=True, pdcbackedupfile__deleted=False)

    if confirm_ok:
        user_op = get_operator_user()
        if not ignore_dups:
            sfs = sfs.exclude(storedfileloc__pk__in=qcjobs).exclude(duprun_q)
        deleted_files = sfs.filter(deleted=True)
        sfs = sfs.filter(deleted=False)
        retr_msg = ''
        if retrieve_archive:
            retrieve_files = deleted_files.filter(retrieve_q)
            for sf in retrieve_files:
                create_job('restore_from_pdc_archive', sf_id=sf.pk)
            sfs = sfs.union(retrieve_files)
            retr_msg = f' - Queued {retrieve_files.count()} QC raw files for retrieval from archive'
        msg = f'Queued {sfs.count()} QC raw files for running{retr_msg}'
        for sf in sfs:
            # FIXME first call too random!
            sfloc = sf.storedfileloc_set.first()
            run_singlefile_qc(sf.rawfile, sfloc, user_op,
                    dm.AcquisistionMode(sf.rawfile.qcrun.runtype))
        state = 'ok'
    else:
        without_duplicates = sfs.exclude(storedfileloc__pk__in=qcjobs).exclude(duprun_q)
        not_deleted_files = sfs.filter(deleted=False)
        archived = sfs.filter(retrieve_q)
        msg = f'You have selected {sfs.count()} QC raw files.'
        if nr_duplicates := sfs.count() - without_duplicates.count():
            msg = (f'{msg} Of these, {nr_duplicates} seem to'
            ' be obsolete reruns ran with the same workflow version as the current '
            'latest (Tick the ignore box to include these in the analysis.')
        if nr_deleted := sfs.count() - not_deleted_files.count():
            msg = (f'{msg} {nr_deleted} seem to be deleted, of which {archived.count()} are '
            ' in backup. (Tick the retrieve box to include these in the analysis.')
        msg = f'{msg} Press confirm to start the run(s)'  
        state = 'confirm'
    return JsonResponse({'msg': msg, 'state': state})


@login_required
@require_GET
def find_unclaimed_files(request):
    query = Q()
    for searchterm in [x for x in request.GET.get('q', '').split(' ') if x != '']:
        subq = Q()
        subq |= Q(filename__icontains=searchterm)
        subq |= Q(rawfile__producer__name__icontains=searchterm)
        query &= subq
    # Find checked+non-qcrun files, that are either not claimed, or have a QC job
    availableq = Q(rawfile__claimed=False) | Q(filejob__job__funcname='run_longit_qc_workflow')
    filtered = rm.StoredFile.objects.filter(checked=True, rawfile__qcrun__isnull=True).filter(
            availableq).filter(query)
    if filtered.count() > 50:
        fns = {}
    else:
        fns = {x.pk: {'id': x.pk, 'name': x.filename} for x in filtered}
    return JsonResponse(fns)


@login_required
@require_GET
def get_qc_files(request):
    query = Q()
    for searchterm in [x for x in request.GET.get('q', '').split(' ') if x != '']:
        subq = Q()
        subq |= Q(filename__icontains=searchterm)
        subq |= Q(rawfile__producer__name__icontains=searchterm)
        query &= subq
    filtered = query_all_qc_files().filter(query)
    if filtered.count() > 50:
        fns = {}
    else:
        fns = {x.pk: {'id': x.pk, 'name': x.filename} for x in filtered}
    return JsonResponse(fns)


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

