import json

from django.utils import timezone
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.decorators import login_required
from django.contrib.admin.views.decorators import staff_member_required
from django.db.models import Q

from kantele import settings
from rawstatus import models as rm
from rawstatus.views import run_singlefile_qc, query_all_qc_files
from datasets import models as dm
from jobs.jobutil import create_job
from analysis import models as am
from jobs import models as jm
from jobs import jobs as jj


@staff_member_required
@require_GET
def show_staffpage(request):
    context = {'qc_instruments': {x['producer__pk']: x['producer__name'] for x in 
        rm.MSInstrument.objects.filter(producer__internal=True, active=True).values(
            'producer__pk', 'producer__name')}}
    return render(request, 'staffpage/staffpage.html', context)


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
    sfs = query_all_qc_files().filter(pk=sfid).select_related('rawfile__qcdata',
            'rawfile__producer__msinstrument__instrumenttype')
    if sfs.count() == 1:
        # Get user for analysis (first Operator which is staff)
        staff_ops = dm.Operator.objects.filter(user__is_staff=True)
        if staff_ops.exists():
            user_op = staff_ops.first()
        else:
            user_op = dm.Operator.objects.first()
        sf = sfs.get()
        if not hasattr(sf.rawfile, 'qcdata'):
            msg = (f'File {sf.filename} is annotated as QC but has not been run previously and has '
            'no acquisition mode stored. Please queue manually')
            state = 'error'

        elif sf.deleted:
            # retrieve if needed
            if hasattr(sf, 'pdcbackedupfile') and sf.pdcbackedupfile.success and not sf.pdcbackedupfile.deleted:
                create_job('restore_from_pdc_archive', sf_id=sf.pk)
                run_singlefile_qc(sf.rawfile, sf, user_op,
                        dm.AcquisistionMode(sf.rawfile.qcdata.runtype).name)
                msg = f'Queued {sf.filename} QC raw for retrieval from archive and rerun'
                state = 'ok'
            else:
                msg = (f'QC file {sf.filename} is marked as deleted, but cannot be restored, '
                        'contact admin')
                state = 'error'
        else:
            run_singlefile_qc(sf.rawfile, sf, user_op,
                    dm.AcquisistionMode(sf.rawfile.qcdata.runtype).name)
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
    # Filter QC files (in path, no dataset, claimed, date)
    sfs = query_all_qc_files().filter(rawfile__producer__pk__in=instruments,
            rawfile__date__gte=lastdate).select_related('rawfile__qcdata',
                'rawfile__producer__msinstrument__instrumenttype')
    latest_qcwf = am.NextflowWfVersionParamset.objects.filter(
            userworkflow__wftype=am.UserWorkflow.WFTypeChoices.QC).last()
    qcjobs = [x.kwargs['sf_id'] for x in jm.Job.objects.filter(funcname='run_longit_qc_workflow',
        state__in=jj.JOBSTATES_WAIT, kwargs__sf_id__in=[x.pk for x in sfs])]
    duprun_q = Q(rawfile__qcdata__analysis__nextflowsearch__nfwfversionparamset=latest_qcwf)
    retrieve_q = Q(deleted=True, pdcbackedupfile__success=True, pdcbackedupfile__deleted=False)

    if confirm_ok:
        # Get user for analysis (first Operator which is staff)
        staff_ops = dm.Operator.objects.filter(user__is_staff=True)
        if staff_ops.exists():
            user_op = staff_ops.first()
        else:
            user_op = dm.Operator.objects.first()
        if not ignore_dups:
            sfs = sfs.exclude(pk__in=qcjobs).exclude(duprun_q)
        if nodata_nr := sfs.filter(rawfile__qcdata__isnull=True).count():
            msg = (f'Ignored {nodata_nr} files which were labeled as QC but had no run data, '
            'and thus no acquisition mode stored - please find these manually')
        sfs = sfs.filter(rawfile__qcdata__isnull=False)
        deleted_files = sfs.filter(deleted=True)
        sfs = sfs.filter(deleted=False)
        retr_msg = ''
        if retrieve_archive:
            retrieve_files = deleted_files.filter(retrieve_q)
            for sf in retrieve_files:
                create_job('restore_from_pdc_archive', sf_id=sf.pk)
            sfs = sfs.union(retrieve_files)
            retr_msg = f' - Queued {retrieve_files.count()} QC raw files for retrieval from archive'
        msg = f'Queued {sfs.count()} QC raw files for running{retr_msg}. {msg}'
        for sf in sfs:
            run_singlefile_qc(sf.rawfile, sf, user_op,
                    dm.AcquisistionMode(sf.rawfile.qcdata.runtype).name)
        state = 'ok'
    else:
        without_duplicates = sfs.exclude(pk__in=qcjobs).exclude(duprun_q)
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
        if nodata_nr := sfs.filter(rawfile__qcdata__isnull=True).count():
            msg = (f'{msg}. Ignored {nodata_nr} files which were labeled as QC but had no run data, '
            'and thus no acquisition mode stored - please find these manually')
        msg = f'{msg} Press confirm to start the run(s)'  
        state = 'confirm'
    return JsonResponse({'msg': msg, 'state': state})


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
