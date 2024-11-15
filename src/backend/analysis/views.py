import re
import os
import json
from datetime import datetime, timedelta
from collections import defaultdict
from uuid import uuid4

from django.utils import timezone
from django.http import (HttpResponseForbidden, HttpResponse, JsonResponse,
        HttpResponseNotFound)
from django.db import IntegrityError
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
from django.shortcuts import render
from django.db.models import Q, Count

from kantele import settings
from analysis import models as am
from analysis import jobs as aj
from datasets import models as dm
from datasets.views import move_dset_project_servershare
from rawstatus import models as rm
from rawstatus.views import create_upload_token
from home import views as hv
from jobs import jobs as jj
from jobs import views as jv
from jobs.jobutil import create_job
from jobs import models as jm


@login_required
@require_GET
def get_analysis_init(request):
    '''New page, empty analysis, only gets dataset ids'''
    dserrors = []
    try:
        dsids = request.GET['dsids'].split(',')
    except (KeyError, ValueError):
        dsids = [] 
    dbdsets = dm.Dataset.objects.filter(pk__in=dsids).select_related('runname__experiment__project')
    deleted = dbdsets.filter(deleted=True).count()
    if deleted:
        dserrors.append('Deleted datasets can not be analysed')
    if dbdsets.filter(deleted=False).count() + deleted < len(dsids):
        dserrors.append('Some datasets could not be found, they may not exist')
    dsets = {d.pk: format_dset_tag(d) for d in dbdsets}
    context = {'dsets': dsets, 'ds_errors': dserrors, 'analysis': False, 'wfs': get_allwfs()}
    return render(request, 'analysis/analysis.html', context)


@login_required
def load_analysis_resultfiles(request, anid):
    # FIXME need test
    '''Load the "resultfiles" that are output from previous analyses, if user asks for 
    specific previous analysis.
    Exclude here the files from analyses which have identical datasets (dsids)
    and files from base_analyses loaded for this analysis. These files will
    already be loaded in the UI.'''
    try:
        ana = am.Analysis.objects.select_related('nextflowsearch__workflow').get(pk=anid)
    except am.Analysis.DoesNotExist:
        return JsonResponse({'error': 'Base analysis not found'}, status=403)
    analysis_date = datetime.strftime(ana.date, '%Y-%m-%d')
    try:
        dsids = [int(x) for x in request.GET['dsids'].split(',')]
    except ValueError:
        dsids = []
    base_ana_id = int(request.GET['base_ana']) or False
    analysis_prev_resfiles_ids = get_prev_resultfiles(dsids, only_ids=True)
    if base_ana_id:
        base_ana_resfiles_ids = [x.sfile_id for x in am.AnalysisResultFile.objects.filter(
            analysis_id=base_ana_id).exclude(sfile_id__in=analysis_prev_resfiles_ids)]
    else:
        base_ana_resfiles_ids = []
    already_loaded_files = analysis_prev_resfiles_ids + base_ana_resfiles_ids
    if hasattr(ana, 'nextflowsearch'):
        wftype = ana.nextflowsearch.workflow.wftype
    else:
        wftype = am.UserWorkflow.WFTypeChoices.USER
    ananame = aj.get_ana_fullname(ana, wftype)
    anadate = datetime.strftime(ana.date, '%Y-%m-%d')
    resultfiles = [{'id': x.sfile_id, 'fn': x.sfile.filename, 'ana': ananame, 'date': anadate}
        for x in ana.analysisresultfile_set.exclude(sfile__pk__in=already_loaded_files)]
    return JsonResponse({'analysisname': ananame, 'date': analysis_date, 'fns': resultfiles})


@require_GET
@login_required
def load_base_analysis(request, wfversion_id, baseanid):
    """Find analysis to base current analysis on, for either copying parameters,
    complementing with new sample sets, or a rerun from PSM table.
    wfversion_id - current workflow version
    baseanid - base analysis to load onto this
    GET['dsids'] - datasets used in current analysis
    GET['added_ana_ids'] - analyses already added to exclude resultfiles from, so they dont show up in duplicates in dropdowns
    """
    try:
        new_ana_dsids = [int(x) for x in request.GET['dsids'].split(',')]
        added_ana_ids = [int(x) for x in request.GET['added_ana_ids'].split(',') if x]
    except KeyError:
        return JsonResponse({'error': 'Something wrong when asking for base analysis, contact admin'}, status=400)
    except ValueError:
        new_ana_dsids = []
        added_ana_ids = []
    try:
        new_pset_id = am.NextflowWfVersionParamset.objects.values('paramset_id').get(pk=wfversion_id)['paramset_id']
    except am.NextflowWfVersionParamset.DoesNotExist:
        return JsonResponse({'error': 'Workflow for applying base analysis not found'}, status=403)
    try:
        ana = am.Analysis.objects.select_related('nextflowsearch__workflow').get(pk=baseanid)
    except am.Analysis.DoesNotExist:
        return JsonResponse({'error': 'Base analysis not found'}, status=403)
    baseana_dsids = [dss.dataset_id for dss in ana.datasetanalysis_set.all()]
    analysis = {
            'analysis_id': ana.pk,
            'dsets_identical': set(baseana_dsids) == set(new_ana_dsids),
            'flags': [],
            'multicheck': [],
            'inputparams': {},
            'multifileparams': {},
            'fileparams': {},
            'isoquants': {},
            'added_results': {},
            }
    for ap in ana.analysisparam_set.filter(param__psetparam__pset_id=new_pset_id):
        if ap.param.ptype == am.Param.PTypes.FLAG and ap.value:
            analysis['flags'].append(ap.param.id)
        elif ap.param.ptype == am.Param.PTypes.MULTI:
            analysis['multicheck'].extend([f'{ap.param.id}___{x}' for x in ap.value])
        elif ap.param.ptype == am.Param.PTypes.SELECT:
            analysis['inputparams'][ap.param_id] = str(ap.value)
        else:
            # For NUMBER, TEXT
            analysis['inputparams'][ap.param_id] = ap.value
    # Collect files used in base analysis, determine if they are multifile or not,
    # and if they're from an added analysis
    prev_resultfiles_ids = get_prev_resultfiles(baseana_dsids, only_ids=True)
    for afp in ana.analysisfileparam_set.filter(param__psetmultifileparam__pset_id=new_pset_id
            ).select_related('analysis__nextflowsearch__workflow'):
        try:
            fnr = max(analysis['multifileparams'][afp.param_id].keys()) + 1
        except KeyError:
            fnr = 0
            analysis['multifileparams'][afp.param_id] = {}
        analysis['multifileparams'][afp.param_id][fnr] = afp.sfile_id
        get_added_analysis_contents(afp, prev_resultfiles_ids, analysis['added_results'])
    for afp in ana.analysisfileparam_set.filter(param__psetfileparam__pset_id=new_pset_id
            ).select_related('analysis__nextflowsearch__workflow'):
        analysis['fileparams'][afp.param_id] = afp.sfile_id
        get_added_analysis_contents(afp, prev_resultfiles_ids, analysis['added_results'])

    # Get datasets from base analysis for their setnames/filesamples etc
    # Only overlapping datasets are fetched here (empty dsets are popped at the end)
    dsets = {x: defaultdict(dict) for x in new_ana_dsids}
    analysis_dsfiles = defaultdict(set)
    for ads in ana.analysisdatasetsetvalue_set.filter(dataset_id__in=new_ana_dsids):
        dsets[ads.dataset_id]['fields'][ads.field] = ads.value
        dsets[ads.dataset_id]['setname'] = ads.setname.setname
        dsets[ads.dataset_id]['files'] = {}
        analysis_dsfiles[ads.dataset_id] = {x.sfile_id for x in
                am.AnalysisDSInputFile.objects.filter(analysisset=ads.setname,
                    dsanalysis__dataset_id=ads.dataset_id)}

    for dsid in new_ana_dsids:
        for fn in am.AnalysisFileValue.objects.filter(analysis=ana,
                sfile__rawfile__datasetrawfile__dataset_id=dsid):
            analysis_dsfiles[dsid].add(fn.sfile_id)
            # FIXME files should maybe be called filesamples -> less confusion
            try:
                dsets[dsid]['files'][fn.sfile_id]['fields'][fn.field] = fn.value
            except KeyError:
                dsets[dsid]['files'][fn.sfile_id] = {'id': fn.sfile_id, 'fields': {fn.field: fn.value}}
                dsets[dsid]['fields'] = {}
        if 'files' in dsets[dsid]:
            # Must check if dset is actually in the overlap before setting allfilessamesample, else it errors
            dsets[dsid]['allfilessamesample'] = all(not x['fields']['__sample'] for x in dsets[dsid]['files'].values())
    # Clean dsets to only contain dsets from base analysis
    [dsets.pop(x) for x in new_ana_dsids if not dsets[x]]

    # Select files (raw, mzml, refined) used in base analysis
    for dsid in dsets:
        dssfiles = rm.StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=dsid,
                deleted=False, purged=False, checked=True)
        dsrawfiles = dssfiles.filter(mzmlfile__isnull=True)
        dset_ftype = dsrawfiles.distinct('filetype')
        rawftype = dset_ftype.get().filetype.name
        dsf_by_type = {rawftype: dsrawfiles}
        if dsrawfiles.filter(rawfile__producer__msinstrument__isnull=False).count():
            nrrawfiles = dsrawfiles.count()
            ds_msfiles, _ = get_msdataset_files_by_type(dssfiles)
            dsf_by_type.update(ds_msfiles)

        dsets[dsid]['picked_ftype'] = rawftype
        for ft, sf_qset in dsf_by_type.items():
            if not analysis_dsfiles[dsid].difference({x.pk for x in sf_qset}):
                dsets[dsid]['picked_ftype'] = ft


    # Isoquants, sampletables, and also shadow isoquants (isoquants from the base analysis
    # own base analysis)
    try:
        sampletables = am.AnalysisSampletable.objects.get(analysis=ana).samples
    except am.AnalysisSampletable.DoesNotExist:
        sampletables = {}
    analysis['isoquants'] = get_isoquants(ana, sampletables)

    # Dont need checking complement component since base ana is included everywhere
    try:
        baseana_dbrec = am.AnalysisBaseanalysis.objects.get(analysis=ana)
    except am.AnalysisBaseanalysis.DoesNotExist:
        pass
    else:
        dsets.update(baseana_dbrec.shadow_dssetnames)
        analysis['isoquants'].update(baseana_dbrec.shadow_isoquants)

    analysis_prev_resfiles_ids = get_prev_resultfiles(new_ana_dsids, only_ids=True)
    added_files_ids = [x.sfile_id for x in am.AnalysisResultFile.objects.filter(
        analysis_id__in=added_ana_ids).exclude(sfile_id__in=analysis_prev_resfiles_ids)]
    already_loaded_files = analysis_prev_resfiles_ids + added_files_ids
    base_resfiles = [{'id': x.sfile_id, 'fn': x.sfile.filename,
        'ana': aj.get_ana_fullname(ana, ana.nextflowsearch.workflow.wftype),
        'date': datetime.strftime(ana.date, '%Y-%m-%d')}
        for x in ana.analysisresultfile_set.exclude(sfile__pk__in=already_loaded_files)]
    return JsonResponse({'base_analysis': analysis, 'datasets': dsets, 'resultfiles': base_resfiles})


@require_GET
@login_required
def get_analysis(request, anid):
    '''Renders analysis HTML page, complete with analysis, values are loaded as JSON in rendered
    page where the JS can pick it up'''
    try:
        ana = am.Analysis.objects.select_related('nextflowsearch__nfwfversionparamset__paramset',
                'nextflowsearch__job', 'externalanalysis__last_token').get(pk=anid)
    except am.Analysis.DoesNotExist:
        return HttpResponseNotFound()
    if ana.user != request.user and not request.user.is_staff:
        return HttpResponseForbidden()
    analysis = {
            'analysis_id': ana.pk,
            'analysisname': ana.name,
            'flags': [],
            'multicheck': [],
            'inputparams': {},
            'multifileparams': {},
            'fileparams': {},
            'isoquants': {},
            'added_results': {},
            'editable': ana.editable,
            'jobstate': False,
            'external_desc': '',
            'wfversion_id': False,
            'wfid': False,
            'external_results': False,
            'base_analysis': {},
            }
    dsets = {x.dataset_id: format_dset_tag(x.dataset) for x in ana.datasetanalysis_set.select_related('dataset').all()}
    prev_resultfiles_ids = get_prev_resultfiles(dsets.keys(), only_ids=True)
    if hasattr(ana, 'nextflowsearch'):
        analysis.update({
                'wfversion_id': ana.nextflowsearch.nfwfversionparamset_id,
                'wfid': ana.nextflowsearch.workflow_id,
                'jobstate': ana.nextflowsearch.job.state,
                })
        PTypes = am.Param.PTypes
        for ap in ana.analysisparam_set.all():
            if ap.param.ptype == PTypes.FLAG and ap.value:
                analysis['flags'].append(ap.param.id)
            elif ap.param.ptype == PTypes.MULTI:
                analysis['multicheck'].extend(['{}___{}'.format(ap.param.id, str(x)) for x in ap.value])
            elif ap.param.ptype == PTypes.SELECT:
                analysis['inputparams'][ap.param_id] = str(ap.value)
            else:
                # For NUMBER, TEXT
                analysis['inputparams'][ap.param_id] = ap.value

        # Parse base analysis if any
        ana_base = am.AnalysisBaseanalysis.objects.select_related('base_analysis__nextflowsearch__workflow'
                ).filter(analysis_id=ana.id)
        if ana_base.exists():
            ana_base = ana_base.get()
            base_dsids = [x.dataset_id for x in ana_base.base_analysis.datasetanalysis_set.all()]
            baname = aj.get_ana_fullname(ana_base.base_analysis,
                    ana_base.base_analysis.nextflowsearch.workflow.wftype)
            badate = datetime.strftime(ana_base.base_analysis.date, '%Y-%m-%d')
            analysis['base_analysis'] = {
                    #### these are repeated in ana/wf if same dsets
                    'resultfiles':  [{'id': x.sfile_id, 'fn': x.sfile.filename, 'ana': baname,
                        'date': badate}
                        for x in ana_base.base_analysis.analysisresultfile_set.exclude(sfile_id__in=prev_resultfiles_ids)],
                    'selected': ana_base.base_analysis_id,
                    'typedname': '{} - {} - {} - {} - {}'.format(baname,
                    ana_base.base_analysis.nextflowsearch.workflow.name,
                    ana_base.base_analysis.nextflowsearch.nfwfversionparamset.update,
                    ana_base.base_analysis.user.username, datetime.strftime(ana_base.base_analysis.date, '%Y%m%d')),
                    'isComplement': ana_base.is_complement,
                    'runFromPSM': ana_base.rerun_from_psms,
                    'dsets_identical': set(base_dsids) == set(dsets.keys()),
                    }
            ana_base_resfiles = {x['id'] for x in analysis['base_analysis']['resultfiles']}
        else:
            ana_base_resfiles = set()

        pset = ana.nextflowsearch.nfwfversionparamset.paramset
        multifiles = {x.param_id for x in pset.psetmultifileparam_set.all()}
        non_added_results_fnids = set(prev_resultfiles_ids).union(ana_base_resfiles)

        for afp in ana.analysisfileparam_set.all():
            # determine if adp to load is from an added analysis,
            # then populate either multifile or single file params
            get_added_analysis_contents(afp, non_added_results_fnids, analysis['added_results'])
            if afp.param_id in multifiles:
                try:
                    fnr = max(analysis['multifileparams'][afp.param_id].keys()) + 1
                except KeyError:
                    fnr = 0
                    analysis['multifileparams'][afp.param_id] = {}
                analysis['multifileparams'][afp.param_id][fnr] = afp.sfile_id
            else:
                # not multi, put in normal file params
                analysis['fileparams'][afp.param_id] = afp.sfile_id

        allcomponents = {x.value: x for x in am.PsetComponent.ComponentChoices}
        wf_components = {allcomponents[x.component].name: x.value for x in pset.psetcomponent_set.all()}

        if 'ISOQUANT_SAMPLETABLE' in wf_components:
            try:
                sampletables = am.AnalysisSampletable.objects.get(analysis=ana).samples
            except am.AnalysisSampletable.DoesNotExist:
                sampletables = {}
        else:
            sampletables = {}

        if 'ISOQUANT' in wf_components:
            analysis['isoquants'] = get_isoquants(ana, sampletables)

    if hasattr(ana, 'externalanalysis'):
        host = settings.KANTELEHOST or request.build_absolute_uri('/')
        analysis.update({'external_desc': ana.externalanalysis.description,
            'upload_token': ana.externalanalysis.last_token.parse_token_for_frontend(host), 
            'external_results': True})

    context = {
            'dsets': dsets,
            'analysis': analysis,
            'wfs': get_allwfs()
            }
    return render(request, 'analysis/analysis.html', context)


def get_allwfs():
    allwfs = [{
        'id': x.id, 'name': x.name, 'wftype': am.UserWorkflow.WFTypeChoices(x.wftype).name,
        'versions': [{'name': wfv.update, 'id': wfv.id,
                 'date': datetime.strftime(wfv.date, '%Y-%m-%d'), }
                 for wfv in x.nfwfversionparamsets.filter(active=True).order_by('pk')][::-1]
    }
            for x in am.UserWorkflow.objects.filter(public=True).order_by('pk')[::-1]]
    order = [x['id'] for x in allwfs]
    allwfs = {x['id']: x for x in allwfs}
    return {'wfs': allwfs, 'order': order}


def get_msdataset_files_by_type(alldsfiles, nrrawfiles=False):
    dsfiles = {}
    last_filetype = False
    dsmzfiles = alldsfiles.filter(mzmlfile__isnull=False, mzmlfile__refined=False).select_related('mzmlfile__pwiz')
    for pwiz_mzs in dsmzfiles.values('mzmlfile__pwiz_id', 'mzmlfile__pwiz__version_description').annotate(pwcount=Count('pk')):
        ftname = f'mzML (pwiz {pwiz_mzs["mzmlfile__pwiz__version_description"]})'
        dsfiles[ftname] = dsmzfiles.filter(mzmlfile__pwiz_id=pwiz_mzs['mzmlfile__pwiz_id'])
        if nrrawfiles and pwiz_mzs['pwcount'] == nrrawfiles:
            last_filetype = ftname
    # Deliver also refined mzML
    dsrefinedfiles = alldsfiles.filter(mzmlfile__isnull=False, mzmlfile__refined=True).select_related('mzmlfile__pwiz')
    for pwiz_mzs in dsrefinedfiles.values('mzmlfile__pwiz_id', 'mzmlfile__pwiz__version_description').annotate(pwcount=Count('pk')):
        ftname = f'refined mzML (pwiz {pwiz_mzs["mzmlfile__pwiz__version_description"]})' 
        dsfiles[ftname] = dsrefinedfiles.filter(mzmlfile__pwiz_id=pwiz_mzs['mzmlfile__pwiz_id'])
        if nrrawfiles and pwiz_mzs['pwcount'] == nrrawfiles:
            last_filetype = ftname
    return dsfiles, last_filetype 


@login_required
@require_GET
def get_base_analyses(request):
    '''For search box of base analyses, this provides the names and IDs of those.
    Querying this returns a list of analyses that have a name matching with 
    the input'''
    # TODO could reuse this in general analysis finding?
    # FIXME needs test
    if 'q' in request.GET:
        query = Q()
        searchterms = request.GET['q'].split()
        wftypes = zip(am.UserWorkflow.WFTypeChoices.choices, am.UserWorkflow.WFTypeChoices.names)
        for st in searchterms:
            subquery = Q(name__icontains=st)
            match_wftypes = [x[0][0] for x in wftypes if st in x[0][1] or st in x[1]]
            subquery |= Q(nextflowsearch__workflow__wftype__in=match_wftypes)
            query &= subquery
        resp = {}
        for x in am.Analysis.objects.select_related('nextflowsearch__workflow',
                'nextflowsearch__nfwfversionparamset').filter(query, deleted=False):
                        # nextflowsearch__isnull=False, 
            if hasattr(x, 'nextflowsearch'):
                txt = (f'{aj.get_ana_fullname(x, x.nextflowsearch.workflow.wftype)} - '
                        f'{x.nextflowsearch.workflow.name, x.nextflowsearch.nfwfversionparamset.update}')
            else:

                txt = f'{aj.get_ana_fullname(x, am.UserWorkflow.WFTypeChoices.USER)}'
            resp[x.id] = {'id': x.id,
            'name': f'{txt} - {x.user.username} - {datetime.strftime(x.date, "%Y%m%d")}'}
        return JsonResponse(resp)
    else:
        return JsonResponse({'error': 'Need to specify search string to base analysis search'})


@login_required
@require_GET
def get_datasets(request, wfversion_id):
    """Fetches datasets to analysis, populates dataset/analysis-specific fields,
    field relevance is guided by which workflow is used (wfversion_id)
    """
    try:
        dsids = request.GET['dsids'].split(',')
        anid = int(request.GET['anid'])
    except (KeyError, ValueError):
        return JsonResponse({'error': True, 'errmsg': ['Something wrong when asking datasets, contact admin']}, status=400)
    response = {'error': False, 'errmsg': []}
    dbdsets = dm.Dataset.objects.filter(pk__in=dsids, deleted=False).select_related(
            'quantdataset__quanttype')
    if dbdsets.count() < len(dsids):
        response['errmsg'].append('Some datasets could not be found, they may not exist or '
                'have been deleted')
    dsetinfo = {}
    allcomponents = {x.value: x for x in am.PsetComponent.ComponentChoices}
    wfcomponents = {allcomponents[x.component].name: x.value
            for x in am.PsetComponent.objects.filter(pset__nextflowwfversionparamset=wfversion_id)}
    inputcomps = wfcomponents.get('INPUTDEF', [])
    non_fields = {'setname', 'sampleID', 'instrument', 'channel', 'plate', 'fraction'}
    fields = {x: '' for x in inputcomps[1:] if not x in non_fields}
    field_order = [x for x in fields.keys()]

    # Get analysis filesamples for later use
    has_filesamples, analysis_dsfiles = defaultdict(dict), set()
    if anid:
        for afv in am.AnalysisFileValue.objects.filter(analysis_id=anid):
            has_filesamples[afv.sfile_id][afv.field] = afv.value
        analysis_dsfiles = {x for x in has_filesamples}
    
    # FIXME accumulate errors across data sets and show all, but do not report other stuff if error
    for dset in dbdsets.select_related('runname__experiment__project', 'prefractionationdataset',
            'quantdataset'):
        # For error reporting per dset:
        dsname = f'{dset.runname.experiment.project.name} / {dset.runname.experiment.name} / {dset.runname.name}'
        # Fractionation
        prefrac, hr, frregex = False, False, ''
        if 'PREFRAC' in wfcomponents:
            if hasattr(dset, 'prefractionationdataset'):
                pf = dset.prefractionationdataset
                prefrac = str(pf.prefractionation.name)
                if 'hirief' in pf.prefractionation.name.lower():
                    hr = f'HiRIEF {str(pf.hiriefdataset.hirief)}'
            frregex = wfcomponents['PREFRAC']

        # Sample(set) names, adsv fields and previously used files
        setname = ''
        if anid:
            if adsis := am.AnalysisDSInputFile.objects.filter(dsanalysis__analysis_id=anid,
                    dsanalysis__dataset=dset):
                anasetname = adsis.select_related('analysisset').first().analysisset
                setname = anasetname.setname
                analysis_dsfiles.update({x.sfile_id for x in adsis})
            # PREFRAC component:
            if adsvs := am.AnalysisDatasetSetValue.objects.filter(analysis_id=anid,
                    dataset=dset, field='__regex'):
                frregex = adsvs.get().value
            # Get other existing fields if any
            fields.update({x.field: x.value for x in
                am.AnalysisDatasetSetValue.objects.filter(analysis_id=anid,
                    dataset=dset).exclude(field__startswith='__')})

        # Get dataset files
        dssfiles = rm.StoredFile.objects.select_related('rawfile__producer', 'servershare',
                'filetype').filter(rawfile__datasetrawfile__dataset=dset,
                        deleted=False, purged=False, checked=True)
        dsrawfiles = dssfiles.filter(mzmlfile__isnull=True)

        # For reporting in interface and checking
        nrrawfiles = dsrawfiles.count()
        dsregfiles = rm.RawFile.objects.filter(datasetrawfile__dataset=dset)
        if dsregfiles.count() > nrrawfiles:
            response['errmsg'].append(f'Dataset {dsname} contains registered files that dont '
                'have a storage entry yet. Maybe the transferring hasnt been finished, '
                'or they are deleted.')

        # Get type of dataset (by files)
        dset_ftype = dsrawfiles.order_by('filetype__name').distinct('filetype__name')
        if dset_ftype.count() > 1:
            response['errmsg'].append(f'Multiple different file types in dataset {dsname}, not allowed')
            # Mainly for being able to continue, will not use this anyway in frontend
            rawtype = 'placeholder_fake_raw'
        elif dset_ftype.count() == 0:
            response['errmsg'].append(f'No stored files in dataset {dsname}')
            # Mainly for being able to continue, will not use this anyway in frontend
            rawtype = 'placeholder_fake_raw'
        else:
            rawtype = dset_ftype.get().filetype.name
        is_msdata = dsrawfiles.filter(rawfile__producer__msinstrument__isnull=False).count()

        # Get quant data from dataset
        is_isobaric, qtype = False, False
        if 'ISOQUANT' in wfcomponents and is_msdata and hasattr(dset, 'quantdataset'):
            if dset.quantchannelsample_set.count() < dset.quantdataset.quanttype.quanttypechannel_set.count():
                response['errmsg'].append(f'Not all channels in dataset {dsname} have '
                        'sample annotations, please edit the dataset first')
            else:
                is_isobaric = any(x in dset.quantdataset.quanttype.shortname for x in ['plex', 'pro'])
                qtype = {'name': dset.quantdataset.quanttype.name,
                        'short': dset.quantdataset.quanttype.shortname,
                        'is_isobaric': is_isobaric}
        elif not hasattr(dset, 'quantdataset'):
            response['errmsg'].append(f'File(s) or channels in dataset {dsname} do not have '
                    'sample annotations, please edit the dataset first')
        
        # Populate files
        usefiles = {rawtype: dsrawfiles}
        incomplete_files = []
        picked_ft = rawtype
        if is_msdata:
            ms_usefiles, new_picked_ft = get_msdataset_files_by_type(dssfiles, nrrawfiles)
            # If no mzML exist there will not be a new picked filetype
            # TODO report incomplete or no-mzML datasets (e.g. deleted)
            # Maybe: if mz.count() < nrrawfiles -> incomplete, etc - but, versions of pwiz
            # would be good if we only ever allow one set of mzml + one refined
            if new_picked_ft:
                picked_ft = new_picked_ft
            for ft, msfiles in ms_usefiles.items():
                if msfiles.count() == nrrawfiles:
                    usefiles[ft] = msfiles
                else:
                    incomplete_files.append(ft)

        resp_files = {x.id: {'ft_name': ft_name, 'id': x.id, 'name': x.filename, 'fr': '',
            'dsetsample': '', 'fields': {'__sample': '', **fields}}
            for ft_name, dsf in usefiles.items() for x in dsf}

        # Fill channels with quant data
        channels = {}
        if is_msdata and is_isobaric:
            # multiplex so add channel/samples if any exist (not for labelcheck)
            channels = {
                ch.channel.channel.name: (ch.projsample.sample, ch.channel.channel_id) for ch in
                dm.QuantChannelSample.objects.select_related(
                    'projsample', 'channel__channel').filter(dataset_id=dset.pk)}
        else:
            # Labelfree or other data type, add file/sample mapping
            channels = False
            for ft_name, dsfiles in usefiles.items():
                for fn in dsfiles.filter(rawfile__datasetrawfile__quantsamplefile__isnull=False).select_related(
                        'rawfile__datasetrawfile__quantsamplefile__projsample'):
                    resp_files[fn.id]['dsetsample'] = fn.rawfile.datasetrawfile.quantsamplefile.projsample.sample
                    if fn.id in has_filesamples:
                        resp_files[fn.id]['fields'].update(has_filesamples[fn.id])

        # Files with samples (non-MS, IP, non-isobaric, etc)
        if anid and is_msdata:
            allfilessamesample  = all((x['fields']['__sample'] == '' for x in resp_files.values()))

        elif not is_msdata:
            # sequencing data etcetera, always have sample-per-file since we dont
            # expect multiplexing or fractionation here
            # Add possible already stored analysis file samplenames
            allfilessamesample= False

        else:
            # New analysis, set names for files can be there quantsamplefile values
            # initially
            allfilessamesample = True 
            [x['fields'].update({'__sample': x['dsetsample']}) for x in resp_files.values()]

        # Finalize response
        grouped_resp_files = defaultdict(list)
        for sfid, respfn in resp_files.items():
            grouped_resp_files[respfn['ft_name']].append(respfn)
        for ft, ft_files in grouped_resp_files.items():
            ft_sfids = [x['id'] for x in ft_files]
            if not analysis_dsfiles.difference(ft_sfids):
                picked_ft = ft

        # Fill response object
        producers = dsrawfiles.distinct('rawfile__producer')
        dsetinfo[dset.pk] = {
                'id': dset.pk,
                'proj': dset.runname.experiment.project.name,
                'exp': dset.runname.experiment.name,
                'run': dset.runname.name,
                'storage': format_dset_tag(dset),
                'dtype': dset.datatype.name,
                'prefrac': prefrac,
                'hr': hr,
                'setname': setname,
                'fields': {'__regex': frregex, **fields},
                'instruments': [x.rawfile.producer.name for x in producers],
                'instrument_types': [x.rawfile.producer.shortname for x in producers],
                'qtype': qtype,
                'nrstoredfiles': [nrrawfiles, rawtype],
                'channels': channels,
                'ft_files': grouped_resp_files,
                'incomplete_files': incomplete_files,
                'picked_ftype': picked_ft,
                'allfilessamesample': allfilessamesample,
                }
    if len(response['errmsg']):
        return JsonResponse({**response, 'error': True}, status=400)
    else:
        response.update({'dsets': dsetinfo, 'field_order': field_order})
        return JsonResponse(response)


@login_required
@require_GET
def get_workflow_versioned(request):
    try:
        wf = am.NextflowWfVersionParamset.objects.get(pk=request.GET['wfvid'])
        dsids = [x for x in request.GET['dsids'].split(',') if x]
    except KeyError:
        return JsonResponse({'error': 'Something is wrong, contact admin'}, status=400)
    except am.NextflowWfVersionParamset.DoesNotExist:
        return JsonResponse({'error': 'Could not find workflow'}, status=404)
    params = wf.paramset.psetparam_set.select_related('param')
    files = wf.paramset.psetfileparam_set.select_related('param')
    multifiles = wf.paramset.psetmultifileparam_set.select_related('param')
    ftypes = [x['param__filetype_id'] for x in files.values('param__filetype_id').distinct()]
    ftypes.extend([x['param__filetype_id'] for x in multifiles.values('param__filetype_id').distinct()])
    ftypes = set(ftypes)
    selectable_files = [x for x in am.LibraryFile.objects.select_related('sfile__filetype').filter(
        sfile__filetype__in=ftypes).order_by('-sfile__regdate')]
    userfiles = [x for x in rm.UserFile.objects.select_related('sfile__filetype').filter(
        sfile__filetype__in=ftypes).order_by('-sfile__regdate')]
    selectable_files.extend(userfiles)
    allcomponents = {x.value: x for x in am.PsetComponent.ComponentChoices}
    PTypes = am.Param.PTypes
    resp = {
            'components': {allcomponents[psc.component].name: psc.value for psc in 
                wf.paramset.psetcomponent_set.all()},
            'flags': [{'nf': f.param.nfparam, 'id': f.param.pk, 'name': f.param.name,
                'help': f.param.help or False}
                for f in params.filter(param__ptype=PTypes.FLAG, param__visible=True)],
            'numparams': [{'nf': p.param.nfparam, 'id': p.param.pk, 'name': p.param.name,
                'help': p.param.help or False} for p in params.filter(param__ptype=PTypes.NUMBER)],
            'textparams': [{'nf': p.param.nfparam, 'id': p.param.pk, 'name': p.param.name,
                'help': p.param.help or False} for p in params.filter(param__ptype=PTypes.TEXT)],
            'selectparams': [{'nf': p.param.nfparam, 'id': p.param.pk, 'name': p.param.name,
                'opts': {po.pk: po.name for po in p.param.paramoption_set.all()},
                'help': p.param.help or False} for p in params.filter(param__ptype=PTypes.SELECT)],
            'multicheck': [{'nf': p.param.nfparam, 'id': p.param.pk, 'name': p.param.name,
                'opts': {po.pk: po.name for po in p.param.paramoption_set.all()},
                'help': p.param.help or False}
                for p in params.filter(param__ptype=PTypes.MULTI, param__visible=True)],
            'fileparams': [{'name': f.param.name, 'id': f.param.pk, 'nf': f.param.nfparam,
                'ftype': f.param.filetype_id, 'allow_resultfile': f.allow_resultfiles,
                'help': f.param.help or False} for f in files],
            'multifileparams': [{'name': f.param.name, 'id': f.param.pk, 'nf': f.param.nfparam,
                'ftype': f.param.filetype_id, 'allow_resultfile': f.allow_resultfiles,
                'help': f.param.help or False} for f in multifiles],
            'libfiles': {ft: [{'id': x.sfile.id, 'desc': x.description,
                'name': x.sfile.filename}
                for x in selectable_files if x.sfile.filetype_id == ft] for ft in ftypes}
    }
    # FIXME we should call get workflow versioned when new datasets are added, to get the new
    # prev_resultfiles -> or at least update it
    if dsids:
        resp['prev_resultfiles'] = get_prev_resultfiles(dsids)
    else:
        resp['prev_resultfiles'] = []
    return JsonResponse({'wf': resp})


def get_prev_resultfiles(dsids, only_ids=False):
    '''Get files from earlier analyses on same datasets
    double filtering gets first all DsS records that that have an analysis with ANY of the records,
    and then strip out:
       - with analysis that also have MORE datasets
       - analysis that have a subset of datasets
    '''
    superset_analysis = am.DatasetAnalysis.objects.filter(
            analysis__datasetanalysis__dataset_id__in=dsids).exclude(dataset__id__in=dsids).values(
                    'analysis')
    qset_analysis = am.Analysis.objects.filter(datasetanalysis__dataset__in=dsids,
            deleted=False).exclude(pk__in=superset_analysis)
    for dsid in dsids:
        qset_analysis = qset_analysis.filter(datasetanalysis__dataset_id=dsid)
    qset_arf = am.AnalysisResultFile.objects.filter(analysis__in=qset_analysis.distinct())
    if only_ids:
        prev_resultfiles = [x['sfile_id'] for x in qset_arf.values('sfile_id')]
    else:
        prev_resultfiles = [{'id': x.sfile.id, 'fn': x.sfile.filename,
            'ana': aj.get_ana_fullname(x.analysis, x.analysis.nextflowsearch.workflow.wftype),
            'date': datetime.strftime(x.analysis.date, '%Y-%m-%d')}
        for x in qset_arf.select_related('analysis__nextflowsearch__workflow')]
    return prev_resultfiles


def get_added_analysis_contents(afp, prev_or_base_resultfns, added_results):
    if (hasattr(afp.sfile, 'analysisresultfile') and not hasattr(afp.sfile, 'libraryfile')
            and not afp.sfile_id in prev_or_base_resultfns
            and afp.sfile.analysisresultfile.analysis_id not in added_results):
        arf = afp.sfile.analysisresultfile
        arf_date = datetime.strftime(arf.analysis.date, '%Y-%m-%d')
        if hasattr(arf.analysis, 'nextflowsearch'):
            wftype = arf.analysis.nextflowsearch.workflow.wftype
        else:
            wftype = am.UserWorkflow.WFTypeChoices.USER
        arf_ananame = aj.get_ana_fullname(arf.analysis, wftype)
        arf_fns = [{'id': x.sfile_id, 'fn': x.sfile.filename, 'ana': arf_ananame,
            'date': arf_date} for x in arf.analysis.analysisresultfile_set.all()]
        added_results[arf.analysis_id] = {'analysisname': arf_ananame, 'date': arf_date, 'fns': arf_fns}


@login_required
def show_analysis_log(request, ana_id):
    try:
        ana = am.Analysis.objects.get(pk=ana_id)
    except am.Analysis.DoesNotExist:
        return HttpResponseNotFound()
    return HttpResponse('\n'.join(ana.log), content_type="text/plain")

 
@login_required
@require_POST
def store_analysis(request):
    """Edits or stores a new analysis, checking for errors along the way and not storing any if those are found"""
    # Init
    jobparams = defaultdict(list)
    isoq_cli = []
    db_isoquant = {}

    # First do checks so we dont save stuff on errors:
    try:
        req = json.loads(request.body.decode('utf-8'))
        req['dsids']
        req['analysis_id']
        req['infiles']
        req['nfwfvid']
        req['components']
        req['dssetnames']
        req['picked_ftypes']
        req['analysisname']
        req['dsetfields']
        req['fnfields']
        req['params']
        req['singlefiles']
        req['multifiles']
        req['base_analysis']
        req['wfid']
        req['upload_external']
    except json.decoder.JSONDecodeError:
        return JsonResponse({'error': 'Something went wrong, contact admin concerning a bad request'}, status=400)
    except KeyError:
        return JsonResponse({'error': 'Something went wrong, contact admin concerning missing data'}, status=400)
    dsetquery = dm.Dataset.objects.filter(pk__in=req['dsids']).select_related(
            'prefractionationdataset__prefractionation',
            'storageshare__server',
            )
    if dsetquery.filter(deleted=True).exists():
        return JsonResponse({'error': 'Deleted datasets cannot be analyzed'})
    if req['analysis_id']:
        analysis = am.Analysis.objects.select_related('nextflowsearch__job').get(pk=req['analysis_id'])
        if analysis.user_id != request.user.id and not request.user.is_staff:
            return JsonResponse({'error': 'You do not have permission to edit this analysis'}, status=403)
        elif hasattr(analysis, 'nextflowsearch') and analysis.nextflowsearch.job.state == jj.Jobstates.DONE:
            return JsonResponse({'error': 'This analysis has already finished running, it cannot be edited'}, status=403)
        elif hasattr(analysis, 'nextflowsearch') and analysis.nextflowsearch.job.state not in jj.JOBSTATES_JOB_NOT_SENT:
            return JsonResponse({'error': 'This analysis has a running or queued job, it cannot be edited, please stop the job first'}, status=403)
        elif not analysis.editable:
            return JsonResponse({'error': 'This analysis cannot be edited'}, status=403)

    response_errors = []
    dsets = {str(x.pk): x for x in dsetquery}
    # Check if files have not changed while editing an analysis (e.g. long open window)
    frontend_files_not_in_ds, ds_withfiles_not_in_frontend = {int(x) for x in req['infiles']}, set()
    dsfiles = {}
    for dsid in req['dsids']:
        if req['upload_external'] and not req['wfid']:
            # Do not do any other dset processing if there is no WF
            continue
        dset = dsets[dsid]
        dsname = f'{dset.runname.experiment.project.name} / {dset.runname.experiment.name} / {dset.runname.name}'
        if not hasattr(dset, 'quantdataset'):
            response_errors.append(f'File(s) or channels in dataset {dsname} do not have '
                    'sample annotations, please edit the dataset first')
        dsregfiles = rm.RawFile.objects.filter(datasetrawfile__dataset_id=dsid)
        dssfiles = rm.StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=dsid,
                        deleted=False, purged=False, checked=True)
        dsrawfiles = dssfiles.filter(mzmlfile__isnull=True)
        nrrawfiles = dsrawfiles.count()
        if nrrawfiles < dsregfiles.count():
            response_errors.append(f'Dataset {dsname} contains registered files that dont '
                'have a storage entry yet. Maybe the transferring hasnt been finished, '
                'or they are deleted.')
        # Load the raw files
        try:
            dsfiles[dsid] = {dsrawfiles.values('filetype__name').distinct().get()['filetype__name']: dsrawfiles}
        except rm.StoredFile.MultipleObjectsReturned:
            response_errors.append(f'Files of multiple datatypes exist in dataset {dsname}')
            dsfiles[dsid] = {req['picked_ftypes'][dsid]: dsrawfiles}
        except rm.StoredFile.DoesNotExist:
            dsfiles[dsid] = {req['picked_ftypes'][dsid]: dsrawfiles}
            response_errors.append(f'No stored files exist for dataset {dsname}')
        # MS data get mzML files 
        if dsrawfiles.filter(rawfile__producer__msinstrument__isnull=False).count():
            ds_msfiles, _ = get_msdataset_files_by_type(dssfiles)
            dsfiles[dsid].update(ds_msfiles)
        # Settle for picked type
        dsfiles[dsid] = dsfiles[dsid][req['picked_ftypes'][dsid]]
        if dsfiles[dsid].count() < nrrawfiles:
            response_errors.append(f'Files of type {req["picked_ftypes"][dsid]} are fewer than '
                    f'raw files, please fix - for dataset {dsname}')

    # Already here, queue migration servershare job
    primary_share = rm.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    for dset in dsets.values():
        if dset.storageshare.server != primary_share.server:
            if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                    settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
                return JsonResponse({'error': error}, status=403)

    for dsid in req['dsids']:
        if req['upload_external'] and not req['wfid']:
            # Do not do any other dset processing if there is no WF
            continue
        for sf in dsfiles[dsid]:
            if sf.pk in frontend_files_not_in_ds:
                frontend_files_not_in_ds.remove(sf.pk)
            else:
                ds_withfiles_not_in_frontend.add(dsid)
    if len(frontend_files_not_in_ds) or len(ds_withfiles_not_in_frontend):
        response_errors.append('Files in dataset(s) have changed while you were editing. Please check the datasets marked.')
        response_special = {'files_nods': list(frontend_files_not_in_ds), 'ds_newfiles': list(ds_withfiles_not_in_frontend)}
    else:
        response_special = {}
    # Components
    allcomponents = {x.value: x for x in am.PsetComponent.ComponentChoices}
    if req['nfwfvid']:
        nfwf_ver = am.NextflowWfVersionParamset.objects.filter(pk=req['nfwfvid']).select_related('paramset').get()
        wf_components = {allcomponents[x.component].name: x.value for x in nfwf_ver.paramset.psetcomponent_set.all()}
    else:
        if not req['upload_external']:
            response_errors.append('Need to pass a workflow version to store analysis')
        wf_components = {}

    # Check if labelcheck - do we have quantchannel in single-file:
    if 'LABELCHECK_ISO' in wf_components and 'channel' in wf_components['INPUTDEF']:
        for dsid in req['dsids']:
            qfcnr = dsets[dsid].datasetrawfile_set.filter(quantfilechannel__isnull=False).count()
            if qfcnr < nrrawfiles:
                response_errors.append('Single-file-channel labelcheck needs file/channel '
                        f'annotations, dataset {dsname} does not have those')
    # Also check if qtypes not mixed for all LC
    if 'LABELCHECK_ISO' in wf_components:
        try:
            qtype = dsetquery.values('quantdataset__quanttype__shortname').distinct().get()
        except dm.Dataset.MultipleObjectsReturned:
            response_errors.append('Labelcheck pipeline cannot handle mixed isobaric types')
        else:
            jobparams['--isobaric'] = [qtype['quantdataset__quanttype__shortname']]

    def parse_isoquant(quants):
        '''Parse passed isoquant for job and DB'''
        vals = {'sweep': False, 'report_intensity': False, 'denoms': {}}
        if quants['sweep']:
            vals['sweep'] = True
            calc_psm = 'sweep'
        elif quants['report_intensity']:
            vals['report_intensity'] = True
            calc_psm = 'intensity'
        elif quants['denoms']:
            vals['denoms'] = quants['denoms']
            calc_psm = ':'.join([ch for ch, is_denom in vals['denoms'].items() if is_denom])
        else:
            return False, False
        return vals, calc_psm

    # isobaric quant, need passed: setname, any denom, or sweep or intensity
    if 'ISOQUANT' in wf_components:
        for setname, quants in req['components']['ISOQUANT'].items():
            if setname not in set(req['dssetnames'].values()):
                response_errors.append(f'Isobaric setname {setname} '
                'could not be matched to dataset setnames, something went wrong')
            vals, calc_psm = parse_isoquant(quants)
            if not vals:
                response_errors.append('Need to select one of '
                    f'sweep/intensity/denominator for set {setname}')
            db_isoquant[setname] = vals
            isoq_cli.append('{}:{}:{}'.format(setname, quants['chemistry'], calc_psm))
        if isoq_cli:
            jobparams['--isobaric'] = [' '.join(isoq_cli)]

    if not req['wfid'] and not req['upload_external']:
        response_errors.append('No workflow passed, also not uploading external data, '
                'need at least one of those.')

    if req['wfid']:
        wftype = am.UserWorkflow.objects.get(pk=req['wfid']).wftype
    elif req['upload_external']:
        wftype = am.UserWorkflow.WFTypeChoices.USER
    else:
        # Shouldnt happen but keeps linter happy about init variable
        wftype = False
    # In case of errors, do not save anything
    # No storing above this line
    if len(response_errors):
        return JsonResponse({'error': response_errors, **response_special}, status=400)

    # Checks passed, now start storing to database
    if req['analysis_id']:
        analysis.name = req['analysisname']
        analysis.save()
        dss = am.DatasetAnalysis.objects.filter(analysis=analysis)
        excess_dss = {x.dataset_id for x in dss}.difference(req['dsids'])
        dss.filter(dataset_id__in=excess_dss).delete()
        newdss = am.DatasetAnalysis.objects.bulk_create([am.DatasetAnalysis(dataset_id=dsid, analysis=analysis) 
            for dsid in set(req['dsids']).difference({x.dataset_id for x in dss})])
        dss_map = {x.dataset_id: x.pk for x in [*dss, *newdss]}
    else:
        analysis = am.Analysis.objects.create(name=req['analysisname'], user_id=request.user.id)
        dss = am.DatasetAnalysis.objects.bulk_create([am.DatasetAnalysis(dataset_id=dsid,
            analysis=analysis) for dsid in req['dsids']])
        dss_map = {x.dataset_id: x.pk for x in dss}
    ana_storpathname = (f'{analysis.pk}_{aj.get_ana_fullname(analysis, wftype)}_'
            f'{datetime.strftime(analysis.date, "%Y%m%d_%H.%M")}')
    analysis.storage_dir = f'{analysis.user.username}/{ana_storpathname}'
    analysis.save()

    if req['upload_external']:
        # Generate new upload token if it does not already exist
        exta = am.ExternalAnalysis.objects.filter(analysis=analysis).select_related('last_token')
        try:
            exta = exta.get()
        except am.ExternalAnalysis.DoesNotExist:
            upl_ft = rm.StoredFileType.objects.get(filetype=settings.ANALYSIS_FT_NAME)
            ana_prod = rm.Producer.objects.get(client_id=settings.ANALYSISCLIENT_APIKEY)
            upl_token = create_upload_token(upl_ft.pk, request.user.pk, ana_prod,
                    rm.UploadToken.UploadFileType.ANALYSIS)
            exta = am.ExternalAnalysis.objects.create(analysis=analysis,
                    description=req['external_description'], last_token=upl_token)
        else:
            # Need to access its upload token so have to get it anyway
            exta.description = req['external_description']
            exta.save()
        host = settings.KANTELEHOST or request.build_absolute_uri('/')
        api_token = exta.last_token.parse_token_for_frontend(host)
    else:
        # Delete any existing external analysis row, but also upload token!
        exta = am.ExternalAnalysis.objects.filter(analysis=analysis).select_related('last_token')
        if exta.exists():
            exta = exta.get()
            exta.delete()
            exta.last_token.delete()
        api_token = False

    in_components = {k: v for k, v in req['components'].items() if v}
    jobinputs = {'components': wf_components, 'singlefiles': {}, 'multifiles': {}, 'params': {}}
    data_args = {'filesamples': {}, 'platenames': {}, 'filefields': defaultdict(dict),
            'infiles': req['infiles'], 'sf_ids': [int(x) for x in req['infiles'].keys()]}

    # Input file definition
    if 'INPUTDEF' in wf_components:
        jobinputs['components']['INPUTDEF'] = wf_components['INPUTDEF']
    else:
        jobinputs['components']['INPUTDEF'] = False

    # Store setnames
    setname_ids = {}
    am.AnalysisSetname.objects.filter(analysis=analysis).exclude(setname__in=req['dssetnames'].values()).delete()
    for setname in set(req['dssetnames'].values()):
        anaset, created = am.AnalysisSetname.objects.get_or_create(analysis=analysis, setname=setname)
        setname_ids[setname] = anaset.pk
    # setnames for datasets, optionally fractions and strips
    new_ads = {}
    am.AnalysisDSInputFile.objects.filter(analysisset__analysis=analysis).exclude(sfile_id__in=req['infiles']).delete()
    for dsid, setname in req['dssetnames'].items():
        for fieldname, value in req['dsetfields'][dsid].items():
            ads, created = am.AnalysisDatasetSetValue.objects.update_or_create(
                    defaults={'setname_id': setname_ids[setname], 'value': value},
                    analysis=analysis, field=fieldname, dataset_id=dsid) 
            new_ads[ads.pk] = created
        for sf in dsfiles[dsid]:
            am.AnalysisDSInputFile.objects.get_or_create(sfile=sf, analysisset_id=setname_ids[setname],
                    dsanalysis_id=dss_map[dsid])
            data_args['filesamples'][sf.pk] = setname
        dset = dsets[dsid]
        if 'PREFRAC' in wf_components and hasattr(dset, 'prefractionationdataset'):
            # get platenames
            pfd = dset.prefractionationdataset
            if hasattr(pfd, 'hiriefdataset'):
                strip = '-'.join([re.sub('.0$', '', str(float(x.strip()))) for x in str(pfd.hiriefdataset.hirief).split('-')])
                data_args['platenames'][dsid] = strip
            else:
                data_args['platenames'][dsid] = pfd.prefractionation.name
    am.AnalysisDatasetSetValue.objects.filter(analysis=analysis).exclude(pk__in=new_ads).delete()

    # store samples if non-prefrac labelfree files are sets
    am.AnalysisFileValue.objects.filter(analysis=analysis).exclude(sfile_id__in=req['fnfields']).delete()
    for sfid, sample in req['fnfields'].items():
        for fieldname, value in sample.items():
            am.AnalysisFileValue.objects.update_or_create(defaults={'value': value},
                    field=fieldname, analysis=analysis, sfile_id=sfid) 
            # __sample etc is stored in filesamples, filefields is for dynamic only
            if not fieldname.startswith('__'):
                data_args['filefields'][sfid][fieldname] = value
            elif fieldname == '__sample':
                data_args['filesamples'][sfid] = value

    # Store params
    passedparams_exdelete = {**req['params']['flags'], **req['params']['inputparams'], **req['params']['multicheck']}
    am.AnalysisParam.objects.filter(analysis=analysis).exclude(param_id__in=passedparams_exdelete).delete()
    paramopts = {po.pk: po.value for po in am.ParamOption.objects.all()}
    for pid, valueids in req['params']['multicheck'].items():
        ap, created = am.AnalysisParam.objects.update_or_create(param_id=pid, analysis=analysis,
                defaults={'value': [int(x) for x in valueids]})
        jobparams[ap.param.nfparam].extend([paramopts[x] for x in ap.value])
    for pid in req['params']['flags'].keys():
        ap, created = am.AnalysisParam.objects.update_or_create(analysis=analysis, param_id=pid, value=True)
        jobparams[ap.param.nfparam] = ['']
    for pid, value in req['params']['inputparams'].items():
        ap, created = am.AnalysisParam.objects.update_or_create(
            defaults={'value': value}, analysis=analysis, param_id=pid)
        if ap.param.ptype == am.Param.PTypes.SELECT:
            ap.value = int(ap.value)
            ap.save()
            jobparams[ap.param.nfparam].append(paramopts[ap.value])
        else:
            jobparams[ap.param.nfparam].append(ap.value)

    # store parameter files
    # TODO remove single/multifiles distinction when no longer in use in home etc
    # Delete multifiles from old analysis and any old params that are not in request
    am.AnalysisFileParam.objects.filter(analysis=analysis).exclude(
            param_id__in=req['singlefiles']).delete()
    for pid, sfid in req['singlefiles'].items():
        afp, created = am.AnalysisFileParam.objects.update_or_create(defaults={'sfile_id': sfid}, analysis=analysis, param_id=pid)
        jobinputs['singlefiles'][afp.param.nfparam] = sfid
    # Re-create multifiles, they cannot be updated since all files map to analysis/param_id
    # resulting in only a single row in DB
    for pid, sfids in req['multifiles'].items():
        for sfid in sfids:
            afp = am.AnalysisFileParam.objects.create(sfile_id=sfid,
                    analysis=analysis, param_id=pid)
            try:
                jobinputs['multifiles'][afp.param.nfparam].append(sfid)
            except KeyError:
                jobinputs['multifiles'][afp.param.nfparam] = [sfid]

    # Base analysis
    if req['base_analysis']['selected']:
        # parse isoquants from base analysis (and possibly its base analysis,
        # which it will have accumulated)
        base_ana = am.Analysis.objects.select_related('analysissampletable').get(pk=req['base_analysis']['selected'])
        if hasattr(base_ana, 'analysissampletable'):
            sampletables = base_ana.analysissampletable.samples
        else:
            sampletables = {}
        shadow_dss = {}
        for x in base_ana.analysisdatasetsetvalue_set.all():
            try:
                shadow_dss[x.dataset_id]['fields'][x.field] = x.value
            except KeyError:
                shadow_dss[x.dataset_id] = {'setname': x.setname.setname, 'fields': {x.field: x.value}}
        shadow_isoquants = get_isoquants(base_ana, sampletables)
        # Add the base analysis' own base analysis shadow isquants/dss is any
        try:
            baseana_dbrec = am.AnalysisBaseanalysis.objects.get(analysis=base_ana)
        except am.AnalysisBaseanalysis.DoesNotExist:
            pass
        else:
            shadow_dss.update(baseana_dbrec.shadow_dssetnames)
            shadow_isoquants.update(baseana_dbrec.shadow_isoquants)
        # Remove current from previous (shadow) data if this is rerun 
        # and current isoquants are defined
        for setname in req['components']['ISOQUANT']:
            if setname in shadow_isoquants:
                del(shadow_isoquants[setname])
        for dsid, setname in req['dssetnames'].items():
            if int(dsid) in shadow_dss:
                del(shadow_dss[int(dsid)])
        if 'COMPLEMENT_ANALYSIS' in wf_components:
            is_complement = req['base_analysis']['isComplement']
            rerun = req['base_analysis']['runFromPSM']
            if rerun:
                # Defensive, make sure it is overridden
                is_complement = False
        else:
            is_complement, rerun = False, False
        base_def = {'base_analysis': base_ana,
                'is_complement': is_complement,
                'rerun_from_psms': rerun,
                'shadow_isoquants': shadow_isoquants,
                'shadow_dssetnames': shadow_dss,
                }
        ana_base, cr = am.AnalysisBaseanalysis.objects.update_or_create(defaults=base_def, analysis_id=analysis.id)
        # Add base analysis isoquant to the job params if it is complement analysis
        if is_complement:
            for setname, quants in shadow_isoquants.items():
                vals, calc_psm = parse_isoquant(quants)
                isoq_cli.append('{}:{}:{}'.format(setname, quants['chemistry'], calc_psm))
        # FIXME if fractionated, add the old mzmls for the plate QC count - yes that is necessary 
        # because plate names are not stored in the SQLite - maybe it should?
        # Options:
        # - store in SQL (msstitch change or dirty in the pipeline with an extra table - rather not?)
        # - pass old_mzmls to task in job, task with fn/instr/set/plate/fraction 
        # - store mzmldef in results, pass it automatically - easiest?
            
    # Already did parsing isoquants in checking part, now (re)create DB objects
    # FIXME should this be below the parsing step instead? Why it wait? Base ana?
    if 'ISOQUANT' in wf_components:
        am.AnalysisIsoquant.objects.filter(analysis=analysis).exclude(setname_id__in=setname_ids.values()).delete()
        for setname, quantvals in db_isoquant.items():
            am.AnalysisIsoquant.objects.update_or_create(defaults={'value': quantvals},
                    analysis=analysis, setname_id=setname_ids[setname])

    # If any, store sampletable
    # TODO make possible to run without sampletable (so users can skip sample names)
    if (sampletable := in_components.get('ISOQUANT_SAMPLETABLE', False)) and \
            'ISOQUANT_SAMPLETABLE' in wf_components:
        am.AnalysisSampletable.objects.update_or_create(defaults={'samples': sampletable}, analysis=analysis)
        # check if we need to concat shadow isoquants to sampletable that gets passed to job
        if req['base_analysis']['isComplement']:
            for sname, isoq in shadow_isoquants.items():
                for ch, (sample, chid) in isoq['channels'].items():
                    sampletable.append([ch, sname, sample, isoq['samplegroups'][ch]])
        # strip empty last-fields (for no-group analysis)
        sampletable = [[f for f in row if f] for row in sampletable]
        jobinputs['components']['ISOQUANT_SAMPLETABLE'] = sampletable
    else:
        jobinputs['components']['ISOQUANT_SAMPLETABLE'] = False
        am.AnalysisSampletable.objects.filter(analysis=analysis).delete()

    # All data collected, now create a job in WAITING state
    if not req['upload_external']:
        fname = 'run_nf_search_workflow'
        jobinputs['params'] = [x for nf, vals in jobparams.items() for x in [nf, ';'.join([str(v) for v in vals])]]
        #param_args = {'wfv_id': req['nfwfvid'], 'inputs': jobinputs}
        kwargs = {'analysis_id': analysis.id, 'dstsharename': settings.ANALYSISSHARENAME,
                'wfv_id': req['nfwfvid'], 'inputs': jobinputs, 'fullname': ana_storpathname,
                'storagepath': analysis.storage_dir, **data_args}
        if req['analysis_id'] and hasattr(analysis, 'nextflowsearch'):
            jobq = jm.Job.objects.filter(nextflowsearch__analysis=analysis)
            jobq.update(kwargs=kwargs, state=jj.Jobstates.WAITING)
            jobid = jobq.values('pk').get()['pk']
        else:
            jobid = create_job(fname, state=jj.Jobstates.WAITING, **kwargs)['id']
        am.NextflowSearch.objects.update_or_create(defaults={'nfwfversionparamset_id': req['nfwfvid'], 'job_id': jobid, 'workflow_id': req['wfid'], 'token': ''}, analysis=analysis)
    return JsonResponse({'error': False, 'analysis_id': analysis.id, 'token': api_token})


def get_isoquants(analysis, sampletables):
    """For analysis passed, return its analysisisoquants from DB in nice format for frontend"""
    isoquants = {}
    for aiq in am.AnalysisIsoquant.objects.select_related('setname').filter(analysis=analysis):
        set_dsets = am.DatasetAnalysis.objects.filter(analysisdsinputfile__analysisset=aiq.setname)
        qtypename = set_dsets.values('dataset__quantdataset__quanttype__shortname').distinct().get()['dataset__quantdataset__quanttype__shortname']
        qcsamples = {qcs.channel.channel_id: qcs.projsample.sample for qcs in dm.QuantChannelSample.objects.filter(dataset_id__in=set_dsets.values('dataset'))}
        channels = {qtc.channel.name: qtc.channel_id for anasds in set_dsets.distinct('dataset__quantdataset__quanttype') for qtc in anasds.dataset.quantdataset.quanttype.quanttypechannel_set.all()}
        isoquants[aiq.setname.setname] = {
                'chemistry': qtypename,
                'channels': {name: (qcsamples[chid], chid) for name, chid in channels.items()},
                'samplegroups': {samch[0]: samch[3] for samch in sampletables if samch[1] == aiq.setname.setname},
                'denoms': aiq.value['denoms'],
                'report_intensity': aiq.value['report_intensity'],
                'sweep': aiq.value['sweep'],
                }
    return isoquants


@login_required
@require_POST
def renew_token(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        analysis = am.Analysis.objects.select_related('externalanalysis__last_token').get(
                editable=True, pk=req['analysis_id'])
    except am.Analysis.DoesNotExist:
        return JsonResponse({'error': 'Analysis does not exist or is frozen'}, status=403)

    if not hasattr(analysis, 'externalanalysis'):
        return JsonResponse({'error': 'Analysis is not stored as externally run and does not '
            'have manual result uploads'}, status=403)

    # Invalidate old token and create new one
    analysis.externalanalysis.last_token.invalidate()
    new_token = create_upload_token(analysis.externalanalysis.last_token.filetype.pk,
            request.user.pk, analysis.externalanalysis.last_token.producer, 
            rm.UploadToken.UploadFileType.ANALYSIS)
    analysis.externalanalysis.last_token = new_token
    analysis.externalanalysis.save()
    host = settings.KANTELEHOST or request.build_absolute_uri('/')
    api_token = new_token.parse_token_for_frontend(host)
    return JsonResponse({'error': False, 'token': api_token})


@login_required
def undelete_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    try:
        analysis = am.Analysis.objects.get(pk=req['item_id'])
    except am.Analysis.DoesNotExist:
        return JsonResponse({'error': 'Analysis does not exist'}, status=403)
    if not analysis.deleted:
        return JsonResponse({'error': 'Analysis is not deleted, cant undelete it'}, status=403)
    if analysis.user == request.user or request.user.is_staff:
        analysis.deleted = False
        analysis.save()
        am.AnalysisDeleted.objects.filter(analysis=analysis).delete()
        return JsonResponse({})
    else:
        return JsonResponse({'error': 'User is not authorized to undelete this analysis'}, status=403)


@login_required
def delete_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    try:
        analysis = am.Analysis.objects.select_related('nextflowsearch__job').get(pk=req['item_id'])
    except am.Analysis.DoesNotExist:
        return JsonResponse({'error': 'Analysis does not exist'}, status=403)
    if analysis.deleted:
        return JsonResponse({'error': 'Analysis is already deleted'}, status=403)
    if analysis.user == request.user or request.user.is_staff:
        if not analysis.deleted:
            analysis.deleted = True
            analysis.save()
            del_record = am.AnalysisDeleted(analysis=analysis)
            del_record.save()
            if hasattr(analysis, 'nextflowsearch'):
                jobq = jm.Job.objects.filter(nextflowsearch__analysis=analysis)
                jv.cancel_or_revoke_job(jobq)
        return JsonResponse({})
    else:
        return JsonResponse({'error': 'User is not authorized to delete this analysis'}, status=403)


@login_required
def purge_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    elif not request.user.is_staff:
        return JsonResponse({'error': 'Only admin is authorized to purge analysis'}, status=403)
    req = json.loads(request.body.decode('utf-8'))
    try:
        analysis = am.Analysis.objects.get(pk=req['item_id'])
    except am.Analysis.DoesNotExist:
        return JsonResponse({'error': 'Analysis does not exist'}, status=403)
    if not analysis.deleted:
        return JsonResponse({'error': 'Analysis is not deleted, cannot purge'}, status=403)
    analysis.purged = True
    analysis.save()
    webshare = rm.ServerShare.objects.get(name=settings.WEBSHARENAME)
    # Delete files on web share here since the job tasks run on storage cannot do that
    for webfile in rm.StoredFile.objects.filter(analysisresultfile__analysis__id=analysis.pk, servershare_id=webshare.pk):
        fpath = os.path.join(settings.WEBSHARE, webfile.path, webfile.filename)
        os.unlink(fpath)
    sfiles = rm.StoredFile.objects.filter(analysisresultfile__analysis__id=analysis.pk)
    sfiles.update(deleted=True)
    sf_ids = [x['pk'] for x in sfiles.values('pk')]
    create_job('purge_analysis', analysis_id=analysis.pk, sf_ids=sf_ids)
    create_job('delete_empty_directory', sf_ids=sf_ids)
    return JsonResponse({})


@login_required
def start_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    if 'item_id' in req:
        # home app
        jobq = Q(nextflowsearch__analysis_id=req['item_id'])
    elif 'analysis_id' in req:
        # analysis start app
        jobq = Q(nextflowsearch__analysis_id=req['analysis_id'])
    else:
        return JsonResponse({'error': 'Incorrect request, contact admin'}, status=400)
    try:
        job = jm.Job.objects.get(jobq)
    except jm.Job.DoesNotExist:
        return JsonResponse({'error': 'This job does not exist (anymore), it may have been deleted'}, status=403)
    ownership = jv.get_job_ownership(job, request)
    if not ownership['owner_loggedin'] and not ownership['is_staff']:
        return JsonResponse({'error': 'Only job owners and admin can start this job'}, status=403)
    elif job.state not in jj.JOBSTATES_JOB_INACTIVE:
        return JsonResponse({'error': 'Only waiting/canceled jobs can be (re)started, '
            f'this job is {job.state}'}, status=403)
    jv.do_retry_job(job)
    job.nextflowsearch.analysis.editable = False
    job.nextflowsearch.analysis.save()
    return JsonResponse({}) 


@login_required
def stop_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    anaq = am.Analysis.objects.filter(pk=req['item_id'], user=request.user)
    if not request.user.is_superuser and not anaq.exists():
        return JsonResponse({'error': 'Analysis does not exist or you dont have permission'}, status=403)
    anaq.update(editable=True)
    jobq = jm.Job.objects.filter(nextflowsearch__analysis_id=req['item_id'])
    jv.cancel_or_revoke_job(jobq)
    return JsonResponse({})


@login_required
def freeze_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    ana = am.Analysis.objects.filter(pk=req['analysis_id'], editable=True, externalanalysis__isnull=False).select_related('externalanalysis__last_token')
    if not ana.exists():
        return JsonResponse({'error': 'Analysis cannot be locked, is already locked, or does '
            'not exist'}, status=403)
    if not request.user.is_superuser:
        ana = ana.filter(user=request.user)
        if not ana.exists():
            return JsonResponse({'error': 'You do not have permission to freeze this analysis'}, status=403)
    ana = ana.get()
    ana.editable = False
    ana.externalanalysis.last_token.invalidate()
    ana.save()
    return JsonResponse({}) 


@login_required
def unfreeze_analysis(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Must use POST'}, status=405)
    req = json.loads(request.body.decode('utf-8'))
    ana = am.Analysis.objects.filter(pk=req['analysis_id'], editable=False,
            externalanalysis__isnull=False)
    if not ana.exists():
        return JsonResponse({'error': 'Analysis cannot be unlocked, is already unlocked, or '
            'does not exist'}, status=403)
    if request.user.is_staff:
        nr = ana.update(editable=True)
    else:
        nr = ana.filter(user=request.user).update(editable=True)
    if nr == 0:
        return JsonResponse({'error': 'You do not have permission to edit this analysis'}, status=403)
    elif nr > 1:
        raise RuntimeError('Unfreeze crashed, too many analyses updated. Should not happen')
    else:
        return JsonResponse({}) 


@login_required
def serve_analysis_file(request, file_id):
    try:
        sf = get_servable_files(am.AnalysisResultFile.objects.select_related(
            'sfile__servershare')).get(pk=file_id)
    except am.AnalysisResultFile.DoesNotExist:
        return HttpResponseForbidden()
    resp = HttpResponse()
    resp['X-Accel-Redirect'] = os.path.join(settings.NGINX_ANALYSIS_REDIRECT, sf.sfile.path, sf.sfile.filename)
    return resp


@require_POST
def upload_servable_file(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'client_id' not in data or data['client_id'] !=settings.ANALYSISCLIENT_APIKEY:
        return JsonResponse({'msg': 'Forbidden'}, status=403)
    elif 'fname' not in data or data['fname'] not in settings.SERVABLE_FILENAMES:
        return JsonResponse({'msg': 'File is not servable'}, status=406)
    else:
        return JsonResponse({'msg': 'File can be uploaded and served'}, status=200)


@require_GET
@login_required
def find_datasets(request):
    searchterms = [x for x in request.GET['q'].split() if x != '']
    dbdsets = hv.dataset_query_creator(searchterms).filter(deleted=False)
    dsets = {d.id: {
        'id': d.id,
        'name': format_dset_tag(d),
        } for d in dbdsets}
    return JsonResponse(dsets)
    

def get_servable_files(resultfiles):
    return resultfiles.filter(sfile__filename__in=settings.SERVABLE_FILENAMES)


def write_analysis_log(logline, analysis_id):
    entry = '[{}] - {}'.format(datetime.strftime(timezone.now(), '%Y-%m-%d %H:%M:%S'), logline)
    analysis = am.Analysis.objects.get(pk=analysis_id)
    analysis.log.append(entry)
    analysis.save()


# FIXME need auth on this view
def nextflow_analysis_log(request):
    req = json.loads(request.body.decode('utf-8'))
    if 'runName' not in req or not req['runName']:
        return JsonResponse({'error': 'Analysis does not exist'}, status=403)
    try:
        nfs = am.NextflowSearch.objects.get(token=req['runName'])
    except am.NextflowSearch.DoesNotExist:
        return JsonResponse({'error': 'Analysis does not exist'}, status=403)
    if nfs.job.state not in jj.JOBSTATES_TASKS_RUNNING:
        return JsonResponse({'error': 'Analysis does not exist'}, status=403)
    if req['event'] in ['started', 'completed']:
        logmsg = 'Nextflow reports: workflow {}'.format(req['event'])
    elif req['event'] == 'process_completed':
        walltime = str(timedelta(seconds=req['trace']['realtime'] / 1000))
        logmsg = 'Process {} completed in {}'.format(req['trace']['name'], walltime)
    else:
        # Not logging anything
        return HttpResponse()
    write_analysis_log(logmsg, nfs.analysis_id)
    return HttpResponse()


def format_dset_tag(dset):
    return f'{dset.storage_loc.replace(os.path.sep, f" {os.path.sep} ")}'


def append_analysis_log(request):
    req = json.loads(request.body.decode('utf-8'))
    write_analysis_log(req['message'], req['analysis_id'])
    return HttpResponse()
