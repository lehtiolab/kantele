import json
import re
import os
from datetime import datetime, timedelta
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.decorators import login_required
from django.http import (JsonResponse, HttpResponse, HttpResponseNotFound,
                         HttpResponseForbidden)
from django.db import IntegrityError, transaction
from django.db.models import Q, Count, Max
from django.db.models.functions import Greatest
from django.utils import timezone

from kantele import settings
from datasets import models
from analysis import models as am
from rawstatus import models as filemodels
from jobs.jobutil import create_job, check_job_error, create_job_without_check, jobmap
from jobs import models as jm
from jobs import jobs as jj
from corefac import models as cm


INTERNAL_PI_PK = 1


def get_dset_context(proj_id):
    proj = models.Project.objects.values('name', 'pi__name', 'ptype__name', 'ptype_id').get(pk=proj_id)
    return {'dataset_id': 'false', 'is_owner': 'true', 'initdata': {'name': proj['name'],
        'proj_id': proj_id, 'pi_name': proj['pi__name'], 'ptype': proj['ptype__name'],
        'secclass': max(models.DataSecurityClass),
        'isExternal': proj['ptype_id'] != settings.LOCAL_PTYPE_ID,
        'datasettypes': [{'name': x.name, 'id': x.id} for x in
            models.Datatype.objects.filter(public=True)],
        'securityclasses': [{'id': x[0], 'name': x[1]} for x in models.DataSecurityClass.choices],
        'all_projects': {x.id: {'name': x.name, 'id': x.id} for x in models.Project.objects.filter(active=True)},
        'all_storlocs': {x.id: {'name': x.name, 'id': x.pk, 'description': x.description} for x in
            filemodels.ServerShare.objects.filter(active=True, function=filemodels.ShareFunction.RAWDATA)}
        }
        }


@login_required
def new_dataset(request, proj_id):
    """Returns dataset view with JS app"""
    context = get_dset_context(proj_id)
    return render(request, 'datasets/dataset.html', context)


@login_required
def show_dataset(request, dataset_id):
    try:
        dset = models.Dataset.objects.filter(purged=False, pk=dataset_id).values('pk', 'deleted',
                'runname__experiment__project_id', 'runname__experiment__project__ptype_id',
                'date', 'securityclass').get()
    except models.Dataset.DoesNotExist:
        return HttpResponseNotFound()
    dsown_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(dataset_id=dset['pk']).values('user_id')]
    is_owner = check_ownership(request.user, dset['runname__experiment__project__ptype_id'],
        dset['deleted'], dsown_ids)
    context = get_dset_context(dset['runname__experiment__project_id'])
    context.update({'dataset_id': dataset_id, 'is_owner': json.dumps(is_owner)})
    context['initdata'].update({'secclass': dset['securityclass'],
        'date': datetime.strftime(dset['date'], '%Y-%m-%d')})
    return render(request, 'datasets/dataset.html', context)


@login_required
@require_GET
def get_species(request):
    if 'q' in request.GET:
        query = Q(popname__icontains=request.GET['q'])
        query |= Q(linnean__icontains=request.GET['q'])
        return JsonResponse({x.id: {'id': x.id, 'linnean': x.linnean, 'name': x.popname} 
                for x in models.Species.objects.filter(query)})


@login_required
def dataset_info(request, project_id, dataset_id=False):
    response_json = {'projdata': {
        'projsamples': {x.id: {'name': x.sample, 'id': x.id} for x in
            models.ProjectSample.objects.filter(project_id=project_id)},
        'experiments': [{'id': x.id, 'name': x.name} for x in
            models.Experiment.objects.filter(project_id=project_id)],
            'prefracs': [{'id': x.id, 'name': x.name}
                          for x in models.Prefractionation.objects.all()],
            'hirief_ranges': [{'name': str(x), 'id': x.id}
                              for x in models.HiriefRange.objects.all()]},
            'dsinfo': {'locked': False}}
    if dataset_id:
        try:
            dset = models.Dataset.objects.select_related(
                'datatype',
                'runname__experiment__project',
                'runname__experiment__project__pi',
                'prefractionationdataset__prefractionation',
                'prefractionationdataset__hiriefdataset',
                'prefractionationdataset__prefractionationfractionamount',
                'prefractionationdataset__prefractionationlength'
            ).filter(purged=False, pk=dataset_id).get()
        except models.Dataset.DoesNotExist:
            return HttpResponseNotFound()
        components = models.DatatypeComponent.objects.filter(
            datatype_id=dset.datatype_id).select_related('component')
        project = dset.runname.experiment.project
        ds_servers = {x.storageshare_id: x.storage_loc_ui for x in
                models.DatasetServer.objects.filter(dataset=dset, active=True,
                    storageshare__function=filemodels.ShareFunction.RAWDATA)}
        response_json['dsinfo'] = {
                'dataset_id': dset.id,
                'experiment_id': dset.runname.experiment_id,
                'runname': dset.runname.name,
                'datatype_id': dset.datatype_id,
                'storage_locations': ds_servers,
                'storage_shares': [x for x in ds_servers.keys()],
                'locked': dset.locked,
            }
        if hasattr(dset, 'prefractionationdataset'):
            response_json['dsinfo'].update(pf_dataset_info_json(
                dset.prefractionationdataset))
        if dset.runname.experiment.project.ptype_id != settings.LOCAL_PTYPE_ID:
            mail = models.ExternalDatasetContact.objects.get(dataset_id=dset.id)
            response_json['dsinfo'].update({'externalcontactmail': mail.email})
    return JsonResponse(response_json)


@login_required
def dataset_files(request, dataset_id=False):
    newfiles = filemodels.StoredFile.objects.select_related('rawfile__producer').filter(
            rawfile__claimed=False, checked=True, rawfile__date__gt=datetime.now() - timedelta(200)).values(
                    'id', 'filename', 'rawfile__size', 'rawfile__producer__name', 'rawfile__date')
    response_json = {'instruments': [x.name for x in filemodels.Producer.objects.all()],
            'datasetFiles': [],
            'newfn_order': [x['id'] for x in newfiles.order_by('-rawfile__date')],
            'newFiles': {x['id']: {'id': x['id'], 'name': x['filename'],
                'size': round(x['rawfile__size'] / (2**20), 1),
                'date': x['rawfile__date'].timestamp() * 1000,
                'instrument': x['rawfile__producer__name'], 'checked': False}
                for x in newfiles}}
    if dataset_id:
        if not models.Dataset.objects.filter(purged=False, pk=dataset_id).count():
            return HttpResponseNotFound()
        dss_ids = models.DatasetServer.objects.filter(dataset_id=dataset_id).values('pk')
        ds_files = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=dataset_id,
                mzmlfile__isnull=True).values('pk', 'filename', 'rawfile__producer__name',
                        'rawfile__size', 'rawfile__date', 'rawfile__datasetrawfile__id').order_by(
                                'rawfile__date')
        if penfileq := jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare',
                kwargs__dss_id__in=[x['pk'] for x in dss_ids], state=jj.Jobstates.HOLD):
            pending_sfl_ids = [x for y in penfileq for x in y.kwargs['sfloc_ids']]
            pending_files = [(x['pk'], x['filename']) for x in filemodels.StoredFile.objects.filter(
                storedfileloc__in=pending_sfl_ids).values('filename', 'pk')]
        else:
            pending_files = []
        response_json.update({
            'dsfn_order': [x['pk'] for x in ds_files],
            'pendingFiles': pending_files,
            'datasetFiles':
            {x['pk']:
              {'id': x['pk'], 'name': x['filename'], 'associd': x['rawfile__datasetrawfile__id'],
                  'instrument': x['rawfile__producer__name'],
                  'size': round(x['rawfile__size'] / (2**20), 1),
                  'date': x['rawfile__date'].timestamp() * 1000, 'checked': False} for x in ds_files}})
    return JsonResponse(response_json)


@login_required
def dataset_mssampleprep(request, dataset_id=False):
    params = {}
    for p in models.SampleprepParameterOption.objects.select_related('param'):
        fill_sampleprepparam(params, p)
    pipelines = {}
    for ps in cm.PipelineStep.objects.filter(pipelineversion__locked=True,
            pipelineversion__active=True).order_by('index').values(
                    'pk', 'pipelineversion__version', 'pipelineversion__pipeline__name',
                    'pipelineversion_id', 'step__paramopt_id', 'step__paramopt__param_id'):
        pid = ps['pipelineversion_id']
        if pid not in pipelines:
            pipelines[pid] = {'id': pid, 'steps': [],
                    'name': f'{ps["pipelineversion__pipeline__name"]} - {ps["pipelineversion__version"]}',
                    }
        pipelines[pid]['steps'].append((ps['step__paramopt__param_id'], ps['step__paramopt_id'], ps['pk']))
    response_json = {'dsinfo': {'params': params, 'prepdatetrack': {}, 'prepsteptrack': {},
        'dspipe_id': False, 'pipe_id': False, 'enzymes': [{'id': x.id, 'name': x.name, 'checked': False}
            for x in models.Enzyme.objects.all()]}, 'pipelines': pipelines,
            }
    if dataset_id:
        response_json['saved'] = True
        if not models.Dataset.objects.filter(purged=False, pk=dataset_id).count():
            return HttpResponseNotFound()
        enzymes_used = {x.enzyme_id for x in 
                models.EnzymeDataset.objects.filter(dataset_id=dataset_id)}
        response_json['dsinfo']['no_enzyme'] = True
        for enzyme in response_json['dsinfo']['enzymes']:
            if enzyme['id'] in enzymes_used:
                enzyme['checked'] = True
                response_json['dsinfo']['no_enzyme'] = False
        for p in models.SampleprepParameterValue.objects.filter(dataset_id=dataset_id):
            fill_sampleprepparam(response_json['dsinfo']['params'], p.value, p.value.id)
        dspipeq = cm.DatasetPipeline.objects.filter(dataset_id=dataset_id)
        if dspipeq.exists():
            dspipe = dspipeq.get()
            response_json['dsinfo']['dspipe_id'] = dspipe.id
            response_json['dsinfo']['pipe_id'] = dspipe.pipelineversion_id
        else:
            get_admin_params_for_dset(response_json['dsinfo'], dataset_id, models.Labcategories.SAMPLEPREP)
        response_json['dsinfo']['prepdatetrack'] = {
                cm.TrackingStages(x.stage).name: datetime.strftime(x.timestamp, '%Y-%m-%d')
                for x in cm.DatasetPrepTracking.objects.filter(dspipe__dataset_id=dataset_id)}
        response_json['dsinfo']['prepsteptrack'] = {x.stage_id: x.finished for x in
            cm.DatasetPrepTrackingNodate.objects.filter(dspipe__dataset_id=dataset_id)}

    return JsonResponse(response_json)


@login_required
def dataset_msacq(request, dataset_id=False):
    response_json = {'dsinfo': {'params': get_dynamic_emptyparams(models.Labcategories.ACQUISITION)},
        'acqdata': {'operators': [{'id': x.id, 'name': '{} {}'.format(
                x.user.first_name, x.user.last_name)}
                for x in models.Operator.objects.select_related('user').all()],
        'acqmodes': [{'id': x[0], 'name': x[1]} for x in models.AcquisistionMode.choices],
            }}
    if dataset_id:
        if not models.Dataset.objects.filter(purged=False, pk=dataset_id).count():
            return HttpResponseNotFound()
        try:
            response_json['dsinfo']['operator_id'] = models.OperatorDataset.objects.values(
                    'operator_id').get(dataset_id=dataset_id)['operator_id']
            response_json['dsinfo']['acqmode'] = models.AcquisistionModeDataset.objects.values(
                    'acqmode').get(dataset_id=dataset_id)['acqmode']
            response_json['dsinfo']['rp_length'] = models.ReversePhaseDataset.objects.values(
                    'length').get(dataset_id=dataset_id)['length']
        except (models.OperatorDataset.DoesNotExist, models.AcquisistionModeDataset.DoesNotExist):
            return JsonResponse(response_json)
        except models.ReversePhaseDataset.DoesNotExist:
            response_json['dsinfo']['dynamic_rp'] = True
            response_json['dsinfo']['rp_length'] = ''
        get_admin_params_for_dset(response_json['dsinfo'], dataset_id, models.Labcategories.ACQUISITION)
    return JsonResponse(response_json)


@login_required
def dataset_samples(request, dataset_id=False):
    response_json = {'species': [], 'quants': get_empty_isoquant(), 'labeled': False,
            'lf_qtid': models.QuantType.objects.get(name='labelfree').pk,
            'allsampletypes': {x.pk: {'id': x.pk, 'name': x.name} for x in models.SampleMaterialType.objects.all()},
            'allspecies': {str(x['species']): {'id': x['species'], 'linnean': x['species__linnean'],
                'name': x['species__popname'], 'total': x['total']} 
                for x in models.SampleSpecies.objects.all().values('species', 'species__linnean', 'species__popname'
                    ).annotate(total=Count('species__linnean')).order_by('-total')[:5]},
            }
    if dataset_id:
        dset = models.Dataset.objects.filter(purged=False, pk=dataset_id).select_related('runname__experiment')
        if not dset:
            return HttpResponseNotFound()
        response_json['samples'] = {fn.id: {'model': '', 'newprojsample': '', 'species': [],
            'sampletypes': [], 'species_error': [], 'sampletypes_error': []}
            for fn in models.DatasetRawFile.objects.filter(dataset_id=dataset_id)}
        for qsf in models.QuantSampleFile.objects.select_related('projsample', 'rawfile').filter(
                rawfile__dataset_id=dataset_id):
            sample = {'model': str(qsf.projsample_id), 'samplename': qsf.projsample.sample,
                    'selectedspecies': '', 'species': [], 'selectedsampletype': '',
                    'sampletypes': [], 'projsam_dup': False, 'overwrite': False,
                    'sampletypes_error': [], 'species_error': [], 'projsam_dup_use': False}
            for samspec in qsf.projsample.samplespecies_set.all():
                sample['species'].append({'id': samspec.species_id, 'name': samspec.species.popname,
                    'linnean': samspec.species.linnean})
            for sammat in qsf.projsample.samplematerial_set.all():
                sample['sampletypes'].append({'id': sammat.sampletype.pk, 'name': sammat.sampletype.name})
            response_json['samples'][qsf.rawfile_id] = sample

        multiplexchans = models.QuantChannelSample.objects.filter(dataset_id=dataset_id)
        chan_samples = []
        for qsc in multiplexchans.select_related('channel__channel'):
            sample = {'id': qsc.channel.id, 'name': qsc.channel.channel.name,
                'model': str(qsc.projsample_id), 'samplename': qsc.projsample.sample,
                'species': [], 'sampletypes': [], 'selectedspecies': '', 'selectedsampletype': '',
                'projsam_dup': False, 'projsam_dup_use': False, 'overwrite': False,
                'sampletypes_error': [], 'species_error': []}
            for samspec in qsc.projsample.samplespecies_set.all():
                sample['species'].append({'id': samspec.species_id, 'name': samspec.species.popname,
                    'linnean': samspec.species.linnean})
            for sammat in qsc.projsample.samplematerial_set.all():
                sample['sampletypes'].append({'id': sammat.sampletype.pk, 'name': sammat.sampletype.name})
            chan_samples.append(sample)
        if len(chan_samples):
            qtid = models.QuantDataset.objects.get(dataset_id=dataset_id).quanttype_id
            # Trick to sort N before C:
            chan_samples.sort(key=lambda x: x['name'].replace('N', 'A'))
            response_json['quants'][qtid]['chans'] = chan_samples
            response_json['labeled'] = qtid
    return JsonResponse(response_json)


@login_required
def labelcheck_samples(request, dataset_id=False):
    response_json = {'quants': get_empty_isoquant()}
    if dataset_id:
        if request.GET['lctype'] == 'single':
            response_json['samples'] = {fn.id: {'model': '', 'channel': '', 'channelname': ''}
                        for fn in models.DatasetRawFile.objects.filter(dataset_id=dataset_id)}
        try:
            qtype = models.QuantDataset.objects.filter(
                dataset_id=dataset_id).get()
        except models.QuantDataset.DoesNotExist:
            # No data stored yet, except for files, so not loading
            pass
        else:
            response_json['quanttype'] = qtype.quanttype_id
            if request.GET['lctype'] == 'single':
                for qfcs in models.QuantFileChannel.objects.select_related(
                        'channel__channel').filter(dsrawfile__dataset_id=dataset_id):
                    response_json['samples'][qfcs.dsrawfile_id] = {
                            'channel': qfcs.channel_id, 
                            'channelname': qfcs.channel.channel.name}
    for qt, q in response_json['quants'].items():
        q['chanorder'] = [ch['id'] for ch in q['chans']]
        q['chans'] = {ch['id']: ch for ch in q['chans']}
    return JsonResponse(response_json)



@login_required
def get_datatype_components(request, datatype_id):
    dtcomps = models.DatatypeComponent.objects.filter(datatype_id=datatype_id)
    return JsonResponse({'dt_id': datatype_id,
        'components': [models.DatasetUIComponent(x.component).name for x in dtcomps]})


def get_admin_params_for_dset(response, dset_id, category):
    """Fetches all stored param values for a dataset and returns nice dict"""
    stored_data, oldparams, newparams = {}, {}, {}
    params = response['params']
    params_saved = False
    for p in models.CheckboxParameterValue.objects.filter(
            dataset_id=dset_id, value__param__category=category):
        params_saved = True
        if p.value.param.active:
            fill_admin_checkboxparam(params, p.value, p.value.id)
        else:
            fill_admin_checkboxparam(oldparams, p.value, p.value.id)
    for p in models.FieldParameterValue.objects.filter(
            dataset_id=dset_id, param__category=category):
        params_saved = True
        if p.param.active:
            fill_admin_fieldparam(params, p.param, p.value)
        else:
            fill_admin_fieldparam(oldparams, p.param, p.value)
    if not params_saved:
        # not saved for this dset id so dont return the params
        return
    # Parse new params (not filled in in dset), old params (not active anymore)
    # use list comprehension so no error: dict changes during iteration
    # TODO why is this, we dont have client support
    for p_id in [x for x in params.keys()]:
        if params[p_id]['model'] == '':
            newparams[p_id] = params.get(p_id)
    if params_saved:
        response['oldparams'] = [x for x in oldparams.values()]
        response['newparams'] = [x for x in newparams.values()]


def get_source_dss_for_transfers(alldss):
    '''Given a queryset of datasetserver, this returns the first of:
    - a share which is accessible to an rsync-key-having controller
    - False
    Maybe make function of storeshare?'''
    rsync_dss = alldss.filter(storageshare__fileservershare__server__can_rsync_remote=True)
    if rsync_dss.exists():
        srcdss = rsync_dss.get()
    elif alldss.exists():
        # dataset is on other share, need to pull from via rsync
        # FIXME first queue rsync to inbox or open?
        srcdss = False
    else:
        # dataset is no longer available except from backup
        # FIXME fetch from backup to primshare if needed (when job queuing)!
        # Or should user do that themselves?
        srcdss = False
    return srcdss


def update_dataset(data, user_id):
    # FIXME it is v annoying to do all the hierarchical creations in multiple steps
    # in a single method, as this generates a lot of error checking, and things like
    # saves that are done when an error pops up later (storage loc is checked last, but a
    # proj is saved in the start. Easier to run
    # one method per update (exp, proj, dset) - but that will create unneccessary move
    # operations. But since it's a rename, it will be short, and usually not all of them
    # change at the same time.
    dset = models.Dataset.objects.filter(pk=data['dataset_id']).select_related(
        'runname__experiment__project', 'datatype').get()
    project = models.Project.objects.get(pk=data['project_id'])
    newrun, newexp, newproj = False, False, False
    if project.id != dset.runname.experiment.project_id:
        newproj = True
    if 'newexperimentname' in data and data['newexperimentname']:
        experiment = models.Experiment(name=data['newexperimentname'], project=project)
        dset.runname.experiment = experiment
        newexp = True
    elif 'experiment_id' in data:
        try:
            experiment = models.Experiment.objects.get(pk=data['experiment_id'])
        except (models.Experiment.DoesNotExist, ValueError):
            return JsonResponse({'error': f'Experiment ID not found!'}, status=404)
        if data['experiment_id'] != dset.runname.experiment_id:
            # another experiment was selected
            newexp = True
            dset.runname.experiment = experiment
    else:
        return JsonResponse({'error': f'You  must fill in an experiment'}, status=400)

    dset.securityclass = data['secclass']
    if data['runname'] != dset.runname.name or newexp:
        # Save if new experiment AND/OR new name Runname coupled 1-1 to dataset
        newrun = True
        dset.runname.name = data['runname']
    
    new_dtcs = []
    if dset.datatype_id != data['datatype_id']:
        dset.datatype_id = data['datatype_id']
        new_dtcomponents = models.DatatypeComponent.objects.filter(datatype_id=data['datatype_id'])
        existing_dtcstates = models.DatasetComponentState.objects.filter(dataset_id=dset.id)
        existing_dtcstates.delete()
        for dtc in new_dtcomponents:
            try:
                old_dtcs = existing_dtcstates.get(dtcomp__component=dtc.component)
            except models.DatasetComponentState.DoesNotExist:
                state = models.DCStates.NEW
            else:
                state = old_dtcs.state
            new_dtcs.append(models.DatasetComponentState(dataset=dset, dtcomp=dtc, state=state))

    dtype = models.Datatype.objects.get(pk=data['datatype_id'])
    prefrac = models.Prefractionation.objects.get(pk=data['prefrac_id']) if data['prefrac_id'] else False
    hrrange = models.HiriefRange.objects.get(pk=data['hiriefrange']) if data.get('hiriefrange', False) else False
    if upd_stor_err := update_storage_shares(data['storage_shares'], project, experiment, dset,
            dtype, prefrac, hrrange, user_id):
        return upd_stor_err

    # update prefrac
    try:
        pfds = models.PrefractionationDataset.objects.filter(
            dataset_id=dset.id).select_related(
            'hiriefdataset', 'prefractionationfractionamount',
            'prefractionationlength').get()
    except models.PrefractionationDataset.DoesNotExist:
        pfds = False
    if not pfds and data['prefrac_id']:
        save_dataset_prefrac(dset.id, data)
    elif pfds and not data['prefrac_id']:
        models.PrefractionationDataset.objects.get(
            dataset_id=data['dataset_id']).delete()
    elif pfds and data['prefrac_id']:
        update_dataset_prefrac(pfds, data)

    # Project etc update
    if newproj:
        move_dset_samples_other_project(dset, project)
    if newexp:
        experiment.save()
    if newrun:
        dset.runname.save()
    dset.securityclass = data['secclass']
    dset.save()
    for dtcs in new_dtcs:
        dtcs.save()
    if dset.runname.experiment.project.ptype_id != settings.LOCAL_PTYPE_ID:
        try:
            if dset.externaldatasetcontact.email != data['externalcontact']:
                dset.externaldatasetcontact.email = data['externalcontact']
                dset.externaldatasetcontact.save()
                models.ProjectLog.objects.create(project=dset.runname.experiment.project,
                        level=models.ProjLogLevels.INFO, message=f'User {user_id} changed '
                        f'dataset {dset.pk} contact mail to {data["externalcontact"]} from '
                        f'{dset.externaldatasetcontact.email}')
        except models.ExternalDatasetContact.DoesNotExist:
            dset_mail = models.ExternalDatasetContact(dataset=dset,
                    email=data['externalcontact'])
            models.ProjectLog.objects.create(project=dset.runname.experiment.project,
                    level=models.ProjLogLevels.INFO, message=f'User {user_id} changed '
                    f'dataset {dset.pk} contact mail to {data["externalcontact"]} from (no entry)')
            dset_mail.save()
    return JsonResponse({'dataset_id': dset.id})


def update_storage_shares(share_ids, project, experiment, dset, dtype, prefrac, hrrange, user_id):
    '''Stores datasets in specified storage shares, rsyncing for new shares,
    removing from old shares.

    Shared by save_new_dataset, update_dataset, and save_storage_shares'''

    shares_q = filemodels.ServerShare.objects.filter(active=True, has_rawdata=True)
    alldss_q = models.DatasetServer.objects.filter(dataset=dset, active=True)
    existing_sfl = filemodels.StoredFileLoc.objects.filter(active=True,
            sfile__rawfile__datasetrawfile__dataset=dset)
    dsshare_upds, dsshare_mvjobs, rsync_jobs, retrieve_jobs = [], [], [], []
    qpid = get_quantprot_id()
    sfls_dss = {}

    if not len(share_ids):
        return JsonResponse({'error': 'A dataset must at least have one active storage share, '
            'otherwise you may retire the dataset instead'}, status=403)
    for sshare_id in share_ids:
        try:
            share = shares_q.get(pk=sshare_id)
        except filemodels.ServerShare.DoesNotExist:
            return JsonResponse({'error': 'Servershare asked to store on does not exist'}, status=403)
        if share.max_security < int(dset.securityclass):
            return JsonResponse({'error': 'Servershare asked to store on has too low security class'
                'for this dataset'}, status=403)
        # Get new storage location if moving stuff, and path/file names for error check on storedfile
        new_storage_loc = share.set_dset_storage_location(qpid, project, experiment,
                dset, dtype, prefrac, hrrange)
        err_fpath, err_fname = os.path.split(new_storage_loc)
        # dsshare_q already has shares that are active etc, through share
        dsshare_q = alldss_q.filter(storageshare=share, active=True)
        is_new_storloc = not dsshare_q.filter(storage_loc_ui=new_storage_loc).exists()
        ## FIXME check, rsync job already checks this errors? Maybe same for the rename job
        # or maybe do so that is done! Centralize -> less code, and it is checked on job creation
        # anyway so we can do it here.
        if not is_new_storloc and dsshare_q.exists():
            # DSS already exists and does not change
            pass
        elif models.DatasetServer.objects.exclude(dataset=dset).filter(storageshare=share,
                storage_loc_ui=new_storage_loc).exists():
            return JsonResponse({'error': 'There is already another dataset with that exact storage'
                f' location: {new_storage_loc}'}, status=403)
        elif is_new_storloc and filemodels.StoredFileLoc.objects.filter(servershare=share,
                path=err_fpath, sfile__filename=err_fname).exists():
            return JsonResponse({'error': 'There is already a file with that exact path '
                f'{new_storage_loc}'}, status=403)
        else:
            # If a path was updated, and files are in it that are not marked as (future) deleted,
            # we do a rename job. If no dss_sflocs exist for that share, we need to rsync them there
            dsshare_upds.append((share, {'storage_loc_ui': new_storage_loc, 'active': True}))
            if is_new_storloc and dsshare_q.exists():
                # Can do .get() since dsshare is Unique per dset/share
                dss = dsshare_q.get()
                renamesfl = [x['pk'] for x in 
                        existing_sfl.filter(servershare=dss.storageshare).values('pk')]
                sfls_dss[dss.pk] = renamesfl
                if joberror := check_job_error('rename_dset_storage_loc', dss_id=dss.pk,
                        sfloc_ids=renamesfl, newpath=new_storage_loc):
                    return JsonResponse({'error': joberror}, status=403)
                else:
                    # Add dss.storage_loc here since it will change before the job is launched!
                    dsshare_mvjobs.append((dss, dss.storage_loc_ui, new_storage_loc))
            elif is_new_storloc and existing_sfl.exists():
                # No active dataset on share, so rsync it over.
                # If we need to fetch dset from other source than primary
                # (because limited lifespan there), try to find open dset
                # otherwise revive from backup
                if existing_sfl.filter(servershare=share).exists():
                    return JsonResponse({'error': 'Error while trying to upload dset files to new '
                        'file server: some files already exist on that server according to DB'},
                        status=403)
                rsync_jobs.append(share)
            elif models.DatasetRawFile.objects.filter(dataset=dset).exists():
                # Dont fail when doing a new dataset save
                return JsonResponse({'error': 'There are no existing stored files to rsync, or '
                    'a dataset location to rename'}, status=403)
                # FIXME append params for rsync job here if no sflocs.exists()?

    if rsync_jobs:
        if rsync_server := filemodels.FileServer.objects.filter(can_rsync_remote=True):
            rsync_server = rsync_server.values('pk').first()
        else:
            return JsonResponse({'error': 'There is no configured fileserver that can handle '
                'rsync jobs, please configure your system'} , status=403)

    # From here we start saving to DB:
    # Before adding dss, create non-changing pre_alldss_pks for source to rsync from:
    pre_alldss_pks = [x['pk'] for x in alldss_q.values('pk')]

    # Update/create remaining shares
    dss_shares = {}

    for share, upd_defaults in dsshare_upds:
        dss, _cr = models.DatasetServer.objects.update_or_create(dataset=dset, storageshare=share,
                defaults=upd_defaults, create_defaults={**upd_defaults, 'startdate': timezone.now(),
                    'storage_loc': upd_defaults['storage_loc_ui']})
        dss_shares[share] = dss

    # Rename any existing dataset sites
    for dss, oldpath, newpath in dsshare_mvjobs:
        create_job_without_check('rename_dset_storage_loc', dss_id=dss.pk, sfloc_ids=sfls_dss[dss.pk],
                newpath=newpath)
        models.ProjectLog.objects.create(project=dset.runname.experiment.project,
                level=models.ProjLogLevels.INFO, message=f'User {user_id} changed dataset '
                f'{dset.pk} path to {newpath} from {oldpath}')

    # Create rsync jobs to new storage shares if needed
    if rsync_jobs:
        # get original alldss_q, since it has been changed above
        pre_alldss = alldss_q.filter(pk__in=pre_alldss_pks)
        rsync_dss = pre_alldss.filter(storageshare__fileservershare__server__can_rsync_remote=True)
        models.ProjectLog.objects.create(project=project, level=models.ProjLogLevels.INFO,
                message=f'User {user_id} transferred dataset {dset.pk} to {share.name}({share.pk})')
        if rsync_dss.exists():
            # we have a dss local on an rsync server
            srcdss = rsync_dss.first()
            src_sfloc_ids = [x['pk'] for x in existing_sfl.filter(servershare=srcdss.storageshare,
                sfile__rawfile__datasetrawfile__dataset=dset).values('pk')]
            for dstshare in rsync_jobs:
                dstdss = dss_shares[dstshare]
                # Rsync but since this is a new(ly activated) dss, also set its storage_loc
                dstdss.storage_loc = dstdss.storage_loc_ui
                dstdss.save()
                create_job('rsync_dset_files_to_servershare', dss_id=dstdss.pk,
                        sfloc_ids=src_sfloc_ids, dstshare_id=dstshare.pk)

        elif pre_alldss.exists():
            # there is a live dss somewhere, get it onto an rsync source first
            # This is VERY similar to what we do in home/views for mzml, except the local storage
            src_sfloc_ids = [x['pk'] for x in existing_sfl.filter(
                servershare=pre_alldss.first().storageshare,
                    sfile__rawfile__datasetrawfile__dataset=dset).values('pk')]
            share_ids_dss_map = {x.storageshare_id: x.pk for x in alldss_q}
            dstshare_classes = filemodels.ServerShare.classify_shares_by_rsync_reach(
                    [x.pk for x in rsync_jobs], rsync_server['pk'])
            for dstshare_id in dstshare_classes['rsync_sourcable']:
                rsjob = create_job('rsync_dset_files_to_servershare', dstshare_id=dstshare_id,
                        dss_id=share_ids_dss_map[dstshare_id], sfloc_ids=src_sfloc_ids)
                src_sflids_for_remote = rsjob['kwargs']['dstsfloc_ids']
            if dstshare_classes['remote'] and not dstshare_classes['rsync_sourcable']:
                # need to rsync to non-dset INBOX (as it is guaranteed to be on an rsyncing capable
                # controller (and sensitive classed)
                dstshare = filemodels.ServerShare.objects.filter(function=filemodels.ShareFunction.INBOX
                        ).values('pk').first()
                src_sflids_for_remote = []
                for sflpk in src_sfloc_ids:
                    inboxjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=sflpk,
                            dstshare_id=dstshare['pk'], dstpath=filemodels.ServerShare.get_inbox_path(
                                dset_id=dset.pk))
                    src_sflids_for_remote.append(inboxjob['kwargs']['dstsfloc_id'])
            for dstshare_id in dstshare_classes['remote']:
                create_job('rsync_dset_files_to_servershare', dss_id=share_ids_dss_map[dstshare_id],
                        sfloc_ids=src_sflids_for_remote, dstshare_id=dstshare_id)
        else:
            # This should not happen! dset.deleted should have been set, show error to user
            return JsonResponse({'error': 'Dataset is not set to deleted but has no storage. - '
                        'This should not happen, please inform admin (copy paste this message)!'})

    # delete files from disabled shares:
    for rmdss in alldss_q.exclude(storageshare_id__in=share_ids):
        dss_rmsfl = existing_sfl.filter(servershare=rmdss.storageshare)
        dssrmsfl_ids = [x['pk'] for x in dss_rmsfl.values('pk')]
        create_job('remove_dset_files_servershare', dss_id=rmdss.pk, sfloc_ids=dssrmsfl_ids)
        dss_rmsfl.update(active=False)
    alldss_q.exclude(storageshare_id__in=share_ids).update(active=False)

    return False


@require_POST
@login_required
def save_storage_shares(request):
    '''Called when updating dataset from home view with new storage shares'''
    data = json.loads(request.body.decode('utf-8'))
    dset = models.Dataset.objects.filter(pk=data['dataset_id']).select_related(
        'runname__experiment__project', 'datatype').get()
    prefrac = dset.prefractionationdataset.prefractionation if hasattr(dset, 'prefractionationdataset') else False
    hrrange = prefrac.hiriefdataset if prefrac and hasattr(prefrac, 'hiriefdataset') else False
    #prefrac = models.Prefractionation.objects.get(pk=data['prefrac_id']) if data['prefrac_id'] else False
    #hrrange = models.HiriefRange.objects.get(pk=data['hiriefrange']) if data.get('hiriefrange', False) else False

    if upd_stor_err := update_storage_shares(data['storage_shares'],
            dset.runname.experiment.project, dset.runname.experiment, dset, dset.datatype,
            prefrac, hrrange, request.user.id):
        return upd_stor_err
    return JsonResponse({'msg': 'Updated storage locations for dataset'})


def move_dset_samples_other_project(dset, project):
    # all ds proj samples need new project, either move or duplicate...
    dsraws = dset.datasetrawfile_set.all()
    dspsams = models.ProjectSample.objects.filter(quantchannelsample__dataset=dset).union(
            models.ProjectSample.objects.filter(quantsamplefile__rawfile__in=dsraws),
            models.ProjectSample.objects.filter(quantfilechannel__dsrawfile__in=dsraws)
            ).values('pk')
    # Since unions cant be filtered/excluded on, re-query
    dspsams = models.ProjectSample.objects.filter(pk__in=dspsams)
    # Duplicate multi DS projsamples from QCS, they are going to a new project:
    multipsams = set()
    multidsqcs = models.QuantChannelSample.objects.filter(projsample__in=dspsams).exclude(dataset=dset)
    for qcs in multidsqcs.distinct('projsample'):
        multipsams.add(qcs.projsample_id)
        newpsam = models.ProjectSample.objects.create(sample=qcs.projsample.sample, project=project)
        models.QuantChannelSample.objects.filter(dataset=dset, projsample=qcs.projsample).update(projsample=newpsam)
    multidsqsf = models.QuantSampleFile.objects.filter(projsample__in=dspsams).exclude(rawfile__in=dsraws)
    for qsf in multidsqsf.distinct('projsample'):
        multipsams.add(qsf.projsample_id)
        newpsam = models.ProjectSample.objects.create(sample=qsf.projsample.sample, project=project)
        models.QuantSampleFile.objects.filter(rawfile__in=dsraws, projsample=qsf.projsample).update(projsample=newpsam)
    multidsqfcs = models.QuantFileChannel.objects.filter(projsample__in=dspsams).exclude(dsrawfile__in=dsraws)
    for qfcs in multidsqfcs.distinct('projsample'):
        multipsams.add(qfcs.projsample_id)
        newpsam = models.ProjectSample.objects.create(sample=qfcs.projsample.sample, project=project)
        models.QuantFileChannel.objects.filter(dsrawfile__in=dsraws, projsample=qfcs.projsample).update(projsample=newpsam)
    # having found multi-dset-psams, now move project_id on non-shared projectsamples
    dspsams.exclude(pk__in=multipsams).update(project=project)


@require_POST
@login_required
def save_new_project(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        data['pi_id'], data['ptype_id'], data['name'], data['extref']
    except KeyError:
        return JsonResponse({'error': 'Please fill in the fields'}, status=400)
    if 'newpiname' in data:
        pi_id = models.PrincipalInvestigator.objects.create(name=data['newpiname']).pk
    elif data['ptype_id'] == settings.LOCAL_PTYPE_ID:
        # FIXME set internal PI in conf/settings maybe, or do we even need one since this is
        # a ONE-PI-SYSTEM!
        pi_id = INTERNAL_PI_PK
    else:
        try:
            pi_id = int(data['pi_id'])
        except ValueError:
            return JsonResponse({'error': 'Please select a PI'}, status=403)
    try:
        project = models.Project.objects.create(name=data['name'], pi_id=pi_id,
                ptype_id=data['ptype_id'], externalref=data['extref'])
    except IntegrityError:
        return JsonResponse({'error': 'Project name already exists'}, status=403)
    tableproject, _order = populate_proj(models.Project.objects.filter(pk=project.pk), request.user)
    models.ProjectLog.objects.create(project=project, level=models.ProjLogLevels.OPEN,
            message=f'Project opened by user {request.user.id}')
    return JsonResponse({'project': tableproject[project.pk]})


def populate_proj(dbprojs, user, showjobs=True, include_db_entry=False):
    projs, order = {}, []
    dbprojs = dbprojs.annotate(dsmax=Max('experiment__runname__dataset__date'),
            anamax=Max('experiment__runname__dataset__datasetanalysis__analysis__date')).annotate(
            greatdate=Greatest('dsmax', 'anamax'))
    for proj in dbprojs.order_by('-greatdate'): # latest first
        order.append(proj.id)
        projs[proj.id] = {
            'id': proj.id,
            'name': proj.name,
            'inactive': proj.active == False,
            'start': datetime.strftime(proj.registered, '%Y-%m-%d %H:%M'),
            'ptype': proj.ptype.name,
            'dset_ids': [x.dataset.pk for y in proj.experiment_set.all() for x in y.runname_set.all() if hasattr(x, 'dataset')],
            'details': False,
            'selected': False,
            'lastactive': datetime.strftime(proj.greatdate, '%Y-%m-%d %H:%M') if proj.greatdate else '-',
            'actions': ['new dataset', 'close'],
        }
    return projs, order


def get_prefrac_ids():
    return (models.Prefractionation.objects.get(name__icontains='hirief').id,
            models.Prefractionation.objects.get(name__icontains='high_pH').id)
            

def get_quantprot_id():
    return models.Datatype.objects.get(name__icontains='quantitative').id


@login_required
def change_owners(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = models.Dataset.objects.filter(pk=data['dataset_id']).values('pk', 'deleted',
                'runname__experiment__project_id', 'runname__experiment__project__ptype_id').get()
    except models.Dataset.DoesNotExist:
        print('change_owners could not find dataset with that ID {}'.format(data['dataset_id']))
        return JsonResponse({'error': 'Something went wrong trying to change ownership for that dataset'}, status=403)
    dsownq = models.DatasetOwner.objects.filter(dataset_id=dset['pk'])
    if not check_ownership(request.user, dset['runname__experiment__project__ptype_id'],
            dset['deleted'], [x['user_id'] for x in dsownq.values('user_id')]):
        return HttpResponseForbidden()
    is_already_ownerq = dsownq.filter(user_id=data['owner'])
    is_already_owner = is_already_ownerq.exists()
    if data['op'] == 'add' and not is_already_owner:
        newowner = models.DatasetOwner.objects.create(dataset_id=dset['pk'], user_id=data['owner'])
        models.ProjectLog.objects.create(project_id=dset['runname__experiment__project_id'],
                level=models.ProjLogLevels.SECURITY,
                message=f'User {request.user.id} made {data["owner"]} an owner of dset {dset["pk"]}')
        return JsonResponse({'result': 'ok'})
    elif data['op'] == 'del' and is_already_owner and dsownq.count() > 1:
        is_already_ownerq.delete()
        models.ProjectLog.objects.create(project_id=dset['runname__experiment__project_id'],
                level=models.ProjLogLevels.SECURITY,
                message=f'User {request.user.id} deleted owner {data["owner"]} from dataset {dset["pk"]}')
        return JsonResponse({'result': 'ok'})
    else:
        return JsonResponse({'result': 'error', 'message': 'Something went wrong trying to change ownership'}, status=500)
    

def get_dataset_owners_ids(dset):
    return [x.user.id for x in dset.datasetowner_set.all()]


def check_ownership(user, ptype_id, deleted, owners):
    """
    Ownership is OK if:
    - User is staff OR
    - dataset is not deleted AND (user is dset owner OR dset is CF and user has CF admin rights)
    """
    if deleted and not user.is_staff:
        return False
    elif user.id in owners or user.is_staff:
        return True
    elif ptype_id == settings.LOCAL_PTYPE_ID:
        return False
    else:
        try:
            models.UserPtype.objects.get(ptype_id=ptype_id, user_id=user.id)
        except models.UserPtype.DoesNotExist:
            return False
    return True


def check_save_permission(dset_id, logged_in_user, action=False):
    '''Only datasets which are not deleted can be saved, and a check_ownership is done,
    return (False, 200) if ok,
    return e.g. ('errmsg', 403) if not ok
    '''
    dsq = models.Dataset.objects.filter(purged=False, deleted=False, pk=dset_id,
            runname__experiment__project__active=True)
    if not dsq.exists():
        return ('Cannot find dataset to edit, maybe it is deleted, or is its project no longer '
                'active?', 404)
    elif action != 'unlock' and dsq.filter(locked=True).exists():
        return ('You cannot edit a locked dataset', 403)
    elif action == 'unlock' and dsq.filter(locked=False).exists():
        return ('Dataset already unlocked', 403)

    ds = dsq.filter(locked=action == 'unlock').values('deleted',
            'runname__experiment__project__ptype_id').get()
    owner_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(dataset_id=dset_id).values('user_id')]
    if check_ownership(logged_in_user, ds['runname__experiment__project__ptype_id'],
            deleted=ds['deleted'], owners=owner_ids):
        return (False, 200)
    else:
        return ('You are not authorized to edit this dataset', 403)


def get_or_create_px_dset(exp, px_acc, user_id):
    try:
        return models.Dataset.objects.get(
            runname__name=px_acc,
            runname__experiment__project_id=settings.PX_PROJECT_ID)
    except models.Dataset.DoesNotExist:
        project = models.Project.objects.get(pk=settings.PX_PROJECT_ID)
        experiment = models.Experiment(name=exp, project=project)
        experiment.save()
        run = models.RunName(name=px_acc, experiment=experiment)
        run.save()
        data = {'datatype_id': get_quantprot_id(), 'prefrac_id': False,
                'ptype_id': settings.LOCAL_PTYPE_ID}
        return save_new_dataset(data, project, experiment, run, user_id)


def save_new_dataset(data, project, experiment, runname, user_id):
    if not len(data.get('storage_shares', [])):
        raise IntegrityError('A dataset must at least have one active storage share, otherwise '
                'you may retire the dataset instead')
    dtype = models.Datatype.objects.get(pk=data['datatype_id'])
    prefrac = models.Prefractionation.objects.get(pk=data['prefrac_id']) if data['prefrac_id'] else False
    hrrange = models.HiriefRange.objects.get(pk=data['hiriefrange']) if data.get('hiriefrange', False) else False
    dset = models.Dataset.objects.create(date=timezone.now(), runname_id=runname.id,
            datatype=dtype, securityclass=data['secclass'])
    if upd_stor_err := update_storage_shares(data['storage_shares'], project, experiment, dset,
            dtype, prefrac, hrrange, user_id):
        raise IntegrityError(json.loads(upd_stor_err.content)['error'])
    dsowner = models.DatasetOwner.objects.create(dataset=dset, user_id=user_id)
    if data['prefrac_id']:
        save_dataset_prefrac(dset.id, data)
    if project.ptype_id != settings.LOCAL_PTYPE_ID:
        dset_mail = models.ExternalDatasetContact.objects.create(dataset=dset,
                email=data['externalcontact'])
    # Set components
    dtcomp = models.DatatypeComponent.objects.get(datatype_id=dset.datatype_id,
            component=models.DatasetUIComponent.DEFINITION)
    models.DatasetComponentState.objects.create(dtcomp=dtcomp,
                                                dataset_id=dset.id,
                                                state=models.DCStates.OK)
    models.DatasetComponentState.objects.bulk_create([
        models.DatasetComponentState(
            dtcomp=x, dataset_id=dset.id, state=models.DCStates.NEW) for x in
        models.DatatypeComponent.objects.filter(
            datatype_id=dset.datatype_id).exclude(
            component=models.DatasetUIComponent.DEFINITION)])
    models.ProjectLog.objects.create(project=project, level=models.ProjLogLevels.INFO,
            message=f'Dataset {dset.pk} created by user {user_id}')
    return dset


@login_required
@require_POST
def move_project_cold(request):
    '''Closes project:
        - Set to not active in DB
        - For each dataset, archive if needed, and remove from servers
    '''
    # TODO close_project this should be called
    data = json.loads(request.body.decode('utf-8'))
    if 'item_id' not in data or not data['item_id']:
        return JsonResponse({'state': 'error', 'error': 'No project specified for closing'}, status=400)
    projquery = models.Project.objects.filter(pk=data['item_id'], active=True)
    if not projquery:
        return JsonResponse({'state': 'error', 'error': 'Project is retired, purged or never existed'}, status=400)
    # Retiring a project is only allowed if user owns ALL datasets in project or is staff
    dsetowners = models.DatasetOwner.objects.filter(dataset__runname__experiment__project_id=data['item_id'], dataset__purged=False).select_related('dataset')
    if dsetowners.filter(user=request.user).count() != dsetowners.distinct('dataset').count() and not request.user.is_staff:
        return JsonResponse({'state': 'error', 'error': 'User has no permission to retire this project, does not own all datasets in project'}, status=403)
    # Cold store all datasets, delete them from active
    result = {'errormsgs': []}
    for dso in dsetowners.distinct('dataset'):
        archived = archive_and_delete_dataset(dso.dataset)
        if archived['state'] == 'error':
            result.update({'state': 'error', 'error': 'Not all datasets could be updated.'})
            result['errormsgs'].append(archived['error'])
    # if any dataset cannot be cold stored, report it, do not mark proj as retired
    if result['errormsgs']:
        projquery.update(active=True)
        result['error'] = '{} Errors: {}'.format(result['error'], '; '.join(result.pop('errormsgs')))
        return JsonResponse(result, status=500)
    else:
        projquery.update(active=False)
        models.ProjectLog.objects.create(project_id=data['item_id'],
                level=models.ProjLogLevels.INFO, message=f'User {request.user.id} closed project')
        return JsonResponse({})


@login_required
@require_POST
def merge_projects(request):
    """
    Takes list of projects and:
    - check if datasets are owned by user
    - check if no name collisions in experiments
      - if yes, check if no name collisions in experiment__runname
    - queue job to merge projects to the earliest/oldest project
    """
    data = json.loads(request.body.decode('utf-8'))
    if 'projids' not in data:
        return JsonResponse({'error': 'Incorrect request'}, status=400)
    if len(data['projids']) < 2:
        return JsonResponse({'error': 'Must select multiple projects to merge'}, status=400)
    projs = models.Project.objects.filter(pk__in=data['projids']).order_by('registered')
    if projs.count() != len(data['projids']):
        return JsonResponse({'error': 'Only {projs.count()} of the {len(data["projids"])}'
            'project(s) you have passed exist in the system, possibly project ' 
            'input is out of date?'}, status=400)
    qpid = get_quantprot_id()
    for proj in projs[1:]:
        # Refresh oldexps with every merged project
        oldexps = {x.name: x for x in projs[0].experiment_set.all()}
        dsshare_upds, dsshare_jobs = [], []
        for dset in  models.Dataset.objects.filter(runname__experiment__project=proj).values(
                'pk', 'deleted'):
            dsown_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(
                dataset_id=dset['pk']).values('user_id')]
            if not check_ownership(request.user, proj.ptype_id, dset['deleted'], dsown_ids):
                return JsonResponse({'error': f'You do not have the rights to move all datasets in project {proj.name}'}, status=403)
        for exp in proj.experiment_set.all():
            runnames_pks = [x.pk for x in exp.runname_set.all()]
            runnames = models.RunName.objects.filter(pk__in=runnames_pks)
            try:
                if exp.name not in oldexps:
                    exp.project = projs[0]
                    exp.save()
                    print(f'Experiment {exp.pk} moved to project {projs[0].pk} from project {proj.pk}')
                else:
                    runnames.update(experiment=oldexps[exp.name])
                    print(f'Runnames of experiment {exp.pk} moved to exp. {oldexps[exp.name].pk}')
                    exp.delete()
                    exp = oldexps[exp.name]
            except IntegrityError:
                # Unique indexes exist for project, experiment, runnames
                msg = 'Experiments/runnames collisions within merge'
                return JsonResponse({'error': msg}, status=500)
            # Get new dset storage_locs, see if any error, only update at end when no errors
            for runname in runnames:
                dset = runname.dataset
                pfds = dset.prefractionationdataset if hasattr(dset, 'prefractionationdataset') else False
                prefrac = pfds.prefractionation if pfds else False
                hrrange = pfds.hiriefdataset.hirief if hasattr(pfds, 'hiriefdataset') else False
                for dss in models.DatasetServer.objects.filter(active=True, dataset=dset):
                    # Get new storage location if moving stuff, and path/file names for error check on storedfile
                    new_storage_loc = dss.storageshare.set_dset_storage_location(qpid,
                            projs[0], exp, dset, dset.datatype, prefrac, hrrange)

                    err_fpath, err_fname = os.path.split(new_storage_loc)
                    # is_new_storloc is True if project is part of path, which is currently always
                    is_new_storloc = dss.storage_loc_ui != new_storage_loc
                    if models.DatasetServer.objects.exclude(dataset=dset).filter(storageshare=dss.storageshare,
                            storage_loc_ui=new_storage_loc).exists():
                        return JsonResponse({'error': 'There is already another dataset with that exact storage'
                            f' location: {new_storage_loc}'}, status=403)
                    elif is_new_storloc and filemodels.StoredFileLoc.objects.filter(servershare=dss.storageshare,
                            path=err_fpath, sfile__filename=err_fname).exists():
                        return JsonResponse({'error': 'There is already a file with that exact path '
                            f'{new_storage_loc}'}, status=403)
                    else:
                        # If files are in it that are not marked as (future) deleted,
                        # we do a rename job.
                        dss_sflocs = filemodels.StoredFileLoc.objects.filter(
                                sfile__rawfile__datasetrawfile__dataset_id=dset.id,
                                servershare=dss.storageshare, sfile__deleted=False)
                        if is_new_storloc and dss_sflocs.exists():
                            dsshare_upds.append((dss, new_storage_loc))
                            models.ProjectLog.objects.create(project=runname.experiment.project,
                                    level=models.ProjLogLevels.INFO,
                                    message=f'User {request.user.id} changed dataset '
                                    f'{dset.pk} path to {new_storage_loc} from {dss.storage_loc_ui}')

        # Now update:
        for dss, newstorloc in dsshare_upds:
            dss.storage_loc_ui = newstorloc
            dss.save()
            renamesfl = [x['pk'] for x in
                    filemodels.StoredFileLoc.objects.filter(servershare=dss.storageshare,
                        sfile__rawfile__datasetrawfile__dataset_id=dset.id).values('pk')]
            create_job('rename_dset_storage_loc', dss_id=dss.pk, sfloc_ids=renamesfl,
                newpath=new_storage_loc)
        for proj in projs[1:]:
            models.ProjectLog.objects.create(project=projs[0], level=models.ProjLogLevels.INFO,
                    message=f'User {request.user.id} merged in project {proj.pk}')
            # FIXME Also, should we possibly NOT chaneg anything here but only check pre the job, 
            # then merge after job complete?
            # In case of waiting times, job problems, etc? Prob doesnt matter much.
        proj.delete()
    return JsonResponse({})


@login_required
@require_POST
def rename_project(request):
    """
    Rename project in database, jobs queued to move all data to new name
    """
    data = json.loads(request.body.decode('utf-8'))
    try:
        proj = models.Project.objects.get(pk=data['projid'])
    except models.Project.DoesNotExist:
        return JsonResponse({'error': f'Project with that ID does not exist in DB'}, status=404)
    # check if new project not already exist, and user have permission for all dsets
    if data['newname'] == proj.name or models.Project.objects.filter(name=data['newname']).exists():
        return JsonResponse({'error': f'Cannot change name to existing name for project {proj.name}'}, status=403)
    elif is_invalid_proj_exp_runnames(data['newname']):
        return JsonResponse({'error': f'Project name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
    dsets = models.Dataset.objects.filter(runname__experiment__project=proj)
    for dset in  dsets.values('pk', 'deleted'):
        dsown_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(dataset_id=dset['pk']).values('user_id')]
        if not check_ownership(request.user, proj.ptype_id, dset['deleted'], dsown_ids):
            return JsonResponse({'error': f'You do not have the rights to change all datasets in this project'}, status=403)
    models.ProjectLog.objects.create(project=proj, level=models.ProjLogLevels.INFO,
            message=f'User {request.user.id} renamed project from {proj.name} to {data["newname"]}')
    proj.name = data['newname']
    proj.save()
    for dss in models.DatasetServer.objects.select_related('storageshare').filter(active=True,
            dataset__runname__experiment__project_id=data['projid']):
        newstorloc = dss.storageshare.rename_storage_loc_toplvl(data['newname'], dss.storage_loc_ui,
                data['projid'], dss.dataset_id)
        if newstorloc != dss.storage_loc_ui:
            # For some shares the project names is not in path
            dss.storage_loc_ui = newstorloc
            dss.save()
            renamesfl = [x['pk'] for x in 
                    filemodels.StoredFileLoc.objects.filter(servershare=dss.storageshare,
                        sfile__rawfile__datasetrawfile__dataset_id=dss.dataset_id).values('pk')]
            job = create_job('rename_dset_storage_loc', dss_id=dss.pk, sfloc_ids=renamesfl,
                    newpath=newstorloc)
    return JsonResponse({})


#@login_required
#def move_project_active(request):
    # This endpoint is disabled in URLs
    # FIXME This method should only be for admin, need to decouple 
    # - I have to send these raw files somewhere / reanalyze
    # - I'm going to edit the datasets
    # Data should always be avail to reanalyze (e.g. on sens server) until purge from backup
    # This should be fixed when doing the new raw file setup (avail on analysis server, etc)

    #data = json.loads(request.body.decode('utf-8'))
    #if 'item_id' not in data or not data['item_id']:
    #    return JsonResponse({'state': 'error', 'error': 'No project specified for reactivating'}, status=400)
    #projquery = models.Project.objects.filter(pk=data['item_id'], active=False)
    #if not projquery:
    #    return JsonResponse({'state': 'error', 'error': 'Project is already active, or does not exist'}, status=403)
    ## Reactivating a project is only allowed if user owns ALL datasets in project or is staff
    #dsetowners = models.DatasetOwner.objects.filter(dataset__runname__experiment__project_id=data['item_id'], dataset__purged=False).select_related('dataset')
    #if dsetowners.filter(user=request.user).count() != dsetowners.distinct('dataset').count() and not request.user.is_staff:
    #    return JsonResponse({'state': 'error', 'error': 'User has no permission to reactivate this project, does not own all datasets in project'}, status=403)
    ## Reactivate all datasets
    #result = {'errormsgs': []}
    #for dso in dsetowners.distinct('dataset'):
    #    reactivated = reactivate_dataset(dso.dataset)
    #    if reactivated['state'] == 'error':
    #        result.update({'state': 'error', 'error': 'Not all project datasets could be reactivated.'})
    #        result['errormsgs'].append(f'Problem with dataset {dso.dataset.pk}: {reactivated["error"]}')
    #    else:
    #        # if ANY dataset gets reactivated, project is active
    #        projquery.update(active=True)
    #if result['errormsgs']:
    #    result['error'] = f'{result["error"]} {"; ".join(result.pop("errormsgs"))}'
    #    return JsonResponse(result, status=500)
    #else:
    #    models.ProjectLog.objects.create(project_id=data['item_id'], level=models.ProjLogLevels.INFO,
    #            message=f'User {request.user.id} reopened project')
    #    return JsonResponse({})


@login_required
def purge_project(request):
    """Deletes project datasets (not analyses) from disk, AND BACKUP!"""
    data = json.loads(request.body.decode('utf-8'))
    if 'item_id' not in data or not data['item_id']:
        return JsonResponse({'state': 'error', 'error': 'No project specified for reactivating'}, status=400)
    projquery = models.Project.objects.filter(pk=data['item_id'], active=False)
    if not projquery:
        return JsonResponse({'state': 'error', 'error': 'Project does not exist or is still active'}, status=403)
    dsetowners = models.DatasetOwner.objects.filter(dataset__runname__experiment__project_id=data['item_id'], dataset__purged=False).select_related('dataset')
    if not request.user.is_staff:
        return JsonResponse({'state': 'error', 'error': 'User has no permission to purge this project, must be staff'}, status=403)
    result = {'errormsgs': []}
    for dso in dsetowners.distinct('dataset'):
        purged = delete_dataset_from_cold(dso.dataset)
        if purged['state'] == 'error':
            result.update({'state': 'error', 'error': 'Not all project datasets could be purged'})
            result['errormsgs'].append(purged['error'])
    # if any dataset cannot be purged, report it, do not mark proj as purged
    if result['errormsgs']:
        result['error'] = '{} Errors: {}'.format(result['error'], '; '.join(result.pop('errormsgs')))
        return JsonResponse(result, status=500)
    else:
        projquery.update(active=False)
        models.ProjectLog.objects.create(project_id=data['item_id'], level=models.ProjLogLevels.INFO,
                message=f'User {request.user.id} purged project')
        return JsonResponse({})


def get_dset_storestate(dset_id, ds_deleted):
    '''Count SFLoc associated with a dataset (created when adding/removing), in different 
    servershares and in backup - return a state for the dataset (currently a string)'''
    dsf_c = filemodels.StoredFile.objects.exclude(mzmlfile__isnull=False).filter(
                rawfile__datasetrawfile__dataset_id=dset_id).count()
    if dsf_c == 0:
        return 'empty', '-'
    nr_active_coldfiles = filemodels.PDCBackedupFile.objects.filter(
            storedfile__rawfile__datasetrawfile__dataset_id=dset_id).filter(deleted=False).count()
    if ds_deleted:
        if dsf_c == nr_active_coldfiles:
            storestate = 'cold'
        elif nr_active_coldfiles == 0:
            storestate = 'purged'
        else:
            storestate = 'broken'
    else:
        nrs_active = filemodels.StoredFileLoc.objects.exclude(sfile__mzmlfile__isnull=False).filter(
                    sfile__rawfile__datasetrawfile__dataset_id=dset_id, active=True).values(
                            'servershare').annotate(nrss=Count('pk'))
        if nrs_active.exists():
            max_nr_active = max(x['nrss'] for x in nrs_active)
        else:
            max_nr_active = 0
        if max_nr_active == dsf_c == nr_active_coldfiles:
            storestate = 'complete'
        elif dsf_c == max_nr_active:
            storestate = 'active-only'
        else:
            storestate = 'broken'
    return storestate, dsf_c


def archive_and_delete_dataset(dset):
    # TODO - archiving datasets should not really be done anymore, is done on individual incoming files now
    # we can archive any non-archived dataset left on the system?

    # FIXME dataset reactivating and archiving reports error when ok and vv? I mean, if you click archive and reactivate quickly, you will get error (still in active storage), and also in this func, storestate is not updated at same time as DB (it is result of jobs)
    if dset.deleted:
        return {'state': 'error', 'error': 'Cannot archive dataset, it is already in '
                'deleted state'}
    else:
        backing_up = False
        storestate, _ = get_dset_storestate(dset.pk, dset.deleted)
        alldss = models.DatasetServer.objects.filter(dataset=dset, active=True)
        if storestate == 'broken':
            return {'state': 'error', 'error': 'Cannot archive dataset, files missing on storage'}
        elif storestate == 'active-only':
            srcdss = get_source_dss_for_transfers(alldss)
            srcsfl = filemodels.StoredFileLoc.objects.exclude(sfile__mzmlfile__isnull=False).filter(
                    sfile__rawfile__datasetrawfile__dataset__datasetserver=srcdss).values('pk')
            create_job('backup_dataset', dss_id=srcdss.pk, sfloc_ids=[x['pk'] for x in srcsfl])
            backing_up = True

        # If you reach this point, the dataset is either backed up or has a job queued for that, so queue delete job
        bup_msg = 'Files are already in backup'
        if storestate != 'empty':
            rmjobs, rmsfls, rmdirjobs = [], [], []
            for dss in alldss.values('pk', 'storage_loc', 'storageshare_id'):
                rmsfl = filemodels.StoredFileLoc.objects.filter(active=True,
                        sfile__rawfile__datasetrawfile__dataset__datasetserver__id=dss['pk'])
                sfloc_pks = [x['pk'] for x in rmsfl.values('pk')]
                rmkw = {'dss_id': dss['pk'], 'sfloc_ids': sfloc_pks}
                if joberror := check_job_error('remove_dset_files_servershare', **rmkw):
                    bup_msg = 'Files are queued for backup' if backing_up else 'Files already backed up'
                    return {'state': 'error', 'error': f'{joberror} -- {bup_msg}'}
                rmjobs.append(rmkw)
                rmsfls.append(rmsfl)
                rmdirkw = {'path': dss['storage_loc'], 'share_id': dss['storageshare_id'],
                        'sfloc_ids': sfloc_pks}
                if joberror := check_job_error('delete_empty_directory', **rmdirkw):
                    return {'state': 'error', 'error': f'{joberror} -- {bup_msg}'}
                rmdirjobs.append(rmdirkw)
            # Errors checked for all dss, now queue jobs
            for rmjobkw, rmsfl, rmdirkw in zip(rmjobs, rmsfls, rmdirjobs):
                create_job_without_check('remove_dset_files_servershare', **rmjobkw)
                rmsfl.update(active=False)
                create_job('delete_empty_directory', **rmdirkw)
            filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset).update(
                    deleted=True)
        # Empty no-file-yet dsets also have dss, set all to non-active
        alldss.update(active=False)

        dset.deleted, dset.purged = True, False
        dset.save()
        return {'state': 'ok', 'error': f'Dataset queued for removal -- {bup_msg}'}


def reactivate_dataset(dset, share_ids, project, experiment, dtype, prefrac, hrrange, user_id):
    '''Tries to reactivate a dataset, restoring it to dataset inbox share and then queuing rsync job'''
    # FIXME if reactivated quickly after archiving, you get an error. Because archive job is queued and it will execute, 
    # but that is not done yet, so we wont queue a reactivate job. Fine. But, if archive job is deleted, 
    # the dataset is still marked as deleted. In that case, we could set undelete on dset here, but that is not correct
    # when the dset is archived afterwards by said job.
    # Another point is that if a dataset has a red error job in front of it, it will not be deactivated, job will wait
    # and dset sits in deleted mode, unable to reactivate. 
    # 
    # Solutions:
    #  - delete/undelete ALSO in post-job view - bad UI, dataset keeps showing in interface, should not
    #  - reactivate cancels delete job (if it is pending
    #  - 

    if not len(share_ids):
        return {'state': 'error', 'error': 'Must reactivate to a specified storage share'}
    storestate, _ = get_dset_storestate(dset.pk, dset.deleted)
    if dset.deleted:
        if storestate == 'purged':
            return {'state': 'error', 'error': 'Cannot reactivate purged dataset'}
        elif storestate == 'empty':
            dset.deleted, dset.purged, dset.runname.experiment.project.active = False, False, True
            dset.save()
            dset.runname.experiment.project.save()
            if upd_stor_err := update_storage_shares(share_ids, project, experiment, dset,
                    dtype, prefrac, hrrange, user_id):
                msg = json.loads(upd_stor_err.content)['error']
                return {'state': 'error', 'error': 'Error trying to update storage for reactivating '
                        f'dataset, please contact admin. Error msg: {msg}'}
            return {'state': 'ok', 'error': 'Empty dataset (contains no files) re-activated'}
        elif storestate == 'cold':
            dset.deleted, dset.purged, dset.runname.experiment.project.active = False, False, True
            dset.save()
            dset.runname.experiment.project.save()
            # retrieve to inbox - a guaranteed sensitive data share on the backup controller
            inbox_share = filemodels.ServerShare.objects.get(function=filemodels.ShareFunction.INBOX)
            buppath = filemodels.ServerShare.get_inbox_path(dset_id=dset.pk)
            upd_defaults = {'active': True, 'storage_loc_ui': buppath}
            bupdss, _ = models.DatasetServer.objects.update_or_create(dataset=dset,
                    storageshare=inbox_share, defaults=upd_defaults, create_defaults={
                        **upd_defaults, 'startdate': timezone.now(), 'storage_loc': buppath})
            sfs = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset,
                    mzmlfile__isnull=True)
            sfs.update(deleted=False)
            sfls_inbox = filemodels.StoredFileLoc.objects.filter(sfile__in=sfs,
                    servershare=inbox_share)
            if sfls_inbox.count() < sfs.count():
                filemodels.StoredFileLoc.objects.bulk_create([filemodels.StoredFileLoc(sfile=sf, servershare=inbox_share, path=buppath) for sf in sfs.exclude(storedfileloc__in=sfls_inbox)])
            sfls_inbox.update(path=buppath, active=True)

            sflids = [x['pk'] for x in sfls_inbox.values('pk')]
            create_job('reactivate_dataset', dss_id=bupdss.pk, sfloc_ids=sflids)
            if upd_stor_err := update_storage_shares(share_ids, project, experiment, dset,
                    dtype, prefrac, hrrange, user_id):
                msg = json.loads(upd_stor_err.content)['error']
                return {'state': 'error', 'error': 'Could not rsync dataset directly to storage, '
                        f'please try manually or contact admin. Error was: {msg}'}
            return {'state': 'ok'}
        elif storestate == 'broken':
            return {'state': 'error', 'error': 'Cannot reactivate dataset, files missing in backup storage'}

    else:
        if storestate == 'broken':
            return {'state': 'error', 'error': 'Cannot reactivate dataset, files missing in backup storage'}
        elif storestate in ['active-only', 'complete']:
            return {'state': 'error', 'error': 'Dataset already in active storage'}
        elif storestate == 'empty':
            dset.deleted, dset.purged, dset.runname.experiment.project.active = False, False, True
            dset.save()
            dset.runname.experiment.project.save()
            return {'state': 'ok', 'error': 'Empty dataset (contains no files) re-activated'}


@login_required
def move_dataset_cold(request):
    '''Calls archive_and_delete_dataset (shared with project -> cold) to remove
    dataset from all active storage after (if needed) backing it up'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = models.Dataset.objects.select_related('runname__experiment__project').get(pk=data['item_id'])
    except models.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist'}, status=403)
    dsownq = models.DatasetOwner.objects.filter(dataset_id=dset.pk)
    if not check_ownership(request.user, dset.runname.experiment.project.ptype_id,
            dset.deleted, [x['user_id'] for x in dsownq.values('user_id')]):
        return JsonResponse({'error': 'Cannot archive dataset, no permission for user'}, status=403)
    archived = archive_and_delete_dataset(dset)
    if archived['state'] == 'error':
        return JsonResponse(archived, status=500)
    else:
        models.ProjectLog.objects.create(project=dset.runname.experiment.project,
                level=models.ProjLogLevels.INFO,
                message=f'User {request.user.id} deactivated dataset {dset.pk}')
        return JsonResponse(archived)


@login_required
def move_dataset_active(request):
    '''Calls reactivate_dataset (shared with currently disabled
    reactivate project endpoint) to move a dataset from backup
    into (not-user-readable) TMP share, whereafter rsync to user-selected shares'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = models.Dataset.objects.select_related('runname__experiment__project', 'datatype',
            'prefractionationdataset__prefractionation').get(pk=data['dataset_id'])
    except models.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist'}, status=403)
    # check_ownership without deleted demand:
    pt_id = dset.runname.experiment.project.ptype_id 
    if request.user.id in get_dataset_owners_ids(dset) or request.user.is_staff:
        pass
    elif pt_id == settings.LOCAL_PTYPE_ID:
        return JsonResponse({'error': 'Cannot reactivate dataset, no permission for user'}, status=403)
    else:
        try:
            models.UserPtype.objects.get(ptype_id=pt_id, user_id=request.user.id)
        except models.UserPtype.DoesNotExist:
            return JsonResponse({'error': 'Cannot reactivate dataset, no permission for user'}, status=403)
    
    prefrac = dset.prefractionationdataset.prefractionation if hasattr(dset, 'prefractionationdataset') else False
    hrrange = prefrac.hiriefdataset if prefrac and hasattr(prefrac, 'hiriefdataset') else False
    reactivated_msg = reactivate_dataset(dset, data['storage_shares'],
            dset.runname.experiment.project, dset.runname.experiment, dset.datatype, prefrac, hrrange, request.user.id)
    if reactivated_msg['state'] == 'error':
        return JsonResponse(reactivated_msg, status=500)
    else:
        models.ProjectLog.objects.create(project=dset.runname.experiment.project,
                level=models.ProjLogLevels.INFO,
                message=f'User {request.user.id} reactivated dataset {dset.pk}')
        return JsonResponse(reactivated_msg)


def delete_dataset_from_cold(dset):
    return {'state': 'error', 'error': 'Cannot permanent delete yet, not implemented'}
    # TODO Should we allow direct purging? the delete from active job is fired anyway
    if not dset.deleted:
        return {'state': 'error', 'error': 'Dataset is not deleted, will not purge'}
    dset.purged = True
    dset.save()
    # Also create delete active job just in case files are lying around
    # FIXME add check if active on primary share
    create_job('remove_dset_files_servershare', dset_id=dset.id, sfloc_ids=[]) # FIXME dss, sfloc_ids
    create_job('delete_empty_directory', sf_ids=[x.id for x in filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset)])
    storestate, _ = get_dset_storestate(dset.pk, dset.deleted)
    if storestate != 'empty':
        create_job('delete_dataset_coldstorage', dset_id=dset.id)
    sfids = [sf.id for dsrf in dset.datasetrawfile_set.select_related('rawfile') for sf in dsrf.rawfile.storedfile_set.all()]
    create_job('delete_empty_directory', sf_ids=sfids)
    return {'state': 'ok', 'error': 'Dataset queued for permanent deletion'}


@login_required
def purge_dataset(request):
    """Deletes dataset from disk, only leaving backup copies."""
    data = json.loads(request.body.decode('utf-8'))
    if not request.user.is_staff:
        return JsonResponse({'error': 'Only admin can purge dataset'}, status=403)
    try:
        dset = models.Dataset.objects.select_related('runname__experiment__project').get(pk=data['item_id'])
    except models.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist'}, status=403)
    purgemsg = delete_dataset_from_cold(dset)
    if purgemsg['state'] == 'error':
        return JsonResponse(purgemsg, status=500)
    else:
        models.ProjectLog.objects.create(project=dset.runname.experiment.project,
                level=models.ProjLogLevels.INFO,
                message=f'User {request.user.id} purged dataset {dset.pk}')
        return JsonResponse(purgemsg)


def is_invalid_proj_exp_runnames(name):
    """Validates any project/experiment/run names for datasets"""
    return re.search(f'[^{settings.ALLOWED_PROJEXPRUN_CHARS}]', name)


@login_required
def lock_dataset(request):
    data = json.loads(request.body.decode('utf-8'))
    if data['dataset_id']:
        user_denied, status = check_save_permission(data['dataset_id'], request.user)
        if user_denied:
            return JsonResponse({'error': user_denied}, status=status)
    else:
        return JsonResponse({'error': 'Can not find dataset to lock'}, status=404)
    updated = models.Dataset.objects.filter(pk=data['dataset_id'], locked=False).update(locked=True)
    if not updated:
        return JsonResponse({'error': 'Can not find dataset to lock'}, status=404)
    proj = models.Project.objects.get(experiment__runname__dataset__id=data['dataset_id'])
    models.ProjectLog.objects.create(project=proj, level=models.ProjLogLevels.INFO,
            message=f'User {request.user.id} locked dataset {data["dataset_id"]}')
    return JsonResponse({})


@login_required
def unlock_dataset(request):
    data = json.loads(request.body.decode('utf-8'))
    if data['dataset_id']:
        user_denied, status = check_save_permission(data['dataset_id'], request.user, action='unlock')
        if user_denied:
            return JsonResponse({'error': user_denied}, status=status)
    else:
        return JsonResponse({'error': 'Can not find dataset to unlock'}, status=404)
    analysis_q = am.Analysis.objects.filter(datasetanalysis__dataset_id=data['dataset_id'])
    if analysis_q.filter(nextflowsearch__job__state__in=jj.JOBSTATES_JOB_SENT).exists():
        return JsonResponse({'error': 'Dataset is currently in an analysis and you cannot edit it'},
                status=403)
    elif analysis_q.filter(nextflowsearch__job__state=jj.Jobstates.DONE).exists():
        usrmessage = (f'WARNING - Dataset {data["dataset_id"]} has one or more analyses that are '
            'already finished. Editing the dataset or adding/removing files can distort the view of the '
            'data that analysis has resulted in')
    elif analysis_q.filter(nextflowsearch__job__state__in=[jj.Jobstates.WAITING, jj.Jobstates.HOLD,
            jj.Jobstates.PENDING]).exists():
        usrmessage = (f'WARNING - Dataset {data["dataset_id"]} has one or more analyses that are '
            'not yet run. Unlocking and editing can potentially change the analyses')
    else:
        usrmessage = f'Dataset {data["dataset_id"]} unlocked'
    updated = models.Dataset.objects.filter(pk=data['dataset_id'], locked=True).update(locked=False)
    if not updated:
        return JsonResponse({'error': 'Can not find dataset to unlock'}, status=404)
    proj = models.Project.objects.get(experiment__runname__dataset__id=data['dataset_id'])
    models.ProjectLog.objects.create(project=proj, level=models.ProjLogLevels.INFO,
            message=f'User {request.user.id} unlocked dataset {data["dataset_id"]}')
    return JsonResponse({'msg': usrmessage})


@login_required
def save_dataset(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'newexperimentname' in data and is_invalid_proj_exp_runnames(data['newexperimentname']):
        return JsonResponse({'error': f'Experiment name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
    if is_invalid_proj_exp_runnames(data['runname']):
        return JsonResponse({'error': f'Run name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)

    if data['dataset_id']:
        user_denied, status = check_save_permission(data['dataset_id'], request.user)
        if user_denied:
            return JsonResponse({'error': user_denied}, status=status)
        return update_dataset(data, request.user.id)
    else:
        try:
            project = models.Project.objects.get(pk=data['project_id'], active=True)
        except models.Project.DoesNotExist:
            return JsonResponse({'error': f'Project {data["project_id"]} is not active'}, status=403)
        if data['datatype_id'] in settings.LC_DTYPE_IDS:
            experiment, _ = models.Experiment.objects.get_or_create(project=project, name=settings.LCEXPNAME)
            try:
                runname = models.RunName.objects.create(name=data['runname'], experiment=experiment)
            except IntegrityError:
                return JsonResponse({'error': 'There already exists a run name by that '
                    'name for this experiment! Reload the page if it is not in the dropdown.'}, status=403)
        else:
            if 'newexperimentname' in data and data['newexperimentname']:
                try:
                    experiment = models.Experiment.objects.create(name=data['newexperimentname'],
                            project=project)
                except IntegrityError:
                    return JsonResponse({'error': 'There already exists an experiment by that '
                        'name for this project! Reload the page if it is not in the dropdown.'}, status=403)
            elif 'experiment_id' in data:
                try:
                    experiment = models.Experiment.objects.get(pk=data['experiment_id'])
                except (models.Experiment.DoesNotExist, ValueError):
                    return JsonResponse({'error': 'Experiment ID not found!'}, status=404)
            else:
                return JsonResponse({'error': 'You  must fill in an experiment'}, status=400)
            try:
                runname = models.RunName.objects.create(name=data['runname'], experiment=experiment)
            except IntegrityError:
                return JsonResponse({'error': 'There already exists a run name by that '
                    'name for this experiment! Reload the page if it is not in the dropdown.'}, status=403)
        # Have runname/exp/project, now save rest of dataset:
        try:
            dset = save_new_dataset(data, project, experiment, runname, request.user.id)
        except IntegrityError as e:
            runname.delete()
            return JsonResponse({'state': 'error', 'error': f'Cannot save dataset, {e}'}, status=403)
    return JsonResponse({'dataset_id': dset.id})


def save_dataset_prefrac(dset_id, data):
    hrf_id, hiph_id = get_prefrac_ids()
    pfds = models.PrefractionationDataset(
        dataset_id=dset_id, prefractionation_id=data['prefrac_id'])
    pfds.save()
    models.PrefractionationFractionAmount.objects.create(
        pfdataset=pfds, fractions=data['prefrac_amount'])
    if data['prefrac_id'] == hrf_id:
        models.HiriefDataset.objects.create(pfdataset=pfds,
                                            hirief_id=data['hiriefrange'])
    else:
        models.PrefractionationLength.objects.create(
            pfdataset=pfds, length=data['prefrac_length'])


def update_prefrac_len(pfds, data):
    pass


def update_dataset_prefrac(pfds, data):
    hrf_id, hiph_id = get_prefrac_ids()
    if pfds.prefractionation_id != data['prefrac_id']:
        if pfds.prefractionation_id == hrf_id:
            models.HiriefDataset.objects.filter(pfdataset=pfds).delete()
            models.PrefractionationLength.objects.create(
                pfdataset=pfds, length=data['prefrac_length'])
        elif data['prefrac_id'] == hrf_id:
            models.PrefractionationLength.objects.filter(pfdataset=pfds).delete()
            models.HiriefDataset.objects.create(pfdataset=pfds,
                                                hirief_id=data['hiriefrange'])
        else:
            models.PrefractionationLength.objects.filter(pfdataset=pfds).update(
                length=data['prefrac_length'])
        pfds.prefractionation_id = data['prefrac_id']
        pfds.save()
    else:
        if data['prefrac_id'] == hrf_id:
            if pfds.hiriefdataset != data['hiriefrange']:
                pfds.hiriefdataset.hirief_id = data['hiriefrange']
                pfds.hiriefdataset.save()
        else:
            if pfds.prefractionationlength.length != data['prefrac_length']:
                pfds.prefractionationlength.length = data['prefrac_length']
                pfds.prefractionationlength.save()
    # update fr amount if neccessary
    pffa = pfds.prefractionationfractionamount
    if (pffa.fractions != data['prefrac_amount']):
        pffa.fractions = data['prefrac_amount']
        pffa.save()


@login_required
def get_project(request, project_id):
    proj = models.Project.objects.select_related('pi', 'ptype').get(pk=project_id)
    return JsonResponse({
        'id': project_id, 'pi_name': proj.pi.name, 'name': proj.name,
		'ptype_name': proj.ptype.name, 'isExternal': proj.ptype_id != settings.LOCAL_PTYPE_ID,
        'projsamples': {x.id: {'name': x.sample, 'id': x.id} for x in models.ProjectSample.objects.filter(project_id=project_id)},
        'experiments': [{'id': x.id, 'name': x.name} for x in 
        models.Experiment.objects.filter(project_id=project_id)]})


def pf_dataset_info_json(pfds):
    resp_json = {'prefrac_id': pfds.prefractionation.id,
                 'prefrac_amount': pfds.prefractionationfractionamount.fractions}
    if hasattr(pfds, 'hiriefdataset'):
        resp_json.update({'hiriefrange': pfds.hiriefdataset.hirief.id})
    else:
        resp_json.update({'prefrac_length': pfds.prefractionationlength.length})
    return resp_json


def get_empty_isoquant():
    quants = {}
    for chan in models.QuantTypeChannel.objects.all().select_related(
            'quanttype', 'channel'):
        if not chan.quanttype.id in quants:
            quants[chan.quanttype.id] = {'id': chan.quanttype.id, 'chans': [],
                                         'name': chan.quanttype.name}
        quants[chan.quanttype.id]['chans'].append({'id': chan.id,
                                                   'name': chan.channel.name,
                                                   'samplename': '',
                                                   'species': [],
                                                   'species_error': [],
                                                   'sampletypes': [],
                                                   'sampletypes_error': [],
                                                   'model': ''})
        # Trick to sort N before C:
        quants[chan.quanttype.id]['chans'].sort(key=lambda x: x['name'].replace('N', 'A'))
    return quants



def fill_sampleprepparam(params, p, value=False):
    # This is now only used for sample prep params. Keep around for when that changes
    """Fills params dict with select parameters passed, in proper JSON format
    This takes care of both empty params (for new dataset), filled parameters,
    and old parameters"""
    # If reusing this, you need to make a new version for non sampleprep
    # that uses f'select_{p.param.id}' - this is to not mix them with e.g. fieldparam
    # with same ID
    #p_id = f'select_{p.param.id}'
    p_id = p.param_id
    if p_id not in params:
        params[p_id] = {'param_id': p.param.id, 'fields': [],
                              'inputtype': 'select', 'title': p.param.title}
    if value:
        # fields is already populated by call to empty params
        params[p_id]['model'] = value
    else:
        params[p_id]['model'] = ''
        params[p_id]['fields'].append({'value': p.id, 'text': p.value})


def fill_admin_checkboxparam(params, p, value=False):
    """Fills params dict with select parameters passed, in proper JSON format
    This takes care of both empty params (for new dataset), filled parameters,
    and old parameters"""
    # param must have a model for ability of checking if it is new param
    # which dataset does not have
    p_id = f'cb_{p.param.id}'
    if p_id not in params:
        params[p_id] = {'param_id': p.param.id, 'fields': [], 'model': True,
                              'inputtype': 'checkbox', 'title': p.param.title}
    if value:
        # fields key is already populated by call to empty params
        for box in params[p_id]['fields']:
            if box['value'] == value:
                box['checked'] = True 
    else:
        params[p_id]['fields'].append({'checked': False, 'value': p.id, 'text': p.value})


def fill_admin_fieldparam(params, p, value=False):
    """Fills params dict with field parameters passed, in proper JSON format
    This takes care of both empty params (for new dataset), filled parameters,
    and old parameters"""
    p_id = f'field_{p.id}'
    params[p_id] = {'param_id': p.id, 'placeholder': p.placeholder,
                    'inputtype': p.paramtype.typename, 'title': p.title}
    params[p_id]['model'] = value if value else ''


def get_dynamic_emptyparams(category):
    # If you ever add more lab categories, they get empty params here
    # This used to be used by multiple components but has since 2024 or so
    # been collapsed into only a single component (first sampleprep/acquisition, and
    # later only acquisition) The latter removal of sample prep led to
    # the conversion of selectparam into sampleprep param (sample prep contained
    # no field params, or checkbox except dedicated enzyme box)
    params = {}
    for p in models.FieldParameter.objects.select_related(
            'paramtype').filter(category=category):
        if p.active:
            fill_admin_fieldparam(params, p)
    for p in models.CheckboxParameterOption.objects.select_related(
            'param').filter(param__category=category):
        if p.param.active:
            fill_admin_checkboxparam(params, p)
    return params




@login_required
@require_GET
def find_files(request):
    searchterms = [x for x in request.GET['q'].split(',') if x != '']
    query = Q(filename__icontains=searchterms[0])
    query |= Q(rawfile__producer__name__icontains=searchterms[0])
    for term in searchterms[1:]:
        subquery = Q(filename__icontains=term)
        subquery |= Q(rawfile__producer__name__icontains=term)
        query &= subquery
    newfiles = filemodels.StoredFile.objects.filter(query).filter(rawfile__claimed=False,
            checked=True, storedfileloc__servershare__function=filemodels.ShareFunction.INBOX
            ).values('rawfile__date', 'pk', 'filename', 'rawfile__size', 'rawfile__producer__name')
    return JsonResponse({
        'newfn_order': [x['pk'] for x in newfiles.order_by('-rawfile__date')],
        'newFiles': {x['pk']:
                         {'id': x['pk'], 'name': x['filename'], 
                          'size': round(x['rawfile__size'] / (2**20), 1),
                          'date': x['rawfile__date'].timestamp() * 1000,
                          'instrument': x['rawfile__producer__name'], 'checked': False}
                         for x in newfiles}})


def save_or_update_files(data, user_id):
    '''Called from views in jobs as well (reg external files), so broke out from request
    handling view'''
    dset_id = data['dataset_id']
    added_fnids = [x['id'] for x in data['added_files'].values()]
    removed_ids = [int(x['id']) for x in data['removed_files'].values()]
    dset = models.Dataset.objects.select_related('runname__experiment').get(pk=dset_id)
    alldss = dset.datasetserver_set.filter(active=True).values('pk', 'storageshare_id', 'storage_loc_ui')
    addsfl = filemodels.StoredFileLoc.objects.filter(sfile_id__in=added_fnids, sfile__checked=True,
            servershare__function=filemodels.ShareFunction.INBOX)
    # TODO what if addsfl files no longer on tmpshare? Need to retrieve from backup
    pre_addsfl = addsfl.filter(sfile__rawfile__claimed=False)
    rmsfl = filemodels.StoredFileLoc.objects.filter(sfile_id__in=removed_ids, sfile__checked=True,
            sfile__rawfile__claimed=True)
    if added_fnids:
        # First error check: we can only add files if they are only on one servershare (primary)
        # so not from another dataset/analysis etc.
        if len(added_fnids) > pre_addsfl.count():
            return {'error': 'Cannot find some of the files asked to store. They may have been '
                    'added to another dataset while your browser window was opened. They could '
                    'also have been removed from the inbox share and may need to be retrieved '
                    'from backup. Contact admin in case you cant figure it out'}, 403
        elif len(added_fnids) < pre_addsfl.count():
            return {'error': 'Something is wrong, added files are available but likely already exist '
                    'on multiple file servers. This should not happen, please contact admin'}, 403
        for dss in alldss:
            if joberr := check_job_error('rsync_dset_files_to_servershare', dss_id=dss['pk'],
                    sfloc_ids=[x['pk'] for x in pre_addsfl.values('pk')], dstsfloc_ids=[],
                    dstshare_id=dss['storageshare_id']):
                return {'error': joberr}, 403

    if removed_ids:
        if len(removed_ids) > filemodels.StoredFile.objects.filter(pk__in=removed_ids,
                rawfile__datasetrawfile__dataset_id=dset_id).count():
            return {'error': 'Some of the files you specified to remove from the dataset do not '
                    'belong to this dataset. Maybe you are working in an old browser tab?' }, 403
        elif len(removed_ids) > rmsfl.distinct('sfile_id').count():
            return {'error': 'Cannot find some of the files asked to remove on storage. '
                    'This should not happen. Contact admin' }, 403

    # Errors checked, now store DB records and queue move jobs
    if added_fnids:
        # First add datasetrawfile - then create job, since waiting-for-files depends on
        # datasetrawfiles at job creation time
        models.DatasetRawFile.objects.bulk_create([
            models.DatasetRawFile(dataset_id=dset_id, rawfile_id=rfnid['rawfile_id'])
            for rfnid in filemodels.StoredFile.objects.filter(pk__in=added_fnids
                ).values('rawfile_id')])
        models.ProjectLog.objects.create(project_id=dset.runname.experiment.project_id,
                level=models.ProjLogLevels.INFO,
                message=f'User {user_id} added files {",".join([str(x) for x in added_fnids])} '
                f'to dataset {dset_id}')
        filemodels.RawFile.objects.filter(storedfile__id__in=added_fnids).update(claimed=True)
        for dss in alldss:
            # Error check already done above before any creation to be able to return 403
            # without side effects
            create_job_without_check('rsync_dset_files_to_servershare', dss_id=dss['pk'],
                    # need to re-filter addsfl since have changed claimed=True now
                    sfloc_ids=[x['pk'] for x in addsfl.filter(sfile__rawfile__claimed=True).values('pk')],
                    dstshare_id=dss['storageshare_id'])

    if removed_ids:
        # Create job first so the dataset files are waited for by other jobs since that depends
        # on datasetrawfiles on job creation time
        for dss in alldss:
            dssrmsfl = [x['pk'] for x in rmsfl.filter(servershare_id=dss['storageshare_id']).values('pk')]
            if len(dssrmsfl):
                # TODO maybe first rsync them back to tmpshare, in case
                # they are "released"
                create_job('remove_dset_files_servershare', dss_id=dss['pk'], sfloc_ids=dssrmsfl)
        rmsfl.update(active=False)
        models.DatasetRawFile.objects.filter(
            dataset_id=dset_id, rawfile__storedfile__id__in=removed_ids).delete()
        filemodels.RawFile.objects.filter(storedfile__pk__in=removed_ids).update(
            claimed=False)
        models.ProjectLog.objects.create(project_id=dset.runname.experiment.project_id,
                level=models.ProjLogLevels.INFO,
                message=f'User {user_id} removed files {",".join([str(x) for x in removed_ids])} '
                f'from dataset {dset_id}')
        
    # If files changed and labelfree, set sampleprep component status
    # to not good. Which should update the tab colour (green to red)
    try:
        qtype = models.Dataset.objects.select_related(
            'quantdataset__quanttype').get(pk=dset_id).quantdataset.quanttype
    except models.QuantDataset.DoesNotExist:
        pass
    else:
        if (added_fnids or removed_ids) and qtype.name == 'labelfree':
            set_component_state(dset_id, models.DatasetUIComponent.SAMPLES,
                    models.DCStates.INCOMPLETE)
    set_component_state(dset_id, models.DatasetUIComponent.FILES, models.DCStates.OK)
    return {'error': False}, 200


@login_required
def accept_or_reject_dset_preassoc_files(request):
    # FIXME handle errors in classified function!
    data = json.loads(request.body.decode('utf-8'))
    user_denied, status = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    # First remove jobs on rejected files and un-claim rawfiles
    dss_ids = [x['pk'] for x in
            models.DatasetServer.objects.filter(dataset_id=data['dataset_id']).values('pk')]
    if len(data['rejected_files']):
        rejsfl = filemodels.StoredFileLoc.objects.filter(sfile_id__in=data['rejected_files'])
        rej_sflocs = [x['pk'] for x in rejsfl.values('pk')]
        rsjobs = jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare',
                state=jj.Jobstates.HOLD, kwargs__dss_id__in=dss_ids,
                kwargs__sfloc_ids__0__in=rej_sflocs, kwargs__dstsfloc_ids__0__in=rej_sflocs)
        dstsfloc_ids = [x for j in rsjobs for x in j.kwargs['dstsfloc_ids']]
        filemodels.StoredFileLoc.objects.filter(pk__in=dstsfloc_ids).update(active=False)
        deleted = rsjobs.delete()
        if deleted[0]:
            filemodels.RawFile.objects.filter(
                    storedfile__pk__in=data['rejected_files']).update(claimed=False)
    # Now start jobs for accepted files
    if len(data['accepted_files']):
        acc_sflocs = filemodels.StoredFileLoc.objects.filter(sfile_id__in=data['accepted_files'])
        acc_sfloc_ids = [x['pk'] for x in acc_sflocs.values('pk')]
        # FIXME what if acc_sflocs no longer on TMPSHARE because time deleted?
        # Need to retrieve from backup!

        # Take advantage of the accepting files always only contain 1 file in the job,
        # as it is a result from classify_msrawfile
        accjobs = jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare',
                state=jj.Jobstates.HOLD, kwargs__dss_id__in=dss_ids,
                kwargs__dstsfloc_ids__0__in=acc_sfloc_ids, kwargs__sfloc_ids__0__in=acc_sfloc_ids)
        acc_dstsfl_ids = []
        for accjob in accjobs:
            acc_dstsfl_ids.extend(accjob.kwargs['dstsfloc_ids'])
        accjobs.update(state=jj.Jobstates.PENDING)
        models.DatasetRawFile.objects.bulk_create([
            models.DatasetRawFile(dataset_id=data['dataset_id'], rawfile_id=x['rawfile_id'])
            for x in filemodels.StoredFile.objects.filter(
                pk__in=data['accepted_files']).values('rawfile_id')])
        proj = models.Project.objects.get(experiment__runname__dataset__id=data['dataset_id'])
        models.ProjectLog.objects.create(project=proj, level=models.ProjLogLevels.INFO,
                message=f'User {request.user.id} accepted storedfiles '
                f'{",".join([str(x) for x in data["accepted_files"]])}, to dset {data["dataset_id"]}')
        # If files changed and labelfree, set sampleprep component status
        # to not good. Which should update the tab colour (green to red)
        try:
            qtype = models.Dataset.objects.select_related(
                'quantdataset__quanttype').get(pk=data['dataset_id']).quantdataset.quanttype
        except models.QuantDataset.DoesNotExist:
            pass
        else:
            set_component_state(data['dataset_id'], models.DatasetUIComponent.SAMPLES,
                    models.DCStates.INCOMPLETE)
        # Only set file component to OK if any files have been accepted
        set_component_state(data['dataset_id'], models.DatasetUIComponent.FILES, models.DCStates.OK)
    return JsonResponse({'error': False})
    # FIXME switch fileserver should happen - in that case on the classify start!


@login_required
def save_files(request):
    """Updates and saves files"""
    data = json.loads(request.body.decode('utf-8'))
    user_denied, status = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    err_result, status = save_or_update_files(data, request.user.id)
    return JsonResponse(err_result, status=status)


def update_msacq(dset, data):
    # FIXME oneliner:
    #models.OperatorDataset.objects.filter(dataset=dset).update(operator_id=data['operator_id'])
    if data['operator_id'] != dset.operatordataset.operator_id:
        dset.operatordataset.operator_id = data['operator_id']
        dset.operatordataset.save()
    # Just update is not enough since there may be datasets without acq mode
    models.AcquisistionModeDataset.objects.update_or_create(dataset=dset,
            defaults={'acqmode': data['acqmode']})
    if not hasattr(dset, 'reversephasedataset'):
        if data['rp_length']:
            models.ReversePhaseDataset.objects.create(dataset_id=dset.id,
                                                      length=data['rp_length'])
    elif not data['rp_length']:
            dset.reversephasedataset.delete()
    elif data['rp_length'] != dset.reversephasedataset.length:
        dset.reversephasedataset.length = data['rp_length']
        dset.reversephasedataset.save()
# FIXME do his instead
#    if hasattr(dset, 'reversephasedataset') and not data['rp_length']:
#        dset.reversephasedataset.delete()
#    else:
#        dm.ReversePhaseDataset.objects.update_or_create(dataset=dset,
#                defaults={'length': data['rp_length']})
    update_admin_defined_params(dset, data, models.Labcategories.ACQUISITION)
    return JsonResponse({})




@login_required
def save_ms_sampleprep(request):
    data = json.loads(request.body.decode('utf-8'))
    user_denied, status = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    dset_id = data['dataset_id']
    dset = models.Dataset.objects.filter(pk=data['dataset_id']).select_related().get()
    models.EnzymeDataset.objects.filter(dataset=dset).delete()
    if len([x for x in data['enzymes'] if x['checked']]):
        models.EnzymeDataset.objects.bulk_create([models.EnzymeDataset(dataset=dset,
            enzyme_id=enz['id']) for enz in data['enzymes'] if enz['checked']])

    if data['pipeline']:
        models.SampleprepParameterValue.objects.filter(dataset=dset).delete()
        cm.DatasetPipeline.objects.update_or_create(dataset=dset, defaults={
            'pipelineversion_id': data['pipeline']})
    else:
        cm.DatasetPipeline.objects.filter(dataset=dset).delete()
        if models.SampleprepParameterValue.objects.filter(dataset=dset).exists():
            update_admin_defined_params(dset, data, models.Labcategories.SAMPLEPREP)
        else:
            save_admin_defined_params(data, dset_id)
    set_component_state(dset_id, models.DatasetUIComponent.SAMPLEPREP, models.DCStates.OK)
    return JsonResponse({})


@login_required
def save_ms_sampleprep_tracking_step(request):
    '''Save pipeline step when user fills in date or "done today". If a dated step is done which
    is later than the PREPSTARTED step, this will set to done the not-dated steps before that 
    as well.  '''
    data = json.loads(request.body.decode('utf-8'))
    try:
        stagename, step_id, dspipe_id = data['stagename'], data['pipelinestep'], data['pipeline_id']
        timestamp = data['timestamp']
    except KeyError:
        return JsonResponse({'error': 'Bad request to save tracking step method, contact admin'},
                status=400)

    dspipe = cm.DatasetPipeline.objects.filter(pk=dspipe_id).values('dataset_id').get()
    user_denied, status = check_save_permission(dspipe['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    
    if stagename and hasattr(cm.TrackingStages, stagename):
        stage = cm.TrackingStages[stagename]
        timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d')
        if cm.DatasetPrepTracking.objects.filter(dspipe_id=dspipe_id,
                stage__lt=stage).count() < stage - 1:
            # If stage = 3, there should be 2 stages already in DB
            return JsonResponse({'error': 'Cannot update that stage without first updating previous'
                ' dated stages'}, status=403)
        cm.DatasetPrepTracking.objects.update_or_create(dspipe_id=dspipe_id, stage=stage,
                defaults={'timestamp': timestamp_dt})
        if stage > cm.TrackingStages.PREPSTARTED:
            preptracks = []
            for prepstage in cm.PipelineStep.objects.filter(
                    pipelineversion__datasetpipeline__pk=dspipe_id):
                preptracks.append(cm.DatasetPrepTrackingNodate(dspipe_id=dspipe_id, finished=True,
                    stage=prepstage))
            cm.DatasetPrepTrackingNodate.objects.bulk_create(preptracks, update_conflicts=True,
                    update_fields=['finished'], unique_fields=['dspipe', 'stage'])
    elif step_id:
        pre_stages = [cm.TrackingStages.SAMPLESREADY, cm.TrackingStages.PREPSTARTED]
        if cm.DatasetPrepTracking.objects.filter(dspipe_id=dspipe_id,
                stage__in=pre_stages).count() < len(pre_stages):
            return JsonResponse({'error': 'Cannot update that stage without first updating previous'
                ' dated stages'}, status=403)
        cm.DatasetPrepTrackingNodate.objects.update_or_create(dspipe_id=dspipe_id, stage_id=step_id,
                defaults={'finished': True})
    else:
        return JsonResponse({'error': 'No proper date could be saved'}, status=400)

    updated = {'tracked': [(cm.TrackingStages(x.stage).name, datetime.strftime(x.timestamp, '%Y-%m-%d'))
        for x in cm.DatasetPrepTracking.objects.filter(dspipe_id=dspipe_id)],
        'nodate': [(x.stage_id, x.finished) for x in cm.DatasetPrepTrackingNodate.objects.filter(dspipe_id=dspipe_id)]
            }
    return JsonResponse(updated)


@login_required
def save_ms_acquisition(request):
    data = json.loads(request.body.decode('utf-8'))
    user_denied, status = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    dset_id = data['dataset_id']
    dset = models.Dataset.objects.filter(pk=data['dataset_id']).get()
    if hasattr(dset, 'operatordataset'):
        return update_msacq(dset, data)
    models.AcquisistionModeDataset.objects.create(dataset=dset, acqmode=data['acqmode'])
    models.OperatorDataset.objects.create(dataset=dset, operator_id=data['operator_id'])
    save_admin_defined_params(data, dset_id)
    set_component_state(dset_id, models.DatasetUIComponent.ACQUISITION, models.DCStates.OK)
    return JsonResponse({})


def update_labelcheck(data, qtype):
    dset_id = data['dataset_id']
    if data['quanttype'] != qtype.quanttype_id:
        qtype.quanttype_id = data['quanttype']
        qtype.save()
    # Add any possible new channels 
    oldqfcs = {x.dsrawfile_id: x for x in models.QuantFileChannel.objects.filter(dsrawfile__dataset_id=dset_id)}
    for fn in data['filenames']:
        sam = data['samples'][str(fn['associd'])]
        try:
            exis_q = oldqfcs.pop(fn['associd'])
        except KeyError:
            models.QuantFileChannel.objects.create(dsrawfile_id=fn['associd'], channel_id=sam['channel'])
        else:
            if sam['channel'] != exis_q.channel_id:
                exis_q.channel_id = sam['channel']
                exis_q.save()
    # delete non-existing qcfs (files have been popped)
    [qcfs.delete() for qcfs in oldqfcs.values()]


@login_required
def save_labelcheck(request):
    data = json.loads(request.body.decode('utf-8'))
    user_denied, status = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    dset_id = data['dataset_id']
    try:
        qtype = models.QuantDataset.objects.select_related(
            'quanttype').get(dataset_id=dset_id)
    except models.QuantDataset.DoesNotExist:
        # new data, insert, not updating
        models.QuantDataset.objects.create(dataset_id=dset_id, quanttype_id=data['quanttype'])
        if not data['pooled']:
            models.QuantFileChannel.objects.bulk_create([
                models.QuantFileChannel(dsrawfile_id=fid,
                    channel_id=sam['channel']) for fid, sam in data['samples'].items()])
    else:
        if data['pooled'] and data['quanttype'] != qtype.quanttype_id:
            qtype.quanttype_id = data['quanttype']
            qtype.save()
        elif not data['pooled']:
            update_labelcheck(data, qtype)
    if data['pooled']:
        # TODO maybe merge these two UI components in frontend
        uicomp = models.DatasetUIComponent.POOLEDLCSAMPLES 
    else:
        uicomp = models.DatasetUIComponent.LCSAMPLES
    set_component_state(dset_id, uicomp, models.DCStates.OK)
    return JsonResponse({})


@login_required
@require_POST
def save_samples(request):
    '''Saving sample sheet. Start with validating the input:
    Each file/channel should have a sample and organism, and there needs to be feedback
    in case there is an alternative projectsample already in use
    Then either save or update
    '''
    data = json.loads(request.body.decode('utf-8'))
    user_denied, status = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return JsonResponse({'error': user_denied}, status=status)
    dset_id = data['dataset_id']

    # Gather all data from the input about samples/channels and map with ch/file keys
    if data['multiplex']:
        fid_or_chid_samples = [(ch['id'], ch) for chix, ch in enumerate(data['multiplex']['chans'])]
    else:
        fid_or_chid_samples = data['samples'].items()

    # All sampletypes and species for addressing 
    stype_ids = {chfid: {x['id'] for x in sample['sampletypes']} for chfid, sample in fid_or_chid_samples}
    spec_ids = {chfid: {x['id'] for x in sample['species']} for chfid, sample in fid_or_chid_samples}
    stype_dups = {}
    for _, sample in fid_or_chid_samples:
        stypes_passed = '_'.join(sorted([str(x['id']) for x in sample['sampletypes']]))
        try:
            stype_dups[sample['samplename']].add(stypes_passed)
        except KeyError:
            stype_dups[sample['samplename']] = set([stypes_passed])
    stype_dups = [k for k,v in stype_dups.items() if len(v) > 1]
    if stype_dups:
        return JsonResponse({'error': f'Sampletypes need to be identical for identical sample IDs, check {", ".join(stype_dups)}'}, status=400)

    # Check if all channels/samples have values for sampletype and species
    dset = models.Dataset.objects.select_related('runname__experiment__project').get(pk=dset_id)
    if data['multiplex']:
        dsr_chids = {x['pk'] for x in models.QuantTypeChannel.objects.filter(quanttype_id=data['qtype']).values('pk')}
    else:
        dsr_chids = {str(x['pk']) for x in models.DatasetRawFile.objects.filter(dataset=dset).values('pk')}
    samples_complete = all(stype_ids[x] and spec_ids[x] for x in dsr_chids
            if x in stype_ids and x in spec_ids)
    if dsr_chids.symmetric_difference(stype_ids) or dsr_chids.symmetric_difference(spec_ids) or not samples_complete:
        return JsonResponse({'error': 'Samples and species need to be specified for all files or channels'}, status=400)
    # Sample name check if they already exist as projectsample in system, then reply with
    # which of them exist and which sampletype/species they have
    projsamples, allspecies = {}, {}
    allsampletypes = {x.pk: x.name for x in models.SampleMaterialType.objects.all()}
    for fidix, sample in fid_or_chid_samples:
        if sample['model']:
            # Any sample which has a model (sample pk) is not checked for existing, 
            # since it represents an existing sample, or overwrites an existing sample
            continue
        elif psam := models.ProjectSample.objects.filter(project=dset.runname.experiment.project,
                sample=sample['samplename']):
            # Sample ID is found in DB already, shall we update it or rather
            # give user a message that it cant update since it's in multiple datasets
            st_error, sp_error = [], []
            psam = psam.get()
            lastrun = False
            if check_lr := psam.datasetsample_set.exclude(dataset=dset):
                # proj sample w same name found in another dataset
                lastrun = check_lr.select_related('dataset__runname__experiment').last().dataset.runname
                duprun_txt = f'{lastrun.experiment.name} - { lastrun.name}'
            elif lastrun := psam.datasetsample_set.select_related('dataset__runname__experiment').filter(
                    dataset=dset):
                # proj sample w name already in this dataset (no ID passed), but not in any other, so 
                # eg new species/sampletype, need to update those
                continue
            elif not psam.datasetsample_set.count():
                # Sample is up for grabs, already registered but not in dataset
                duprun_txt = 'not used in dataset, only registered'
            else:
                duprun_txt = 'Something went wrong, contact admin'
                print('Something went wrong here validating project samples')
            for psam_st in psam.samplematerial_set.all():
                if psam_st.sampletype_id not in stype_ids[fidix]:
                    st_error.append({'id': psam_st.sampletype_id, 'name': psam_st.sampletype.name, 'add': True, 'remove': False})
                else:
                    stype_ids[fidix].remove(psam_st.sampletype_id)
            for psam_sp in psam.samplespecies_set.all():
                if psam_sp.species_id not in spec_ids[fidix]:
                    sp_error.append({'id': psam_sp.species_id, 'name': psam_sp.species.popname, 'linnean': psam_sp.species.linnean, 'add': True, 'remove': False})
                else:
                    spec_ids[fidix].remove(psam_sp.species_id)
            for rm_id in stype_ids[fidix]:
                st_error.append({'id': rm_id, 'name': allsampletypes[rm_id], 'add': False, 'remove': True})
            for rm_id in spec_ids[fidix]:
                if rm_id not in allspecies:
                    rm_species = models.Species.objects.get(pk=rm_id)
                    allspecies[rm_id] = (rm_species.popname, rm_species.linnean)
                rm_name = allspecies[rm_id]
                sp_error.append({'id': rm_id, 'name': rm_name[0], 'linnean': rm_name[1], 'add': False, 'remove': True})
            projsamples[fidix] = {'id': psam.pk, 'duprun_example': duprun_txt,
                'sampletypes_error': st_error, 'species_error': sp_error}
    if projsamples:
        return JsonResponse({'error': 'Project samples exist in database, please validate the '
            'sample IDs', 'sample_dups': projsamples}, status=400)

    # All is ready for saving or updating
    if is_update := models.DatasetComponentState.objects.filter(dataset=dset,
            dtcomp=models.DatasetUIComponent.SAMPLES, state=models.DCStates.OK).count():
        # existing component state OK -> update: if quanttype changes - delete the old quanttype
        qds = models.QuantDataset.objects.select_related('quanttype').get(dataset=dset)
        updated_qtype = False
        old_qtype = qds.quanttype.pk
        if data['qtype'] != old_qtype:
            qds.quanttype_id = data['qtype']
            qds.save()
            updated_qtype = True
        if old_qtype == 'labelfree' and updated_qtype:
            # switch to multiplex from labelfree, delete LF
            models.QuantSampleFile.objects.filter(rawfile__dataset=dset).delete()
        elif updated_qtype and not data['multiplex']:
            # switch from old multiplex type, delete old plex
            models.QuantChannelSample.objects.filter(dataset=dset).delete()

    _cr, qds = models.QuantDataset.objects.update_or_create(dataset=dset,
            defaults={'quanttype_id': data['qtype']})
    dset_psams = set()
    for fidix, passedsample in fid_or_chid_samples:
        psampk, sname = passedsample['model'], passedsample['samplename']
        if not psampk:
            # get or create to get same sample (if dset has dup names for file or channel: 
            # use same sample)
            psam, _cr = models.ProjectSample.objects.get_or_create(
                    project=dset.runname.experiment.project, sample=sname)
            psampk = psam.pk
            models.DatasetSample.objects.get_or_create(dataset=dset, projsample=psam)
            if not _cr and not psampk in dset_psams:
                # If projsample already exist, it may need updating the material/type,
                # so delete existing bindings
                models.SampleMaterial.objects.filter(sample=psam).delete()
                models.SampleSpecies.objects.filter(sample=psam).delete()
            if not psampk in dset_psams:
                # only the first of duplicate sample within a dset will need to create new
                # material/type
                models.SampleMaterial.objects.bulk_create([models.SampleMaterial(sample=psam,
                    sampletype_id=x['id']) for x in passedsample['sampletypes']])
                models.SampleSpecies.objects.bulk_create([models.SampleSpecies(sample=psam,
                    species_id=x['id']) for x in passedsample['species']])
            dset_psams.add(psampk)

        elif psampk and passedsample['overwrite']:
            # Grab projsample by PK and delete bindings to material/species
            psam = models.ProjectSample.objects.get(pk=psampk)
            models.DatasetSample.objects.get_or_create(dataset=dset, projsample=psam)
            models.SampleMaterial.objects.filter(sample=psam).delete()
            models.SampleSpecies.objects.filter(sample=psam).delete()
            models.SampleMaterial.objects.bulk_create([models.SampleMaterial(sample=psam,
                sampletype_id=x['id']) for x in passedsample['sampletypes']])
            models.SampleSpecies.objects.bulk_create([models.SampleSpecies(sample=psam,
                species_id=x['id']) for x in passedsample['species']])
            dset_psams.add(psampk)

        # Even though we delete the QCS/QSF rows above in case of quanttype switch, we still
        # need to update_or_create in case of not switching but using different project sample
        if data['multiplex']:
            models.QuantChannelSample.objects.update_or_create(channel_id=fidix, dataset=dset,
                defaults={'projsample_id': psampk})
        else:
            models.QuantSampleFile.objects.update_or_create(rawfile_id=fidix,
                defaults={'projsample_id': psampk})
    if is_update:
        # Remove any project samples that are now orphaned:
        # TODO if we have dedicated sample sheets, we should probably NOT do this
        # If we dont, we need to manually delete DatasetSample since that cascades here
        models.ProjectSample.objects.filter(project__experiment__runname__dataset=dset,
                quantchannelsample__isnull=True,
                quantsamplefile__isnull=True,
                ).delete()
    set_component_state(dset_id, models.DatasetUIComponent.SAMPLES, models.DCStates.OK)
    return JsonResponse({})


def set_component_state(dset_id, comp, state):
    '''If you want to use this in a job, be careful that it will not roll back
    since the job runner doesnt have atomic transactions'''
    updated = models.DatasetComponentState.objects.filter(
            dataset_id=dset_id, dtcomp__component=comp).update(state=state)
    if updated == 0:
        raise RuntimeError(f'Could not find datasetcomponentstate row of type {comp} for dset {dset_id}')


def update_admin_defined_params(dset, data, category):
    fieldparams = dset.fieldparametervalue_set.filter(param__category=category)
    selectparams = dset.sampleprepparametervalue_set.select_related('value')
    checkboxparamvals = dset.checkboxparametervalue_set.filter(
        value__param__category=category).select_related('value')
    selectparams = {p.value.param_id: p for p in selectparams}
    fieldparams = {p.param_id: p for p in fieldparams}
    checkboxparams = {}
    for p in checkboxparamvals:
        try:
            checkboxparams[p.value.param_id][p.value_id] = p
        except KeyError:
            checkboxparams[p.value.param_id] = {p.value_id: p}
    new_selects, new_fields = [], []
    for param in data['params'].values():
        value = param['model']
        pid = param['param_id']
        if param['inputtype'] == 'select':
            if not value:
                models.SampleprepParameterValue.objects.filter(dataset_id=data['dataset_id'],
                        value__param_id=pid).delete()
            else:
                models.SampleprepParameterValue.objects.update_or_create(dataset_id=data['dataset_id'],
                        value__param_id=pid, defaults={'value_id': value})
        elif param['inputtype'] == 'checkbox':
            value = [box['value'] for box in param['fields'] if box['checked']]
            if pid in checkboxparams:
                oldvals = {val_id: pval for val_id, pval in checkboxparams[pid].items()}
                models.CheckboxParameterValue.objects.bulk_create([
                    models.CheckboxParameterValue(dataset_id=data['dataset_id'],
                                                  value_id=newval)
                    for newval in set(value).difference(oldvals)])
                for removeval in set(oldvals.keys()).difference(value):
                    oldvals[removeval].delete()
            elif pid not in checkboxparams:
                models.CheckboxParameterValue.objects.bulk_create([
                    models.CheckboxParameterValue(
                        dataset_id=data['dataset_id'], value_id=val)
                    for val in value])
        else:
            if pid in fieldparams and value != fieldparams[pid].value:
                fieldparams[pid].value = value
                fieldparams[pid].save()
            elif pid not in fieldparams:
                models.FieldParameterValue.objects.create(
                    dataset_id=data['dataset_id'], param_id=pid, value=value)
    # FIXME delete old ones?


def save_admin_defined_params(data, dset_id):
    # FIXME wtf is this also for ms acq?
    selects, checkboxes, fields = [], [], []
    for param in data['params'].values():
        if param['inputtype'] == 'select':
            if value := param['model']:
                selects.append(models.SampleprepParameterValue(dataset_id=dset_id, value_id=value))
        elif param['inputtype'] == 'checkbox':
            value = [box['value'] for box in param['fields'] if box['checked']]
            checkboxes.extend([models.CheckboxParameterValue(dataset_id=dset_id,
                                                            value_id=val) for val in value])
        else:
            value = param['model']
            fields.append(models.FieldParameterValue(dataset_id=dset_id,
                                                     param_id=param['param_id'],
                                                     value=value))
    models.SampleprepParameterValue.objects.bulk_create(selects)
    models.CheckboxParameterValue.objects.bulk_create(checkboxes)
    models.FieldParameterValue.objects.bulk_create(fields)
