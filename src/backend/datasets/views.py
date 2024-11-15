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
from django.db.models import Q, Count
from django.utils import timezone

from kantele import settings
from datasets import models
from rawstatus import models as filemodels
from jobs.jobutil import create_job, check_job_error, create_job_without_check, jobmap
from jobs import models as jm
from jobs import jobs as jj


INTERNAL_PI_PK = 1


@login_required
def new_dataset(request):
    """Returns dataset view with JS app"""
    context = {'dataset_id': 'false', 'newdataset': True, 'is_owner': 'true'}
    return render(request, 'datasets/dataset.html', context)



@login_required
def show_dataset(request, dataset_id):
    try:
        dset = models.Dataset.objects.filter(purged=False, pk=dataset_id).values('pk', 'deleted',
                'runname__experiment__project__projtype__ptype_id').get()
    except models.Dataset.DoesNotExist:
        return HttpResponseNotFound()
    dsown_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(dataset_id=dset['pk']).values('user_id')]
    is_owner = check_ownership(request.user, dset['runname__experiment__project__projtype__ptype_id'],
        dset['deleted'], dsown_ids)
    context = {'dataset_id': dataset_id, 'newdataset': False, 'is_owner': json.dumps(is_owner)}
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
def dataset_info(request, dataset_id=False):
    response_json = {'projdata': empty_dataset_json(), 'dsinfo': {}}
    if dataset_id:
        try:
            dset = models.Dataset.objects.select_related(
                'runname__experiment__project__projtype', 'datatype',
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
        response_json['dsinfo'] = {
                'dataset_id': dset.id,
                'experiment_id': dset.runname.experiment_id,
                'runname': dset.runname.name,
                'pi_id': project.pi_id,
                'pi_name': project.pi.name,
                'project_id': project.id,
                'project_name': project.name,
                'ptype_id': project.projtype.ptype_id,
                'datatype_id': dset.datatype_id,
                'storage_location': dset.storage_loc,
            }
        if hasattr(dset, 'prefractionationdataset'):
            response_json['dsinfo'].update(pf_dataset_info_json(
                dset.prefractionationdataset))
        if dset.runname.experiment.project.projtype.ptype_id != settings.LOCAL_PTYPE_ID:
            mail = models.ExternalDatasetContact.objects.get(dataset_id=dset.id)
            response_json['dsinfo'].update({'externalcontactmail': mail.email})
    return JsonResponse(response_json)


@login_required
def dataset_files(request, dataset_id=False):
    response_json = empty_files_json()
    if dataset_id:
        if not models.Dataset.objects.filter(purged=False, pk=dataset_id).count():
            return HttpResponseNotFound()
        ds_files = models.DatasetRawFile.objects.select_related(
                'rawfile__producer').filter(dataset_id=dataset_id).order_by('rawfile__date')
        if penfileq := jm.Job.objects.filter(funcname='move_files_storage', kwargs__dset_id=dataset_id,
                state=jj.Jobstates.HOLD):
            pending_ids = [x for y in penfileq for x in y.kwargs['rawfn_ids']]
            pending_files = [(x['pk'], x['name']) for x in
                    filemodels.RawFile.objects.filter(pk__in=pending_ids).values('name', 'pk')]
        else:
            pending_files = []
        response_json.update({
            'dsfn_order': [x.rawfile_id for x in ds_files],
            'pendingFiles': pending_files,
            'datasetFiles':
            {x.rawfile_id:
              {'id': x.rawfile_id, 'name': x.rawfile.name, 'associd': x.id,
               'instrument': x.rawfile.producer.name,
               'size': round(x.rawfile.size / (2**20), 1),
               'date': x.rawfile.date.timestamp() * 1000, 'checked': False} for x in ds_files}})
    return JsonResponse(response_json)


@login_required
def dataset_mssamples(request, dataset_id=False):
    response_json = empty_mssamples_json()
    if dataset_id:
        if not models.Dataset.objects.filter(purged=False, pk=dataset_id).count():
            return HttpResponseNotFound()
        try:
            response_json['dsinfo']['operator_id'] = models.OperatorDataset.objects.get(
                    dataset_id=dataset_id).operator_id

            response_json['dsinfo']['rp_length'] = models.ReversePhaseDataset.objects.get(
                    dataset_id=dataset_id).length
        except models.OperatorDataset.DoesNotExist:
            return JsonResponse(response_json)
        except models.ReversePhaseDataset.DoesNotExist:
            response_json['dsinfo']['dynamic_rp'] = True
            response_json['dsinfo']['rp_length'] = ''

        enzymes_used = {x.enzyme_id for x in 
                models.EnzymeDataset.objects.filter(dataset_id=dataset_id)}
        response_json['no_enzyme'] = True
        for enzyme in response_json['dsinfo']['enzymes']:
            if enzyme['id'] in enzymes_used:
                enzyme['checked'] = True
                response_json['no_enzyme'] = False

        get_admin_params_for_dset(response_json['dsinfo'], dataset_id, models.Labcategories.MSSAMPLES)
    return JsonResponse(response_json)


@login_required
def dataset_samples(request, dataset_id=False):
    response_json = empty_samples_json()
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
    for p in models.SelectParameterValue.objects.filter(
            dataset_id=dset_id, value__param__category=category):
        params_saved = True
        if p.value.param.active:
            fill_admin_selectparam(params, p.value, p.value.id)
        else:
            fill_admin_selectparam(oldparams, p.value, p.value.id)
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


def update_dataset(data):
    # FIXME it is v annoying to do all the hierarchical creations in multiple steps
    # in a single method, as this generates a lot of error checking, and things like
    # saves that are done when an error pops up later (storage loc is checked last, but a
    # proj is saved in the start. Easier to run
    # one method per update (exp, proj, dset) - but that will create unneccessary move
    # operations. But since it's a rename, it will be short, and usually not all of them
    # change at the same time.
    # Thus: its time to do "new dset", "new proj" in the excel table view!
    dset = models.Dataset.objects.filter(pk=data['dataset_id']).select_related(
        'runname__experiment', 'datatype').get()
    if 'newprojectname' in data:
        if is_invalid_proj_exp_runnames(data['newprojectname']):
            return JsonResponse({'error': f'New project name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
        project = newproject_save(data)
    else:
        project = models.Project.objects.get(pk=data['project_id'])
    if project.id != dset.runname.experiment.project_id:
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

    newexp = False
    if 'newexperimentname' in data:
        if is_invalid_proj_exp_runnames(data['newexperimentname']):
            return JsonResponse({'error': f'Experiment name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
        experiment = models.Experiment(name=data['newexperimentname'],
                                       project=project)
        dset.runname.experiment = experiment
        newexp = True
    else:
        experiment = models.Experiment.objects.get(pk=data['experiment_id'])
        experiment.project_id = project.id
        if data['experiment_id'] != dset.runname.experiment_id:
            # another experiment was selected
            newexp = True
            dset.runname.experiment = experiment
    experiment.save()
    if data['runname'] != dset.runname.name or newexp:
        if is_invalid_proj_exp_runnames(data['runname']):
            return JsonResponse({'error': f'Run name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
        # Save if new experiment AND/OR new name Runname coupled 1-1 to dataset
        dset.runname.name = data['runname']
        dset.runname.save()
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
            dtcs = models.DatasetComponentState(dataset=dset, dtcomp=dtc, state=state)
            dtcs.save()
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
    dtype = models.Datatype.objects.get(pk=data['datatype_id'])
    prefrac = models.Prefractionation.objects.get(pk=data['prefrac_id']) if data['prefrac_id'] else False
    hrrange_id = data['hiriefrange'] if 'hiriefrange' in data and data['hiriefrange'] else False
    new_storage_loc = set_storage_location(project, experiment, dset.runname,
                                           dtype, prefrac, hrrange_id)
    err_fpath, err_fname = os.path.split(new_storage_loc)
    prim_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    if new_storage_loc != dset.storage_loc and filemodels.StoredFile.objects.filter(
            servershare=prim_share, path=err_fpath, filename=err_fname).exists():
        return JsonResponse({'error': 'There is already a file with that exact path '
            f'{new_storage_loc}'}, status=403)
    elif (new_storage_loc != dset.storage_loc and 
            models.DatasetRawFile.objects.filter(dataset_id=dset.id).count()):
        # Update storage loc, first check if dataset needs moving
        if dset.storageshare != prim_share:
            if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                    settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
                return JsonResponse({'error': error}, status=403)
        job = create_job('rename_dset_storage_loc', dset_id=dset.id, dstpath=new_storage_loc)
        if job['error']: 
            return JsonResponse({'error': job['error']}, status=403)
    dset.save()
    if data['ptype_id'] != settings.LOCAL_PTYPE_ID:
        try:
            if dset.externaldatasetcontact.email != data['externalcontact']:
                dset.externaldatasetcontact.email = data['externalcontact']
                dset.externaldatasetcontact.save()
        except models.ExternalDatasetContact.DoesNotExist:
            dset_mail = models.ExternalDatasetContact(dataset=dset,
                    email=data['externalcontact'])
            dset_mail.save()
    return JsonResponse({'dataset_id': dset.id})


def newproject_save(data):
    if 'newpiname' in data:
        pi = models.PrincipalInvestigator(name=data['newpiname'])
        pi.save()
        project = models.Project(name=data['newprojectname'], pi=pi)
    else:
        project = models.Project(name=data['newprojectname'], pi_id=data['pi_id'])
    project.save()
    ptype = models.ProjType(project=project, ptype_id=data['ptype_id'])
    ptype.save()
    return project


def get_prefrac_ids():
    return (models.Prefractionation.objects.get(name__icontains='hirief').id,
            models.Prefractionation.objects.get(name__icontains='high_pH').id)
            

def get_quantprot_id():
    return models.Datatype.objects.get(name__icontains='quantitative').id


def rename_storage_loc_toplvl(newprojname, oldpath):
    """Put this method here, used in post-job view renamed_project.
    This is a good place since its functionality is tied to
    the set_storage_location method
    """
    # Using .split(pathsep) because os.path.split only removes leaf,
    # we need the first/root dir of the path
    return os.path.join(newprojname, *oldpath.split(os.path.sep)[1:])


def set_storage_location(project, exp, runname, dtype, prefrac, hiriefrange_id):
    """
    Governs rules for storage path naming of datasets.
    Project name is always top level, this is also used in 
    rename_storage_loc_toplvl
    """
    quantprot_id = get_quantprot_id()
    subdir = ''
    if dtype.pk != quantprot_id:
        subdir = dtype.name
    if prefrac and hiriefrange_id:
        subdir = os.path.join(subdir, models.HiriefRange.objects.get(
            pk=hiriefrange_id).get_path())
    elif prefrac:
        subdir = os.path.join(prefrac.name)
    subdir = re.sub('[^a-zA-Z0-9_\-\/\.]', '_', subdir)
    if len(subdir):
        subdir = '/{}'.format(subdir)
    if dtype.id in settings.LC_DTYPE_IDS:
        return os.path.join(project.name, exp.name, str(runname.id))
    else:
        return '{}/{}{}/{}'.format(project.name, exp.name, subdir, runname.name)


@login_required
def change_owners(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = models.Dataset.objects.filter(pk=data['dataset_id']).values('pk', 'deleted',
                'runname__experiment__project__projtype__ptype_id').get()
    except models.Dataset.DoesNotExist:
        print('change_owners could not find dataset with that ID {}'.format(data['dataset_id']))
        return JsonResponse({'error': 'Something went wrong trying to change ownership for that dataset'}, status=403)
    dsownq = models.DatasetOwner.objects.filter(dataset_id=dset['pk'])
    if not check_ownership(request.user, dset['runname__experiment__project__projtype__ptype_id'],
            dset['deleted'], [x['user_id'] for x in dsownq.values('user_id')]):
        return HttpResponseForbidden()
    is_already_ownerq = dsownq.filter(user_id=data['owner'])
    is_already_owner = is_already_ownerq.exists()
    if data['op'] == 'add' and not is_already_owner:
        newowner = models.DatasetOwner(dataset=dset, user_id=data['owner'])
        newowner.save()
        return JsonResponse({'result': 'ok'})
    elif data['op'] == 'del' and is_already_owner and dset.datasetowner_set.count() > 1:
        is_already_ownerq.delete()
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


def check_save_permission(dset_id, logged_in_user):
    '''Only datasets which are not deleted can be saved, and a check_ownership is done'''
    try:
        ds = models.Dataset.objects.filter(purged=False, deleted=False,
                pk=dset_id).values('runname__experiment__project__projtype__ptype_id', 'deleted').get()
    except models.Dataset.DoesNotExist:
        return HttpResponseNotFound()
    else:
        owner_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(dataset_id=dset_id).values('user_id')]
        if not check_ownership(logged_in_user, ds['runname__experiment__project__projtype__ptype_id'],
                ds['deleted'], owner_ids):
            return HttpResponseForbidden()
    return False


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
    dtype = models.Datatype.objects.get(pk=data['datatype_id'])
    prefrac = models.Prefractionation.objects.get(pk=data['prefrac_id']) if data['prefrac_id'] else False
    hrrange_id = data['hiriefrange'] if 'hiriefrange' in data and data['hiriefrange'] else False
    prim_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    storloc = set_storage_location(project, experiment, runname, dtype, prefrac, hrrange_id)
    err_fpath, err_fname = os.path.split(storloc)
    if filemodels.StoredFile.objects.filter(servershare=prim_share, path=err_fpath,
            filename=err_fname).exists():
        raise IntegrityError(f'There is already a file with that exact path {storloc}')
    dset = models.Dataset.objects.create(date=timezone.now(), runname_id=runname.id,
            storage_loc=storloc, storageshare=prim_share, datatype=dtype)
    dsowner = models.DatasetOwner(dataset=dset, user_id=user_id)
    dsowner.save()
    if data['prefrac_id']:
        save_dataset_prefrac(dset.id, data)
    if data['ptype_id'] != settings.LOCAL_PTYPE_ID:
        dset_mail = models.ExternalDatasetContact(dataset=dset,
                                                 email=data['externalcontact'])
        dset_mail.save()
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
    return dset


@login_required
def move_project_cold(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'item_id' not in data or not data['item_id']:
        return JsonResponse({'state': 'error', 'error': 'No project specified for retiring'}, status=400)
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
        archived = archive_dataset(dso.dataset)
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

    prim_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    for proj in projs[1:]:
        # Refresh oldexps with every merged project
        oldexps = {x.name: x for x in projs[0].experiment_set.all()}
        for dset in  models.Dataset.objects.filter(runname__experiment__project=proj).values(
                'pk', 'deleted'):
            dsown_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(
                dataset_id=dset['pk']).values('user_id')]
            if not check_ownership(request.user, proj.projtype.ptype_id, dset['deleted'], dsown_ids):
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
            # Update dset storage_loc field in DB
            for runname in runnames:
                dset = runname.dataset
                pfds = dset.prefractionationdataset if hasattr(dset, 'prefractionationdataset') else False
                prefrac = pfds.prefractionation if pfds else False
                hrrange_id = pfds.hiriefdataset.hirief_id if hasattr(pfds, 'hiriefdataset') else False
                new_storage_loc = set_storage_location(projs[0], exp, runname, dset.datatype, prefrac, hrrange_id)
                if dset.storageshare_id != prim_share.pk:
                    if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                            settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
                        return JsonResponse({'error': error}, status=403)
                job = create_job('rename_dset_storage_loc', dset_id=dset.id,
                        dstpath=new_storage_loc)
                if job['error']:
                    return JsonResponse({'error': job['error']}, status=403)
            # Also, should we possibly NOT chaneg anything here but only check pre the job, then merge after job complete?
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
    if data['newname'] == proj.name:
        return JsonResponse({'error': f'Cannot change name to existing name for project {proj.name}'}, status=403)
    elif is_invalid_proj_exp_runnames(data['newname']):
        return JsonResponse({'error': f'Project name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
    dsets = models.Dataset.objects.filter(runname__experiment__project=proj)
    storageshare_id = False
    for dset in  dsets.values('pk', 'deleted', 'storageshare_id', 'storageshare__name', 'runname__experiment__project_id'):
        if not storageshare_id:
            # Only first dataset
            storageshare_id = dset['storageshare_id']
        dsown_ids = [x['user_id'] for x in models.DatasetOwner.objects.filter(dataset_id=dset['pk']).values('user_id')]
        if not check_ownership(request.user, proj.projtype.ptype_id, dset['deleted'], dsown_ids):
            return JsonResponse({'error': f'You do not have the rights to change all datasets in this project'}, status=403)
    # queue jobs to rename project, update project name after that since it is needed in job for path
    prim_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    if dsets.exists() and storageshare_id != prim_share.pk:
        # Take leftover dset from loop
        if error := move_dset_project_servershare(dset['pk'], dset['storageshare__name'],
                settings.PRIMARY_STORAGESHARENAME, dset['runname__experiment__project_id']):
            return JsonResponse({'error': error}, status=403)
    job = create_job('rename_top_lvl_projectdir', newname=data['newname'], proj_id=data['projid'])
    if job['error']:
        return JsonResponse({'error': job['error']}, status=403)
    proj.name = data['newname']
    proj.save()
    return JsonResponse({})


@login_required
def move_project_active(request):
    data = json.loads(request.body.decode('utf-8'))
    if 'item_id' not in data or not data['item_id']:
        return JsonResponse({'state': 'error', 'error': 'No project specified for reactivating'}, status=400)
    projquery = models.Project.objects.filter(pk=data['item_id'], active=False)
    if not projquery:
        return JsonResponse({'state': 'error', 'error': 'Project is already active, or does not exist'}, status=403)
    # Reactivating a project is only allowed if user owns ALL datasets in project or is staff
    dsetowners = models.DatasetOwner.objects.filter(dataset__runname__experiment__project_id=data['item_id'], dataset__purged=False).select_related('dataset')
    if dsetowners.filter(user=request.user).count() != dsetowners.distinct('dataset').count() and not request.user.is_staff:
        return JsonResponse({'state': 'error', 'error': 'User has no permission to reactivate this project, does not own all datasets in project'}, status=403)
    # Reactivate all datasets
    result = {'errormsgs': []}
    for dso in dsetowners.distinct('dataset'):
        reactivated = reactivate_dataset(dso.dataset)
        if reactivated['state'] == 'error':
            result.update({'state': 'error', 'error': 'Not all project datasets could be reactivated.'})
            result['errormsgs'].append(reactivated['error'])
        else:
            # if ANY dataset gets reactivated, project is active
            projquery.update(active=True)
    if result['errormsgs']:
        result['error'] = '{} Errors: {}'.format(result['error'], '; '.join(result.pop('errormsgs')))
        return JsonResponse(result, status=500)
    else:
        return JsonResponse({})


@login_required
def purge_project(request):
    """Deletes project datasets (not analyses) from disk, only leaving backup copies."""
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
        return JsonResponse({})


def get_dset_storestate(dset_id, dsfiles=False):
    # Dsfiles is passed in home/views, but only to cache it?
    if not dsfiles:
        dsfiles = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=dset_id)
    dsfiles = dsfiles.exclude(mzmlfile__isnull=False)
    dsfc = dsfiles.count()
    if dsfc == 0:
        return 'empty'
    coldfiles = dsfiles.filter(pdcbackedupfile__isnull=False, pdcbackedupfile__deleted=False, pdcbackedupfile__success=True)
    if dsfiles.filter(checked=True, deleted=False).count() == dsfc == coldfiles.count():
        storestate = 'complete'
    elif dsfiles.filter(checked=True, deleted=False).count() == dsfc:
        storestate = 'active-only'
    elif coldfiles.count() == dsfc:
        storestate = 'cold'
    elif dsfiles.filter(purged=True).count() == dsfc and dsfiles.filter(pdcbackedupfile__deleted=True).count() == dsfiles.filter(pdcbackedupfile__isnull=False).count():
        # FIXME this is incorrect backup count?
        storestate = 'purged'
    elif dsfiles.filter(pdcbackedupfile__deleted=True).exists():
        storestate = 'broken'
    elif dsfiles.filter(checked=False).exists() or dsfiles.filter(servershare__name=settings.TMPSHARENAME).exists():
        storestate = 'new'
    else:
        storestate = 'unknown'
    return storestate


def archive_dataset(dset):
    # FIXME dataset reactivating and archiving reports error when ok and vv? I mean, if you click archive and reactivate quickly, you will get error (still in active storage), and also in this func, storestate is not updated at same time as DB (it is result of jobs)
    storestate = get_dset_storestate(dset.pk)
    prim_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    backing_up = False
    if storestate == 'purged':
        return {'state': 'error', 'error': 'Cannot archive dataset, already purged'}
    elif storestate == 'broken':
        return {'state': 'error', 'error': 'Cannot archive dataset, files missing on active storage'}
    elif storestate == 'new':
        return {'state': 'error', 'error': 'Cannot archive new dataset'}
    elif storestate == 'unknown':
        return {'state': 'error', 'error': 'Cannot archive dataset with unknown storage state'}
    elif storestate == 'active-only':
        if dset.storageshare_id != prim_share.pk:
            if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                    settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
                return {'state': 'error', 'error': error}
        backing_up = True
        create_job('backup_dataset', dset_id=dset.id)

    # If you reach this point, the dataset is either backed up or has a job queued for that, so queue delete job
    if storestate != 'empty' and (backing_up or dset.storageshare_id == prim_share.pk):
        create_job('delete_active_dataset', dset_id=dset.id)
        create_job('delete_empty_directory', sf_ids=[x.id for x in filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset)])
    elif storestate != 'empty' and dset.storageshare_id != prim_share.pk:
        # Mark files as purged since they are in secondary storage
        # This is very much temporary code and should be removed once we have consolidated all storage
        fakejob = jobmap['delete_active_dataset'](False)
        fakejob.getfiles_query(dset_id=dset.pk).update(deleted=True, purged=True)
    dset.deleted, dset.purged = True, False
    dset.save()
    return {'state': 'ok', 'error': 'Dataset queued for archival'}


def reactivate_dataset(dset):
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
    storestate = get_dset_storestate(dset.pk)
    if storestate == 'purged':
        return {'state': 'error', 'error': 'Cannot reactivate purged dataset'}
    elif storestate == 'broken':
        return {'state': 'error', 'error': 'Cannot reactivate dataset, files missing on active storage'}
    elif storestate == 'new':
        return {'state': 'error', 'error': 'Cannot reactivate new dataset'}
    elif storestate == 'unknown':
        return {'state': 'error', 'error': 'Cannot reactivate dataset with unknown storage state'}
    elif storestate in ['active-only', 'complete']:
        return {'state': 'error', 'error': 'Dataset already in active storage'}
    elif storestate == 'empty':
        dset.deleted, dset.purged, dset.runname.experiment.project.active = False, False, True
        dset.save()
        dset.runname.experiment.project.save()
        return {'state': 'ok', 'error': 'Empty dataset (contains no files) re-activated'}
    elif storestate == 'cold':
        dset.deleted, dset.purged, dset.runname.experiment.project.active = False, False, True
        dset.save()
        dset.runname.experiment.project.save()
        create_job('reactivate_dataset', dset_id=dset.id)
        return {'state': 'ok'}


@login_required
def move_dataset_cold(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = models.Dataset.objects.select_related('runname__experiment__project__projtype').get(pk=data['item_id'])
    except models.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist'}, status=403)
    dsownq = models.DatasetOwner.objects.filter(dataset_id=dset.pk)
    if not check_ownership(request.user, dset.runname.experiment.project.projtype.ptype_id,
            dset.deleted, [x['user_id'] for x in dsownq.values('user_id')]):
        return JsonResponse({'error': 'Cannot archive dataset, no permission for user'}, status=403)
    archived = archive_dataset(dset)
    if archived['state'] == 'error':
        return JsonResponse(archived, status=500)
    else:
        return JsonResponse(archived)


@login_required
def move_dataset_active(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = models.Dataset.objects.select_related('runname__experiment__project__projtype').get(pk=data['item_id'])
    except models.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist'}, status=403)
    # check_ownership without deleted demand:
    pt_id = dset.runname.experiment.project.projtype.ptype_id 
    if request.user.id in get_dataset_owners_ids(dset) or request.user.is_staff:
        pass
    elif pt_id == settings.LOCAL_PTYPE_ID:
        return JsonResponse({'error': 'Cannot reactivate dataset, no permission for user'}, status=403)
    else:
        try:
            models.UserPtype.objects.get(ptype_id=pt_id, user_id=request.user.id)
        except models.UserPtype.DoesNotExist:
            return JsonResponse({'error': 'Cannot reactivate dataset, no permission for user'}, status=403)
    reactivated_msg = reactivate_dataset(dset)
    if reactivated_msg['state'] == 'error':
        return JsonResponse(reactivated_msg, status=500)
    else:
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
    create_job('delete_active_dataset', dset_id=dset.id)
    create_job('delete_empty_directory', sf_ids=[x.id for x in filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset)])
    storestate = get_dset_storestate(dset.pk)
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
        dset = models.Dataset.objects.get(pk=data['item_id'])
    except models.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist'}, status=403)
    purgemsg = delete_dataset_from_cold(dset)
    if purgemsg['state'] == 'error':
        return JsonResponse(purgemsg, status=500)
    else:
        return JsonResponse(purgemsg)


def is_invalid_proj_exp_runnames(name):
    """Validates any project/experiment/run names for datasets"""
    return re.search(f'[^{settings.ALLOWED_PROJEXPRUN_CHARS}]', name)


@login_required
def save_dataset(request):
    data = json.loads(request.body.decode('utf-8'))
    if data['dataset_id']:
        user_denied = check_save_permission(data['dataset_id'], request.user)
        if user_denied:
            return user_denied
        print('Updating')
        return update_dataset(data)
    else:
        if is_invalid_proj_exp_runnames(data['runname']):
            return JsonResponse({'error': f'Run name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
        if 'newprojectname' in data:
            if is_invalid_proj_exp_runnames(data['newprojectname']):
                return JsonResponse({'error': f'New project name cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
            project = newproject_save(data)
        else:
            try:
                project = models.Project.objects.get(pk=data['project_id'], active=True)
            except models.Project.DoesNotExist:
                print('Project to save to is not active')
                return HttpResponseForbidden()
        if data['datatype_id'] in settings.LC_DTYPE_IDS:
            try:
                experiment = models.Experiment.objects.get(project=project, name=settings.LCEXPNAME)
            except models.Experiment.DoesNotExist:
                experiment = models.Experiment(name=settings.LCEXPNAME, project=project)
                experiment.save()
            runname = models.RunName(name=data['runname'], experiment=experiment)
        else:
            if 'newexperimentname' in data:
                if is_invalid_proj_exp_runnames(data['newexperimentname']):
                    return JsonResponse({
                        'error': f'Experiment name cannot contain characters '
                        'except {settings.ALLOWED_PROJEXPRUN_CHARS}'}, status=403)
                experiment = models.Experiment(name=data['newexperimentname'],
                                               project_id=project.id)
                experiment.save()

            elif 'experiment_id' in data:
                experiment = models.Experiment.objects.get(pk=data['experiment_id'])
            runname = models.RunName(name=data['runname'], experiment=experiment)
        runname.save()
        try:
            dset = save_new_dataset(data, project, experiment, runname, request.user.id)
        except IntegrityError:
            return JsonResponse({'state': 'error', 'error': 'Cannot save dataset, storage location '
                'not unique, there is either a file or an existing dataset on that location.'},
                status=403)
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
    proj = models.Project.objects.select_related('projtype').get(pk=project_id)
    return JsonResponse({
        'id': project_id, 'pi_id': proj.pi_id, 'ptype_id': proj.projtype.ptype_id,
        'projsamples': {x.id: {'name': x.sample, 'id': x.id} for x in models.ProjectSample.objects.filter(project_id=project_id)},
        'experiments': [{'id': x.id, 'name': x.name} for x in 
        models.Experiment.objects.filter(project_id=project_id)]})


def empty_dataset_json():
    edpr = {'projects': {x.id:
        {'name': x.name, 'id': x.id, 'ptype_id': x.projtype.ptype_id,
            'select': False, 'pi_id': x.pi_id} 
        for x in models.Project.objects.select_related('projtype').filter(active=True)},
            'ptypes': [{'name': x.name, 'id': x.id} for x in models.ProjectTypeName.objects.all()],
            'external_pis': {x.id: {'name': x.name, 'id': x.id} for x in
                             models.PrincipalInvestigator.objects.all()},
            'internal_pi_id': INTERNAL_PI_PK,
            'local_ptype_id': settings.LOCAL_PTYPE_ID,
            'datasettypes': [{'name': x.name, 'id': x.id} for x in
                             models.Datatype.objects.filter(public=True)],
            'prefracs': [{'id': x.id, 'name': x.name}
                          for x in models.Prefractionation.objects.all()],
            'hirief_ranges': [{'name': str(x), 'id': x.id}
                              for x in models.HiriefRange.objects.all()]}
    return edpr


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


def empty_samples_json():
    return {'species': [], 'quants': get_empty_isoquant(), 'labeled': False,
            'lf_qtid': models.QuantType.objects.get(name='labelfree').pk,
            'allsampletypes': {x.pk: {'id': x.pk, 'name': x.name} for x in models.SampleMaterialType.objects.all()},
            'allspecies': {str(x['species']): {'id': x['species'], 'linnean': x['species__linnean'],
                'name': x['species__popname'], 'total': x['total']} 
                for x in models.SampleSpecies.objects.all().values('species', 'species__linnean', 'species__popname'
                    ).annotate(total=Count('species__linnean')).order_by('-total')[:5]},
            }


def fill_admin_selectparam(params, p, value=False):
    """Fills params dict with select parameters passed, in proper JSON format
    This takes care of both empty params (for new dataset), filled parameters,
    and old parameters"""
    p_id = f'select_{p.param.id}'
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
    params = {}
    for p in models.SelectParameterOption.objects.select_related(
            'param').filter(param__category=category):
        if p.param.active:
            fill_admin_selectparam(params, p)
    for p in models.FieldParameter.objects.select_related(
            'paramtype').filter(category=category):
        if p.active:
            fill_admin_fieldparam(params, p)
    for p in models.CheckboxParameterOption.objects.select_related(
            'param').filter(param__category=category):
        if p.param.active:
            fill_admin_checkboxparam(params, p)
    return params


def empty_mssamples_json():
    return {'dsinfo': {'params': get_dynamic_emptyparams(models.Labcategories.MSSAMPLES),
        'enzymes': [{'id': x.id, 'name': x.name, 'checked': False}
            for x in models.Enzyme.objects.all()]},
        'acqdata': {'operators': [{'id': x.id, 'name': '{} {}'.format(
                x.user.first_name, x.user.last_name)}
                for x in models.Operator.objects.select_related('user').all()]},
            }


@login_required
@require_GET
def find_files(request):
    searchterms = [x for x in request.GET['q'].split(',') if x != '']
    query = Q(name__icontains=searchterms[0])
    query |= Q(producer__name__icontains=searchterms[0])
    for term in searchterms[1:]:
        subquery = Q(name__icontains=term)
        subquery |= Q(producer__name__icontains=term)
        query &= subquery
    newfiles = filemodels.RawFile.objects.filter(query).filter(claimed=False,
            storedfile__checked=True)
    return JsonResponse({
        'newfn_order': [x.id for x in newfiles.order_by('-date')],
        'newFiles': {x.id:
                         {'id': x.id, 'name': x.name, 
                          'size': round(x.size / (2**20), 1),
                          'date': x.date.timestamp() * 1000,
                          'instrument': x.producer.name, 'checked': False}
                         for x in newfiles}})


def empty_files_json():
    '''Shows to user all uploaded non-claimed and checked files in a 200 day window'''
    newfiles = filemodels.RawFile.objects.select_related('producer').filter(
            claimed=False, storedfile__checked=True, date__gt=datetime.now() - timedelta(200))
    return {'instruments': [x.name for x in filemodels.Producer.objects.all()], 'datasetFiles': [],
            'newfn_order': [x.id for x in newfiles.order_by('-date')],
            'newFiles': {x.id: {'id': x.id, 'name': x.name, 'size': round(x.size / (2**20), 1), 
                'date': x.date.timestamp() * 1000, 'instrument': x.producer.name, 'checked': False}
                for x in newfiles}}


def move_dset_project_servershare(dset_id, storagesharename, dstsharename, projid):
    '''Takes a dataset and moves its entire project to a new
    servershare'''
    kwargs = []
    kw = {'dset_id': dset_id, 'srcsharename': storagesharename, 'dstsharename': dstsharename}
    if not jm.Job.objects.filter(funcname='move_dset_servershare',
            kwargs__dset_id=dset_id, kwargs__srcsharename=storagesharename,
            kwargs__dstsharename=dstsharename).exclude(state__in=jj.JOBSTATES_DONE).exists():
        if error := check_job_error('move_dset_servershare', **kw):
            return error
        kwargs = [kw]
    for other_ds in models.Dataset.objects.filter(deleted=False, purged=False,
            runname__experiment__project_id=projid).exclude(pk=dset_id):
        kw = {'dset_id': other_ds.pk, 'srcsharename': other_ds.storageshare.name,
                'dstsharename': dstsharename}
        jobs_in_progress = jm.Job.objects.filter(funcname='move_dset_servershare',
                kwargs__dset_id=kw['dset_id'], kwargs__srcsharename=kw['srcsharename'],
                kwargs__dstsharename=dstsharename).exclude(state__in=jj.JOBSTATES_DONE)
        if not jobs_in_progress.exists():
            if error := check_job_error('move_dset_servershare', **kw):
                return error
            kwargs.append(kw)
    [create_job_without_check('move_dset_servershare', **kw) for kw in kwargs]
    return False


def save_or_update_files(data):
    '''Called from views in jobs, rawstatus as well, so broke out from request
    handling view'''
    dset_id = data['dataset_id']
    added_fnids = [x['id'] for x in data['added_files'].values()]
    removed_ids = [int(x['id']) for x in data['removed_files'].values()]
    dset = models.Dataset.objects.select_related('storageshare').get(pk=dset_id)
    tmpshare = filemodels.ServerShare.objects.get(name=settings.TMPSHARENAME)
    dsrawfn_ids = filemodels.RawFile.objects.filter(datasetrawfile__dataset=dset)
    switch_fileserver = dset.storageshare.server != tmpshare.server
    mvjobs = []
    # First error check and collect jobs:
    if added_fnids:
        if not models.RawFile.objects.filter(pk__in=added_fnids,
                storedfile__checked=True).exists():
            return {'error': 'Some files cannot be saved to dataset since they '
                    'are not confirmed to be stored yet'}, 403
        mvjobs.append(('move_files_storage', {'dset_id': dset_id, 'rawfn_ids': added_fnids}))
    if removed_ids:
        mvjobs.append(('move_stored_files_tmp', {'dset_id': dset_id, 'fn_ids': removed_ids}))

    # Job error checking for moving the files (files already in tmp or in dset dir)
    for mvjob in mvjobs:
        if error := check_job_error(mvjob[0], **mvjob[1]):
            return {'error': error}, 403
    # Now move servershare if needed (dset is old and not on primary server)
    # All tmp files will be on primary server, so if we move server first, there is no
    # issue with them not being ok anymore
    if switch_fileserver:
        if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
            return {'error': error}, 403

    # Errors checked, now store DB records and queue move jobs
    if added_fnids:
        models.DatasetRawFile.objects.bulk_create([
            models.DatasetRawFile(dataset_id=dset_id, rawfile_id=fnid)
            for fnid in added_fnids])
        filemodels.RawFile.objects.filter(
            pk__in=added_fnids).update(claimed=True)
    if removed_ids:
        models.DatasetRawFile.objects.filter(
            dataset_id=dset_id, rawfile_id__in=removed_ids).delete()
        filemodels.RawFile.objects.filter(pk__in=removed_ids).update(
            claimed=False)
    [create_job_without_check(name, **kw) for name,kw in mvjobs]

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
    return {'error': False}, 200


@login_required
def accept_or_reject_dset_preassoc_files(request):
    data = json.loads(request.body.decode('utf-8'))
    user_denied = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return user_denied
    # First remove jobs on rejected files and un-claim rawfiles
    if len(data['rejected_files']):
        deleted = jm.Job.objects.filter(funcname='move_files_storage', state=jj.Jobstates.HOLD,
                kwargs__dset_id=data['dataset_id'],
                kwargs__rawfn_ids__in=[[x] for x in data['rejected_files']]).delete()
        if deleted:
            filemodels.RawFile.objects.filter(pk__in=data['rejected_files']).update(claimed=False)
    # Now start jobs for accepted files
    if len(data['accepted_files']):
        jm.Job.objects.filter(funcname='move_files_storage', state=jj.Jobstates.HOLD,
                kwargs__dset_id=data['dataset_id'],
                kwargs__rawfn_ids__in=[[x] for x in data['accepted_files']]).update(
                        state=jj.Jobstates.PENDING)
        models.DatasetRawFile.objects.bulk_create([
            models.DatasetRawFile(dataset_id=data['dataset_id'], rawfile_id=fnid)
            for fnid in data['accepted_files']])
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
    return JsonResponse({'error': False})
    # FIXME switch fileserver should happen - in that case on the classify start!


@login_required
def save_files(request):
    """Updates and saves files"""
    data = json.loads(request.body.decode('utf-8'))
    user_denied = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return user_denied
    err_result, status = save_or_update_files(data)
    return JsonResponse(err_result, status=status)


def update_mssamples(dset, data):
    models.EnzymeDataset.objects.filter(dataset=dset).delete()
    models.EnzymeDataset.objects.bulk_create([models.EnzymeDataset(dataset=dset, enzyme_id=enz['id'])
        for enz in data['enzymes'] if enz['checked']])
    if data['operator_id'] != dset.operatordataset.operator_id:
        dset.operatordataset.operator_id = data['operator_id']
        dset.operatordataset.save()
    if not hasattr(dset, 'reversephasedataset'):
        if data['rp_length']:
            models.ReversePhaseDataset.objects.create(dataset_id=dset.id,
                                                      length=data['rp_length'])
    elif not data['rp_length']:
            dset.reversephasedataset.delete()
#            models.ReversePhaseDataset.objects.filter(
#                dataset_id=dset.id).delete()
    elif data['rp_length'] != dset.reversephasedataset.length:
        dset.reversephasedataset.length = data['rp_length']
        dset.reversephasedataset.save()
    update_admin_defined_params(dset, data, models.Labcategories.MSSAMPLES)
    return JsonResponse({})


@login_required
def save_mssamples(request):
    data = json.loads(request.body.decode('utf-8'))
    user_denied = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return user_denied
    dset_id = data['dataset_id']
    dset = models.Dataset.objects.filter(pk=data['dataset_id']).select_related(
        'operatordataset', 'reversephasedataset').get()
    if hasattr(dset, 'operatordataset'):
        return update_mssamples(dset, data)
    if data['rp_length']:
        models.ReversePhaseDataset.objects.create(dataset_id=dset_id,
                                                  length=data['rp_length'])
    models.OperatorDataset.objects.create(dataset_id=dset_id,
                                          operator_id=data['operator_id'])
    models.EnzymeDataset.objects.bulk_create([models.EnzymeDataset(dataset_id=dset_id, enzyme_id=enz['id'])
        for enz in data['enzymes'] if enz['checked']])

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
            models.QuantFileChannel.objects.create(dsrawfile_id=fn, channel=sam['channel'])
        else:
            if sam['channel'] != exis_q.channel_id:
                exis_q.channel_id = sam['channel']
                exis_q.save()
    # delete non-existing qcfs (files have been popped)
    [qcfs.delete() for qcfs in oldqfcs.values()]


@login_required
def save_labelcheck(request):
    data = json.loads(request.body.decode('utf-8'))
    user_denied = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return user_denied
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
    user_denied = check_save_permission(data['dataset_id'], request.user)
    if user_denied:
        return user_denied
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
    selectparams = dset.selectparametervalue_set.filter(
        value__param__category=category).select_related('value')
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
            if (pid in selectparams and value != selectparams[pid].value_id):
                selectparams[pid].value_id = value
                selectparams[pid].save()
            elif pid not in selectparams:
                models.SelectParameterValue.objects.create(
                    dataset_id=data['dataset_id'], value_id=value)
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
    selects, checkboxes, fields = [], [], []
    for param in data['params'].values():
        if param['inputtype'] == 'select':
            value = param['model']
            selects.append(models.SelectParameterValue(dataset_id=dset_id,
                                                       value_id=value))
        elif param['inputtype'] == 'checkbox':
            value = [box['value'] for box in param['fields'] if box['checked']]
            checkboxes.extend([models.CheckboxParameterValue(dataset_id=dset_id,
                                                            value_id=val) for val in value])
        else:
            value = param['model']
            fields.append(models.FieldParameterValue(dataset_id=dset_id,
                                                     param_id=param['param_id'],
                                                     value=value))
    models.SelectParameterValue.objects.bulk_create(selects)
    models.CheckboxParameterValue.objects.bulk_create(checkboxes)
    models.FieldParameterValue.objects.bulk_create(fields)
