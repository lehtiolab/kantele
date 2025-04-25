from collections import defaultdict
from datetime import datetime
import json

from django.contrib.auth.decorators import login_required
from django.contrib.admin.views.decorators import staff_member_required
from django.views.decorators.http import require_GET, require_POST
from django.http import JsonResponse
from django.shortcuts import render
from django.db.models import Q

from corefac import models as cm
from datasets import models as dm


@staff_member_required
@login_required
@require_GET
def corefac_home(request):
    protos = {}
    all_doi = defaultdict(list)
    for pop in cm.PrepOptionProtocol.objects.all():
        all_doi[pop.paramopt_id].append({'id': pop.pk, 'version': pop.version, 'doi': pop.doi,
            'active': pop.active})
        
    # create following JSON:
    # {1: { id: 1, title: Cleanup, methods: [{name: SP3, versions: [(v1, doi123), ]}]}
    for spcat in dm.SampleprepParameter.objects.all().values('title', 'pk', 'active'):
        protos[spcat['pk']] = {'id': spcat['pk'], 'title': spcat['title'], 'active': spcat['active'],
                'methods': []}
    for spo in dm.SampleprepParameterOption.objects.all().values('param__pk', 'value', 'pk', 'active'):
        versions = all_doi[spo['pk']] if spo['pk'] in all_doi else []
        protos[spo['param__pk']]['methods'].append({'name': spo['value'], 'id': spo['pk'],
            'versions': versions, 'active': spo['active']})
    pipelines = {}
    
    enzymes = {x.id: {'id': x.id, 'name': x.name} for x in dm.Enzyme.objects.all()}
    for pv in cm.PipelineVersion.objects.all().values('pk', 'pipeline_id', 'pipeline__name',
            'version', 'active', 'locked', 'timestamp'):
        pipelines[pv['pk']] = {'id': pv['pk'], 'pipe_id': pv['pipeline_id'], 'active': pv['active'],
                'locked': pv['locked'], 'name': pv['pipeline__name'], 'version': pv['version'],
                'enzymes': [x.enzyme_id for x in 
                    cm.PipelineEnzyme.objects.filter(pipelineversion_id=pv['pk'])],
                'steps': [{'name': get_pipeline_step_name(x), 'id': x['step_id'], 'ix': x['index']}
                    for x in  cm.PipelineStep.objects.filter(pipelineversion_id=pv['pk']).values(
                        'step_id', 'index', 'step__doi',
                        'step__version', 'step__paramopt__value',
                        'step__paramopt__param__title')]}
        if pv['locked']:
            pipelines[pv['pk']]['timestamp'] = datetime.strftime(pv['timestamp'], '%Y-%m-%d %H:%M')

    context = {'ctx': {'protocols': protos, 'pipelines': pipelines, 'enzymes': [x for x in enzymes.values()]}}
    return render(request, 'corefac/corefac.html', context)


def get_pipeline_step_name(stepvals):
    return f'{stepvals["step__paramopt__param__title"]} - {stepvals["step__paramopt__value"]} - {stepvals["step__doi"]} - {stepvals["step__version"]}'


@staff_member_required
@login_required
@require_POST
def add_sampleprep_method(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        param_id, name = req['param_id'], req['newname']
    except KeyError:
        return JsonResponse({'error': 'Bad request to add sampleprep method, contact admin'},
                status=400)
    spo, cr = dm.SampleprepParameterOption.objects.get_or_create(param_id=param_id, value=name)
    if not cr:
        return JsonResponse({'error': 'A parameter with this name already exists'}, status=400)
    return JsonResponse({'id': spo.pk})


@staff_member_required
@login_required
def add_sampleprep_method_version(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        paramopt_id, doi, version = req['paramopt_id'], req['doi'], req['version']
    except KeyError:
        return JsonResponse({'error': 'Bad request to add sampleprep method, contact admin'},
                status=400)
    dupq = Q(paramopt_id=paramopt_id, version=version) | Q(doi=doi)

    if cm.PrepOptionProtocol.objects.filter(dupq).exists():
        return JsonResponse({'error': 'A parameter with this version and/or DOI already exists'},
                status=400)
    pop = cm.PrepOptionProtocol.objects.create(paramopt_id=paramopt_id, doi=doi, version=version)
    return JsonResponse({'id': pop.pk})


@staff_member_required
@login_required
@require_POST
def edit_sampleprep_method(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        paramopt_id, name = req['paramopt_id'], req['newname']
    except KeyError:
        return JsonResponse({'error': 'Bad request to edit sampleprep method, contact admin'},
                status=400)
    spo = dm.SampleprepParameterOption.objects.filter(pk=paramopt_id)
    if not spo.exists():
        return JsonResponse({'error': 'Could not find method, contact admin'}, status=400)
    spo.update(value=name)
    return JsonResponse({})


@staff_member_required
@login_required
@require_POST
def edit_sampleprep_method_version(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        protid, doi, version = req['prepprot_id'], req['doi'], req['version']
    except KeyError:
        return JsonResponse({'error': 'Bad request to add sampleprep method, contact admin'},
                status=400)
    pop = cm.PrepOptionProtocol.objects.filter(pk=protid, doi=doi)
    if not pop.exists():
        return JsonResponse({'error': 'Could not find protocol with that DOI, note that DOIs are '
            'not changeable'}, status=403)
    pop.update(version=version)
    return JsonResponse({})


@staff_member_required
@login_required
def disable_sampleprep_method(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        paramopt_id = req['paramopt_id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to disable sampleprep method, contact admin'},
                status=400)
    spo = dm.SampleprepParameterOption.objects.filter(pk=paramopt_id)
    if not spo.count():
        return JsonResponse({'error': 'Could not find method, contact admin'}, status=400)
    spo.update(active=False)
    return JsonResponse({})


@staff_member_required
@login_required
def disable_sampleprep_method_version(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        prepprot_id = req['prepprot_id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to disable sampleprep protocol, contact admin'},
                status=400)
    pop = cm.PrepOptionProtocol.objects.filter(pk=prepprot_id)
    if not pop.count():
        return JsonResponse({'error': 'Could not find method, contact admin'}, status=400)
    pop.update(active=False)
    return JsonResponse({})



@staff_member_required
@login_required
def enable_sampleprep_method(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        paramopt_id = req['paramopt_id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to enable sampleprep method, contact admin'},
                status=400)
    spo = dm.SampleprepParameterOption.objects.filter(pk=paramopt_id, active=False)
    if not spo.count():
        return JsonResponse({'error': 'Could not find method, contact admin'}, status=400)
    spo.update(active=True)
    return JsonResponse({})


@staff_member_required
@login_required
def enable_sampleprep_method_version(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        prepprot_id = req['prepprot_id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to enable sampleprep protocol, contact admin'},
                status=400)
    pop = cm.PrepOptionProtocol.objects.filter(pk=prepprot_id)
    if not pop.count():
        return JsonResponse({'error': 'Could not find method, contact admin'}, status=400)
    pop.update(active=True)
    return JsonResponse({})



@staff_member_required
@login_required
def delete_sampleprep_method(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        paramopt_id = req['paramopt_id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to delete sampleprep method, contact admin'},
                status=400)
    spo = dm.SampleprepParameterOption.objects.filter(pk=paramopt_id)
    if not spo.count():
        return JsonResponse({'error': 'Could not find method, contact admin'}, status=400)
    if dm.SampleprepParameterValue.objects.filter(value_id=paramopt_id).exists():
        return JsonResponse({'error': 'Datasets exist mapped to this method, we cant delete it!'}, status=403)
    spo.delete()
    return JsonResponse({})


@staff_member_required
@login_required
def delete_sampleprep_method_version(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        prepprot_id = req['prepprot_id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to delete sampleprep protocol, contact admin'},
                status=400)
    pop = cm.PrepOptionProtocol.objects.filter(pk=prepprot_id)
    if not pop.count():
        return JsonResponse({'error': 'Could not find sampleprep protocol to delete, contact admin'},
                status=400)
    if cm.DatasetPipeline.objects.filter(pipelineversion__pipelinestep__step_id=prepprot_id).exists():
        return JsonResponse({'error': 'Datasets exist mapped to this protocol, we cant delete it!'}, status=403)
    pop.delete()
    return JsonResponse({})


@staff_member_required
@login_required
def add_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        name, version = req['name'], req['version']
    except KeyError:
        return JsonResponse({'error': 'Bad request to add sampleprep pipeline, contact admin'},
                status=400)
    pipeline, _ = cm.SamplePipeline.objects.get_or_create(name=name)
    pversion, cr = cm.PipelineVersion.objects.get_or_create(pipeline=pipeline, version=version,
            defaults={'timestamp': datetime.now()})
    if not cr:
        return JsonResponse({'error': 'Pipeline of this version already exists'}, status=400)
    return JsonResponse({'id': pversion.pk, 'pipe_id': pipeline.pk})


@staff_member_required
@login_required
def edit_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        pvid, version, pipe_id, steps = req['id'], req['version'], req['pipe_id'], req['steps']
        enzymes = req['enzymes']
    except KeyError:
        return JsonResponse({'error': 'Bad request to edit pipeline method, contact admin'},
                status=400)
    pvq = cm.PipelineVersion.objects.filter(pk=pvid)
    if not pvq.exists():
        return JsonResponse({'error': 'Could not find that pipeline, contact admin'}, status=404)
    elif pvq.filter(locked=True).exists():
        return JsonResponse({'error': 'Pipeline is locked, cannot edit it anymore, create a new version instead'}, status=403)
    # Update version and possibly pipeline FK
    pvq.filter(locked=False).update(version=version, pipeline_id=pipe_id)
    # Remove old steps that are not needed if pipeline is shorter, update remaining/new steps
    cm.PipelineStep.objects.filter(pipelineversion_id=pvid).exclude(
            index__in=[x['ix'] for x in steps]).delete()
    for step in steps:
        cm.PipelineStep.objects.update_or_create(pipelineversion_id=pvid, index=step['ix'],
                defaults={'step_id': step['id']})
    cm.PipelineEnzyme.objects.filter(pipelineversion_id=pvid).delete()
    if len(enzymes):
        cm.PipelineEnzyme.objects.bulk_create([cm.PipelineEnzyme(pipelineversion_id=pvid,
            enzyme_id=eid) for eid in enzymes])
    return JsonResponse({})


@staff_member_required
@login_required
def lock_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        pvid = req['id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to disable pipeline, contact admin'},
                status=400)
    pipeline = cm.PipelineVersion.objects.filter(pk=pvid, locked=False)
    if not pipeline.exists():
        return JsonResponse({'error': 'Could not find pipeline, or pipeline already locked, '
            'contact admin'}, status=400)
    pipeline.update(locked=True, timestamp=datetime.now())
    return JsonResponse({})


@staff_member_required
@login_required
def disable_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        pvid = req['id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to disable pipeline, contact admin'},
                status=400)
    pipeline = cm.PipelineVersion.objects.filter(pk=pvid)
    if not pipeline.count():
        return JsonResponse({'error': 'Could not find pipeline, contact admin'}, status=400)
    pipeline.update(active=False)
    return JsonResponse({})


@staff_member_required
@login_required
def enable_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        pvid = req['id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to enable pipeline, contact admin'},
                status=400)
    pipeline = cm.PipelineVersion.objects.filter(pk=pvid)
    if not pipeline.count():
        return JsonResponse({'error': 'Could not find pipeline, contact admin'}, status=400)
    pipeline.update(active=True)
    return JsonResponse({})


@staff_member_required
@login_required
def delete_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        pvid = req['id']
    except KeyError:
        return JsonResponse({'error': 'Bad request to delete pipeline, contact admin'},
                status=400)
    pipeline = cm.PipelineVersion.objects.filter(pk=pvid)
    if not pipeline.count():
        return JsonResponse({'error': 'Could not find pipeline, contact admin'}, status=400)
    if cm.DatasetPipeline.objects.filter(pipelineversion__id=pvid).exists():
        return JsonResponse({'error': 'Datasets exist mapped to this pipeline, we cant delete it!'}, status=403)
    motherpipeline = pipeline.values('pipeline_id').get()['pipeline_id']
    if not cm.PipelineVersion.objects.filter(pipeline_id=motherpipeline).exists():
        pipeline.delete()
        cm.SamplePipeline.objects.filter(pk=motherpipeline).delete()
    else:
        pipeline.delete()
    return JsonResponse({})

