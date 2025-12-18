from collections import defaultdict
from datetime import datetime
import json
from enum import StrEnum

from django.contrib.auth.decorators import login_required
from django.contrib.admin.views.decorators import staff_member_required
from django.views.decorators.http import require_GET, require_POST
from django.http import JsonResponse
from django.shortcuts import render
from django.db.models import Q, Max, Sum
from django.db import IntegrityError
from django.utils import timezone

from corefac import models as cm
from rawstatus import models as rm
from datasets import models as dm
from datasets import views as dv
from analysis import models as am
from jobs import models as jm
from jobs import jobs as jj
from home.views import parse_userquery


@staff_member_required
@login_required
@require_GET
def corefac_home(request):
    protos = {}
    for pop in cm.PrepOptionProtocol.objects.filter(active=True).values('pk', 'version', 'doi', 'paramopt__value', 'paramopt__param__title'):
        protos[pop['pk']] = {'id': pop['pk'],
                'name': f'{pop["paramopt__value"]} - {pop["paramopt__param__title"]} - {pop["doi"]} - {pop["version"]}'}
    enzymes = {x.id: {'id': x.id, 'name': x.name} for x in dm.Enzyme.objects.all()}
    context = {'ctx': {'protocols': protos,
        'enzymes': [x for x in enzymes.values()],
        'ptypes': [{'id': x['pk'], 'name': x['title']} for x in
            dm.SampleprepParameter.objects.filter(active=True).values('title', 'pk')]
        }}
    return render(request, 'corefac/corefac.html', context)


def get_pipeline_step_name(stepvals):
    return f'{stepvals["step__paramopt__value"]} - {stepvals["step__paramopt__param__title"]} - {stepvals["step__doi"]} - {stepvals["step__version"]}'


def get_project_plotdata(request):
    class Stages(StrEnum):
        OPENED = 'Opened'
        SAMPLES = 'Samples arrived'
        PREP = 'Prep'
        QUEUE = 'MS queue'
        MS = 'Acquisition'
        SEARCH = 'Search'

    data = json.loads(request.body.decode('utf-8'))

    # For all closed projects, get the last log date where the closing happened
    # last date is done by group by proj_id, annotate Max(date)
    closeprojdates = {}
    datefmt = '%Y-%m-%dT%H:%M:%S'
    for closeplog in dm.ProjectLog.objects.filter(project_id__in=data['proj_ids'],
            project__active=False, level=dm.ProjLogLevels.CLOSE).values('project_id'
                    ).annotate(lastclose=Max('date')):
        closeprojdates[closeplog['project_id']] = datetime.strftime(closeplog['lastclose'], datefmt)

    perprojects = []
    firstdate = timezone.now()
    today = timezone.now()
    ds_owners = {}
    for dp in dm.Dataset.objects.filter(runname__experiment__project_id__in=data['proj_ids']
            ).values('pk', 'locked', 'runname__experiment__project__registered',
                    'runname__name', 'runname__experiment__name',
                    'runname__experiment__project__name', 'runname__experiment__project_id',
                    'runname__experiment__project__active'):
        bartext = (f'{dp["runname__experiment__project__name"]} / {dp["runname__experiment__name"]} '
                f'/ {dp["runname__name"]}')
        owner_q = dm.DatasetOwner.objects.filter(dataset_id=dp['pk']).values('user__username')
        ds_owners[dp['pk']] = owner_q.first()['user__username']
        if anas := am.Analysis.objects.filter(datasetanalysis__dataset_id=dp['pk']).values('date',
                'pk').order_by('date'):
            # Create a "search" bar segment
            anadate = anas.first()['date']
            if anadate < firstdate:
                firstdate = anadate
            if jobs := jm.Job.objects.filter(nextflowsearch__analysis_id__in=[x['pk'] for x in anas]):
                lastjob = jobs.values('state', 'pk', 'timestamp').order_by('timestamp').last()
                arfs = am.AnalysisResultFile.objects.filter(analysis__nextflowsearch__job_id=lastjob['pk'])
                if lastjob['state'] == jj.Jobstates.DONE and arfs:
                    end = arfs.values('sfile__regdate').first()['sfile__regdate']
                elif lastjob['state'] in jj.JOBSTATES_DONE:
                    # Canceled or done but no files (can be a refine job also)
                    end = lastjob['timestamp']
                else:
                    end = today
                perprojects.append({'proj': bartext, 'dset': dp['pk'],
                    'stage': Stages.SEARCH, 'start': anadate, 'end': end})

        # Raws can come way before project opening, so lets start with them
        firstfile_date = False
        if raws := rm.RawFile.objects.filter(datasetrawfile__dataset_id=dp['pk']).order_by('date'
                ).values('date', 'pk'):
            firstfile_date = raws.first()['date']
            if firstfile_date < firstdate:
                firstdate = firstfile_date
            rawpp = {'proj': bartext, 'dset': dp['pk'], 'stage': Stages.MS, 'start': firstfile_date,
                    }
            if dp['locked'] or anas:
                sf = rm.StoredFile.objects.values('regdate').get(rawfile_id=raws.last()['pk'],
                        mzmlfile__isnull=True)
                # Use regdate on last file to include the time spent transferring
                rawpp['end'] = sf['regdate']
            else:
                rawpp['end'] = today

        openpp = False
        opendate = dp['runname__experiment__project__registered']
        if not firstfile_date or opendate < firstfile_date:
            if opendate < firstdate:
                firstdate = opendate
            firstbar_start = opendate
            openpp = {'proj': bartext, 'dset': dp['pk'],
                    'open': dp['runname__experiment__project__active'],
                    'pid': dp['runname__experiment__project_id'],
                    'stage': Stages.OPENED, 'start': opendate,
                    'first': True}
        else:
            # The opening of project was done after acquisition of files
            rawpp.update({'first': True, 'pid': dp['runname__experiment__project_id'],
                    'open': dp['runname__experiment__project__active'],
                })
            firstbar_start = firstfile_date
        if raws:
            perprojects.append(rawpp)

        track_pipeline = False
        if openpp:
            firststage = openpp['start']
        elif firstfile_date:
            firststage = firstfile_date
        
        for tr in cm.DatasetPrepTracking.objects.filter(dspipe__dataset_id=dp['pk']).order_by(
                'timestamp'):
            track_pipeline = True
            if tr.stage == cm.TrackingStages.SAMPLESREADY and tr.timestamp > firstbar_start:
                # Normal, i.e first opened proj, then samples arrive
                if openpp:
                    openpp['end'] = tr.timestamp
                pp = {'proj': bartext, 'dset': dp['pk'],
                        'stage': Stages.SAMPLES, 'start': tr.timestamp}

            elif tr.stage == cm.TrackingStages.SAMPLESREADY:
                # Samples ready before opening project
                pp = {'proj': bartext, 'dset': dp['pk'],
                        'stage': Stages.SAMPLES, 'start': tr.timestamp}

            elif tr.stage == cm.TrackingStages.PREPSTARTED and pp['stage'] == Stages.SAMPLES:
                pp['end'] = tr.timestamp
                perprojects.append(pp)
                pp = {'proj': bartext, 'dset': dp['pk'],
                        'stage': Stages.PREP, 'start': tr.timestamp}
            elif tr.stage == cm.TrackingStages.PREPFINISHED and pp['stage'] == Stages.PREP:
                pp['end'] = tr.timestamp
                perprojects.append(pp)

            elif tr.stage == cm.TrackingStages.MSQUEUED:
                pp = {'proj': bartext, 'dset': dp['pk'],
                        'stage': Stages.QUEUE, 'start': tr.timestamp}

        if track_pipeline:
            if pp['stage'] == Stages.QUEUE:
                if firstfile_date:
                    pp['end'] = firstfile_date
                else:
                    pp['end'] = today
            elif 'end' not in pp:
                if dp['locked']:
                    lockdate = dm.ProjectLog.objects.filter(project_id=dp['runname__experiment__project_id'],
                            level=dm.ProjLogLevels.INFO, message__endswith=f'locked dataset {dp["pk"]}')
                    pp['end'] = lockdate.values('date').last()['date']
                else:
                    pp['end'] = today
                perprojects.append(pp)

        if openpp and not openpp.get('end'):
            if firstfile_date:
                openpp['end'] = firstfile_date
            else:
                openpp['end'] = today
        if openpp:
            perprojects.append(openpp)

    firstdates_open, lastdates, mstimes = {}, {}, {}
    for pp in perprojects:
        if not (ld := lastdates.get(pp['dset'])) or ld < pp['end']:
            lastdates[pp['dset']] = pp['end']
    for pp in perprojects:
        if pp.get('first'):
            firstdates_open[pp['dset']] = (pp['start'], pp['open'])
            pp['duration'] = (lastdates[pp['dset']] - pp['start']).days
            files = rm.RawFile.objects.filter(datasetrawfile__dataset_id=pp['dset'])
            msfiles = rm.MSFileData.objects.filter(rawfile__datasetrawfile__dataset_id=pp['dset'])
            pp['fsize'] = files.aggregate(Sum('size'))['size__sum']
            pp['mstime'] = msfiles.aggregate(Sum('mstime'))['mstime__sum'] or 0
            mstimes[pp['dset']] = pp['mstime']

    for pp in perprojects:
        # Mark last segments in dset, add open/owner and start of dset there
        if lastdates[pp['dset']] == pp['end']:
            pp['last'] = True
            pp['startdset'] = firstdates_open[pp['dset']][0]
            pp['open'] = firstdates_open[pp['dset']][1]
            pp['owner'] = ds_owners[pp['dset']]
            pp['mstime'] = mstimes[pp['dset']]

    [x.update({'stage': str(x['stage']), 'start': datetime.strftime(x['start'], datefmt),
        'end': datetime.strftime(x['end'], datefmt)}) for x in perprojects]

    return JsonResponse({'per_proj': perprojects, 'closedates': closeprojdates, 'today': today,
        'firstdate': datetime.strftime(firstdate, datefmt)})


@login_required
def find_sampleprep_method(request):
    searchterms = []
    userquery = request.GET.get('q')
    parsed_query = Q()
    for q in [x for x in userquery.split(',') if x != '']:
        match q.split(':'):
            case ['from', date]:
                if dt := parse_userquery(date, 'date'):
                    parsed_query &= Q(registered__gte=dt)
            case ['to', date]:
                if dt := parse_userquery(date, 'date'):
                    parsed_query &= Q(registered__lte=dt)
            case ['type', ptypename]:
                parsed_query &= Q(paramopt__param__title__iexact=ptypename)
            case ['name', name]:
                parsed_query &= Q(paramopt__value=name)
            case ['doi', doi]:
                parsed_query &= Q(doi=doi)
            case ['version', version]:
                parsed_query &= Q(version=version)
            case ['active', yesno]:
                parsed_query &= Q(active={'yes': True, 'true': True, 'no': False, 'false': False}[yesno])
            case _:
                searchterms.append(q)
    protocols = cm.PrepOptionProtocol.objects.filter(parsed_query)

    if searchterms:
        freetextq = Q()
        for term in searchterms:
            subq = Q(doi__icontains=term)
            subq |= Q(version__icontains=term)
            subq |= Q(paramopt__value__icontains=term)
            freetextq &= subq
        protocols = protocols.filter(freetextq)
    items, order = populate_protocols(protocols)
    # FIXME addd empty protocols?
    # empty protocols are not
    return JsonResponse({'items': items, 'order': order})


@login_required
def find_pipeline(request):
    searchterms = []
    userquery = request.GET.get('q')
    parsed_query = Q()
    for q in [x for x in userquery.split(',') if x != '']:
        match q.split(':'):
            case ['from', date]:
                if dt := parse_userquery(date, 'date'):
                    parsed_query &= Q(timestamp__gte=dt)
            case ['to', date]:
                if dt := parse_userquery(date, 'date'):
                    parsed_query &= Q(timestamp__lte=dt)
            case ['name', name]:
                parsed_query &= Q(pipeline__name=name)
            case ['version', version]:
                parsed_query &= Q(version=version)
            case ['active', yesno]:
                parsed_query &= Q(active={'yes': True, 'true': True, 'no': False, 'false': False}[yesno])
            case _:
                searchterms.append(q)
    pipelines = cm.PipelineVersion.objects.filter(parsed_query)

    if searchterms:
        freetextq = Q()
        for term in searchterms:
            subq = Q(pipeline__name__icontains=term)
            subq |= Q(version__icontains=term)
            freetextq &= subq
        pipelines = pipelines.filter(freetextq)
    items, order = populate_pipelines(pipelines)
    # FIXME addd empty protocols?
    return JsonResponse({'items': items, 'order': order})


def populate_protocols(dbprotocols):
    protocols, order = {}, []
    for prot in dbprotocols.order_by('-registered').values('pk', 'paramopt__value', 'paramopt_id', 'active', 'registered', 'paramopt__param__title', 'version', 'doi'):
        order.append(prot['pk'])
        protocols[prot['pk']] = {
            'id': prot['pk'],
            'name': prot['paramopt__value'],
            'protocol_id': prot['paramopt_id'],
            'version': prot['version'],
            'doi': prot['doi'],
            'inactive': prot['active'] == False,
            'start': datetime.strftime(prot['registered'], '%Y-%m-%d %H:%M'),
            'ptype': prot['paramopt__param__title'],
            'details': False,
            'selected': False,
            'actions': ['new version'],
        }
        if prot['active']:
            protocols[prot['pk']]['actions'].append('deactivate')
        else:
            protocols[prot['pk']]['actions'].append('reactivate')
    return protocols, order


def populate_pipelines(dbpipelines):
    pipelines, order = {}, []
    for pipe in dbpipelines.order_by('-timestamp').values('pk', 'pipeline_id', 'pipeline__name', 'locked', 'active', 'timestamp', 'version'):
        order.append(pipe['pk'])
        pipelines[pipe['pk']] = {
            'id': pipe['pk'],
            'name': pipe['pipeline__name'],
            'pipeline_id': pipe['pipeline_id'],
            'version': pipe['version'],
            'locked': pipe['locked'],
            'inactive': pipe['active'] == False,
            'start': datetime.strftime(pipe['timestamp'], '%Y-%m-%d %H:%M'),
            'details': False,
            'selected': False,
            'actions': ['new version'],
            'enzymes': [x.enzyme_id for x in
                cm.PipelineEnzyme.objects.filter(pipelineversion_id=pipe['pk'])],
            'steps': [{'name': get_pipeline_step_name(x), 'id': x['step_id'], 'ix': x['index']}
                for x in  cm.PipelineStep.objects.filter(pipelineversion_id=pipe['pk']).values(
                    'step_id', 'index', 'step__doi', 'step__version', 'step__paramopt__value',
                    'step__paramopt__param__title')]
        }
        if pipe['active']:
            pipelines[pipe['pk']]['actions'].append('deactivate')
        else:
            pipelines[pipe['pk']]['actions'].append('reactivate')
        if not pipe['locked']:
            pipelines[pipe['pk']]['actions'].extend(['lock', 'edit'])
    return pipelines, order


@staff_member_required
@login_required
@require_POST
def add_protocol(request):
    '''Creates new protocol and version of it (two tables)'''
    req = json.loads(request.body.decode('utf-8'))
    try:
        param_id, name, doi, version = req['param_id'], req['name'], req['doi'], req['version']
    except KeyError:
        return JsonResponse({'error': 'Bad request to add protocol method, contact admin'},
                status=400)
    if cm.PrepOptionProtocol.objects.filter(doi=doi).exists():
        return JsonResponse({'error': 'A parameter with this DOI already exists'},
                status=400)
    spo, cr = dm.SampleprepParameterOption.objects.get_or_create(param_id=param_id, value=name)
    if not cr:
        return JsonResponse({'error': 'A protocol with this name already exists'}, status=400)
    pop = cm.PrepOptionProtocol.objects.create(paramopt=spo, doi=doi, version=version)
    newp, _order = populate_protocols(cm.PrepOptionProtocol.objects.filter(pk=pop.pk))
    return JsonResponse(newp[pop.pk])


@staff_member_required
@login_required
@require_POST
def add_protocol_version(request):
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
    newp, _order = populate_protocols(cm.PrepOptionProtocol.objects.filter(pk=pop.pk))
    return JsonResponse(newp[pop.pk])


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
@require_POST
def delete_protocol_version(request):
    '''Tries to delete protocol versions passed. After that will also, try to
    delete the protocol, if:
    - protocol has no more versions
    - protocol not linked to dataset
    - protocol not used in pipeline
    # FIXME protocol that is linked in dataset which gets versions deleted
    # will be empty - make a method to delete it as it wont show in the table
    '''
    req = json.loads(request.body.decode('utf-8'))
    try:
        prepprot_ids = req['prepprot_ids']
    except KeyError:
        return JsonResponse({'error': 'Bad request to delete sampleprep protocol, contact admin'},
                status=400)
    spo_ids = [x.pk for x in dm.SampleprepParameterOption.objects.filter(
            prepoptionprotocol__in=prepprot_ids)]
    if not (pop := cm.PrepOptionProtocol.objects.filter(pk__in=prepprot_ids)):
        return JsonResponse({'error': 'Could not find sampleprep protocol(s) to delete, contact admin'},
                status=400)
    elif cm.PipelineStep.objects.filter(step_id__in=prepprot_ids).exists():
        return JsonResponse({'error': 'Pipeline(s) exist mapped to this protocol, we can not delete it! Try to inactivate instead'}, status=403)
    pop.delete()

    if spo_ids and (spo := dm.SampleprepParameterOption.objects.filter(pk__in=spo_ids,
                prepoptionprotocol__isnull=True)):
        if dm.SampleprepParameterValue.objects.filter(value__in=spo).exists():
            # Datasets mapped to protocol, so will not delete it
            pass
        elif cm.PipelineStep.objects.filter(step__paramopt__in=spo).exists():
            # Pipeline using some step of mapped to protocol, so will not delete it
            # FIXME this is already checked above no?
            pass
        else:
            spo.delete()
    return JsonResponse({})


@staff_member_required
@login_required
def save_sampleprep_pipeline(request):
    req = json.loads(request.body.decode('utf-8'))
    try:
        pvid, version, pipe_id, steps = req['id'], req['version'], req['pipe_id'], req['steps']
        pipename, enzymes = req['pipename'], req['enzymes']
    except KeyError:
        return JsonResponse({'error': 'Bad request to edit pipeline method, contact admin'},
                status=400)
    pvq = cm.PipelineVersion.objects.filter(pk=pvid)
    if req['id'] and not pvq.exists():
        return JsonResponse({'error': 'Could not find that pipeline, contact admin'}, status=404)
    elif req['id'] and pvq.filter(locked=True).exists():
        return JsonResponse({'error': 'Pipeline is locked, cannot edit it anymore, create a new version instead'}, status=403)
    elif req['id']:
        # Update version with steps or version name
        try:
            pvq.filter(locked=False).update(version=version, pipeline_id=pipe_id)
        except IntegrityError:
            return JsonResponse({'error': 'Pipeline version name already exists for another '
                'pipeline version, choose a another name'}, status=403)
            
        pversion = pvq.get()
    elif req['pipe_id']:
        # Creating a new version of this pipeline
        pversion, cr = cm.PipelineVersion.objects.get_or_create(pipeline_id=req['pipe_id'],
                version=version, defaults={'timestamp': datetime.now()})
        if not cr:
            return JsonResponse({'error': 'Pipeline version name already exists, edit it instead or '
                'choose another name'}, status=403)
    else:
        # no pipe id, no pipeversion id -> new pipeline
        pipeline, cr = cm.SamplePipeline.objects.get_or_create(name=pipename)
        if not cr:
            return JsonResponse({'error': 'Pipeline name already exists, create a new version instead'}, status=403)
        pversion = cm.PipelineVersion.objects.create(pipeline=pipeline, version=version,
                timestamp=datetime.now())

    # Remove old steps that are not needed if pipeline is shorter, update remaining/new steps
    cm.PipelineStep.objects.filter(pipelineversion=pversion).exclude(
            index__in=[x['ix'] for x in steps]).delete()
    for step in steps:
        cm.PipelineStep.objects.update_or_create(pipelineversion=pversion, index=step['ix'],
                defaults={'step_id': step['id']})
    cm.PipelineEnzyme.objects.filter(pipelineversion=pversion).delete()
    if len(enzymes):
        cm.PipelineEnzyme.objects.bulk_create([cm.PipelineEnzyme(pipelineversion=pversion,
            enzyme_id=eid) for eid in enzymes])
    newpipes, _order = populate_pipelines(cm.PipelineVersion.objects.filter(pk=pversion.pk))
    return JsonResponse({'pipeline': newpipes[pversion.pk]})


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
        pvids = req['ids']
    except KeyError:
        return JsonResponse({'error': 'Bad request to delete pipeline, contact admin'},
                status=400)
    pipelines = cm.PipelineVersion.objects.filter(pk__in=pvids)
    if not pipelines.count() == len(pvids):
        return JsonResponse({'error': 'Could not find all pipelines, contact admin'}, status=400)
    if cm.DatasetPipeline.objects.filter(pipelineversion__id__in=pvids).exists():
        return JsonResponse({'error': 'Datasets exist mapped to pipelines, we cant delete!'}, status=403)
    # First delete the parent SamplePipeline where those have no other pipelineversions than
    # the ones passed. This cascades to those versions
    for parent in cm.SamplePipeline.objects.filter(pipelineversion__id__in=pvids).values('pk'
            ).distinct('pk'):
        if not cm.PipelineVersion.objects.filter(pipeline_id=parent['pk']).exclude(pk__in=pvids):
            parent.delete()
    pipelines.delete()
    return JsonResponse({})
