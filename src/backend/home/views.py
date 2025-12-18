import os
from datetime import timedelta, datetime
import json
from base64 import b64encode
from celery import states as tstates

from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.models import User
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden
from django.db.models import Q, Sum, Count, F, Value
from django.db.models.functions import Trunc
from collections import OrderedDict, defaultdict

from kantele import settings
from datasets import models as dsmodels
from analysis import models as anmodels
from analysis import views as av
from analysis import jobs as aj
from datasets import models as dm
from datasets import jobs as dsjobs
from datasets.views import check_ownership, get_dset_storestate, fill_sampleprepparam, populate_proj
from rawstatus import models as filemodels
from jobs import jobs as jj
from jobs import views as jv
from jobs.jobutil import create_job, jobmap
from jobs import models as jm
from mstulos import models as mm
from corefac import models as cm


@login_required
@require_GET
def home(request):
    """Returns home view with apps that will separately request"""
    context = {
               'username': request.user.username,
               'ptypes': [{'name': x.name, 'id': x.id} for x in
                     dsmodels.ProjectTypeName.objects.all()],
               'local_ptype_id': settings.LOCAL_PTYPE_ID,
              'external_pis': {x.id: {'name': x.name, 'id': x.id} for x in
                  dsmodels.PrincipalInvestigator.objects.all()},
               }
    return render(request, 'home/home.html', context)


def find_projects(userquery):
    searchterms = []
    parsed_query = Q()
    for q in [x for x in userquery.split(',') if x != '']:
        match q.split(':'):
            case ['from', date]:
                if dt := parse_userquery(date, 'date'):
                    parsed_query &= Q(registered__gte=dt)
            case ['to', date]:
                if dt := parse_userquery(date, 'date'):
                    parsed_query &= Q(registered__lte=dt)
            case ['type', projtype]:
                ptypename = {'cf': settings.CF_PTYPE_NAME}.get(projtype) or projtype
                parsed_query &= Q(ptype__name__iexact=ptypename)
            case ['name', name]:
                parsed_query &= Q(name=name)
            case ['active', yesno]:
                parsed_query &= Q(active={'yes': True, 'true': True, 'no': False, 'false': False}[yesno])
            case ['user', username]:
                parsed_query &= Q(experiment__runname__dataset__datasetowner__user__username=username)
            case _:
                searchterms.append(q)
    projects = dsmodels.Project.objects.filter(parsed_query)

    if searchterms:
        freetextq = Q()
        for term in searchterms:
            subq = Q(experiment__runname__name__icontains=term)
            subq |= Q(experiment__name__icontains=term)
            subq |= Q(name__icontains=term)
            freetextq &= subq
        projects = projects.filter(freetextq)
    return projects


def dataset_query_creator(searchterms):
    query = Q(runname__name__icontains=searchterms[0])
    query |= Q(runname__experiment__name__icontains=searchterms[0])
    query |= Q(runname__experiment__project__name__icontains=searchterms[0])
    query |= Q(datatype__name__icontains=searchterms[0])
    #query |= Q(user__username__icontains=searchterms[0])
    try:
        float(searchterms[0])
    except ValueError:
        pass
    else:
        query |= Q(prefractionationdataset__hiriefdataset__hirief__start=searchterms[0])
        query |= Q(prefractionationdataset__hiriefdataset__hirief__end=searchterms[0])
    for term in searchterms[1:]:
        subquery = Q(runname__name__icontains=term)
        subquery |= Q(runname__experiment__name__icontains=term)
        subquery |= Q(runname__experiment__project__name__icontains=term)
        subquery |= Q(datatype__name__icontains=term)
        #subquery |= Q(user__username__icontains=term)
        try:
            float(term)
        except ValueError:
            pass
        else:
            subquery |= Q(prefractionationdataset__hiriefdataset__hirief__start=term)
            subquery |= Q(prefractionationdataset__hiriefdataset__hirief__end=term)
        query &= subquery
    return dsmodels.Dataset.objects.filter(query)


@login_required
@require_GET
def find_datasets(request):
    """Loop through comma-separated q-param in GET, do a lot of OR queries on
    datasets to find matches. String GET-derived q-params by AND."""
    if searchterms := [x for x in request.GET['q'].split(',') if x != '']:
        dbdsets = dataset_query_creator(searchterms)
    else:
        dbdsets = dsmodels.Dataset.objects.none()
    if request.GET.get('deleted', 'false') == 'false':
        dbdsets = dbdsets.filter(deleted=False)
    dsids = [x['pk'] for x in dbdsets.values('pk')]
    dsets = populate_dset(dsids, dbdsets, request.user)
    return JsonResponse({'items': dsets, 'order': list(dsets.keys())})


@login_required
@require_GET
def find_analysis(request):
    """Loop through comma-separated q-param in GET, do a lot of OR queries on
    analysis to find matches. String GET-derived q-params by AND."""
    searchterms = [x for x in request.GET['q'].split(',') if x != '']
    query = Q()
    wftypes = zip(anmodels.UserWorkflow.WFTypeChoices.choices, anmodels.UserWorkflow.WFTypeChoices.names)
    for term in searchterms:
        subquery = Q(name__icontains=term)
        subquery |= Q(nextflowsearch__workflow__name__icontains=term)
        subquery |= Q(user__username__icontains=term)
        # WF types is integerchoice, search as such
        match_wftypes = [x[0][0] for x in wftypes if term in x[0][1] or term in x[1]]
        subquery |= Q(nextflowsearch__workflow__wftype__in=match_wftypes)
        query &= subquery
    dbanalyses = anmodels.Analysis.objects.filter(query)
    if request.GET['deleted'] == 'false':
        dbanalyses = dbanalyses.filter(deleted=False)
    items, it_order = populate_analysis(dbanalyses.order_by('-date'), request.user)
    return JsonResponse({'items': items, 'order': it_order})


@login_required
@require_GET
def show_analyses(request):
    if 'ids' in request.GET:
        ids = request.GET['ids'].split(',')
        dbanalyses = anmodels.Analysis.objects.filter(pk__in=ids)
    else:
        # last 6month analyses of a user plus current analyses PENDING/PROCESSING
        run_ana = anmodels.Analysis.objects.select_related('nextflowsearch__workflow').filter(
            nextflowsearch__job__state__in=jj.JOBSTATES_WAIT, deleted=False).exclude(user_id=request.user.id)
        user_ana = anmodels.Analysis.objects.select_related('nextflowsearch__workflow').filter(
            user_id=request.user.id, deleted=False, date__gt=datetime.today() - timedelta(183))
        dbanalyses = user_ana | run_ana
    items, it_order = populate_analysis(dbanalyses.order_by('-date'), request.user)
    return JsonResponse({'items': items, 'order': it_order})


@login_required
@require_GET
def show_projects(request):
    if 'ids' in request.GET:
        pids = request.GET['ids'].split(',')
        dbprojects = dsmodels.Project.objects.filter(pk__in=pids)
    elif userq := request.GET.get('q'):
        dbprojects = find_projects(userq)
    else:
        # all active projects
        dbprojects = dsmodels.Project.objects.filter(active=True)
    items, order = populate_proj(dbprojects, request.user)
    return JsonResponse({'items': items, 'order': order})


@login_required
@require_GET
def show_datasets(request):
    if 'ids' in request.GET:
        dsids = request.GET['ids'].split(',')
        dbdsets = dsmodels.Dataset.objects.filter(pk__in=dsids)
    else:
        # last month datasets of a user
        dbdsets = dsmodels.Dataset.objects.filter(deleted=False, datasetowner__user_id=request.user.id,
                                                  date__gt=datetime.today() - timedelta(30))
        dsids = [x['pk'] for x in dbdsets.values('pk')]
    dsets = populate_dset(dsids, dbdsets, request.user)
    return JsonResponse({'items': dsets, 'order': list(dsets.keys())})


def find_files(userquery):
    query = Q()
    ex_query = Q()
    searchterms = []
    for q in [x for x in userquery.split(',') if x != '']:
        match q.split(':'):
            case ['from', date]:
                if dt := parse_userquery(date, 'date'):
                    query &= Q(rawfile__date__gte=dt)
            case ['to', date]:
                if dt := parse_userquery(date, 'date'):
                    query &= Q(rawfile__date__lte=dt)
            case ['deleted', 'true']:
                query &= Q(deleted=True)
            case ['deleted', 'false']:
                query &= Q(deleted=False)
            case ['type', ftype]:
                match ftype:
                    case 'qc':
                        query &= Q(rawfile__usetype=filemodels.UploadFileType.QC)
                    case 'raw':
                        query &= Q(rawfile__usetype=filemodels.UploadFileType.RAWFILE)
                    case 'analysis':
                        query &= Q(rawfile__usetype=filemodels.UploadFileType.ANALYSIS)
                    case 'shared':
                        query &= Q(rawfile__usetype__in=[filemodels.UploadFileType.LIBRARY,
                            filemodels.UploadFileType.USERFILE])
            case _:
                searchterms.append(q)
    for term in searchterms:
        subquery = Q(filename__icontains=term)
        subquery |= Q(rawfile__name__icontains=term)
        subquery |= Q(rawfile__producer__name__icontains=term)
        subquery |= Q(storedfileloc__path__icontains=term)
        query &= subquery
    return filemodels.StoredFile.objects.filter(query).exclude(ex_query)


@login_required
@require_GET
def show_files(request):
    if 'ids' in request.GET:
        fnids = request.GET['ids'].split(',')
        dbfns = filemodels.StoredFile.objects.filter(pk__in=fnids)
    elif userq := request.GET.get('q'):
        dbfns = find_files(userq)
    else:
        # last week files 
        # FIXME this is a very slow query
        dbfns = filemodels.StoredFile.objects.filter(regdate__gt=datetime.today() - timedelta(7), deleted=False)
    return JsonResponse(populate_files(dbfns))


def getxbytes(bytes, op=50):
    if bytes is None:
        return '0B'
    if bytes >> op:
        return '{}{}B'.format(bytes >> op, {0: '', 10: 'K', 20: 'M', 30: 'G', 40: 'T', 50: 'P'}[op])
    else:
        return getxbytes(bytes, op-10)


def populate_files(dbfns):
    popfiles = {}
    for fn in dbfns.values('pk', 'filename', 'deleted', 'rawfile_id', 'rawfile__date',
            'rawfile__size', 'rawfile__usetype', 'rawfile__claimed', 'rawfile__producer__name',
            'mzmlfile', 'filetype__name', 'pdcbackedupfile__success', 'rawfile__datasetrawfile',
            'rawfile__datasetrawfile__dataset'):
        currentjobs = filemodels.FileJob.objects.filter(rawfile=fn['rawfile_id']).exclude(
                job__state__in=jj.JOBSTATES_DONE).values('job_id', 'job__state')
        ana_afv = anmodels.Analysis.objects.filter(analysisfilevalue__sfile_id=fn['pk'])
        ana_adsi = anmodels.Analysis.objects.filter(
                datasetanalysis__analysisdsinputfile__sfile_id=fn['pk'])
        it = {'id': fn['pk'],
              'name': fn['filename'],
              'date': datetime.strftime(fn['rawfile__date'], '%Y-%m-%d %H:%M'),
              'size': getxbytes(fn['rawfile__size']) if fn['mzmlfile'] is None else '-',
              'ftype': fn['filetype__name'],
              'analyses': [x['pk'] for x in ana_afv.union(ana_adsi).values('pk')],
              'dataset': [],
              'jobstate': [x['job__state'] for x in currentjobs],
              'job_ids': [x['job_id'] for x in currentjobs],
              'deleted': fn['deleted'],
              'smallstatus': [],
              'actions': [],
             }
        it['backup'] = fn['pdcbackedupfile__success']
        if fn['rawfile__usetype'] == filemodels.UploadFileType.QC:
            it['owner'] = 'QC'
        elif fn['rawfile__usetype'] in [filemodels.UploadFileType.USERFILE,
                filemodels.UploadFileType.LIBRARY]:
            it['owner'] = 'shared'
        elif fn['rawfile__usetype'] == filemodels.UploadFileType.RAWFILE and not fn['rawfile__claimed']:
            it['owner'] = fn['rawfile__producer__name']
        elif fn['rawfile__datasetrawfile']:
            it['owner'] = dsmodels.DatasetOwner.objects.filter(
                    dataset__id=fn['rawfile__datasetrawfile__dataset']).values('user__username'
                            ).first()['user__username']
            it['dataset'] = fn['rawfile__datasetrawfile__dataset']
        elif fn['rawfile__usetype'] == filemodels.UploadFileType.ANALYSIS:
            anas = anmodels.Analysis.objects.filter(analysisresultfile__sfile_id=fn['pk']
                    ).values('pk', 'user__username')
            it['analyses'].extend([x['pk'] for x in anas])
            it['owner'] = anas.first()['user__username']
        else:
            # Claimed file without datasetrawfile, analysisfile, possibly file is 
            # pending for dataset
            inbox_sfl = filemodels.StoredFileLoc.objects.filter(sfile_id=fn['pk'],
                    servershare__function=filemodels.ShareFunction.INBOX).values('pk')
            if inbox_sfl.exists():
                jobq = jm.Job.objects.filter(state=jj.Jobstates.HOLD,
                        funcname='rsync_dset_files_to_servershare',
                        kwargs__sfloc_ids=[inbox_sfl.get()['pk']])
                if jobq.exists():
                    dssid = jobq.values('kwargs__dss_id').get()['kwargs__dss_id']
                    dso = dsmodels.DatasetOwner.objects.filter(dataset__datasetserver__id=dssid
                            ).values('dataset_id', 'user__username').first()
                    it.update({'owner': dso['user__username'], 'dataset': dso['dataset_id']})
                    it['smallstatus'].append({'text': 'dataset pending', 'state': 'active'})

        if fn['rawfile__usetype'] == filemodels.UploadFileType.RAWFILE:
            if filemodels.MSFileData.objects.filter(rawfile_id=fn['rawfile_id'], success=False):
                it['smallstatus'].append({'text': 'reading failed'})
                it['actions'].extend(['purge', 'keep'])
            elif fn['pdcbackedupfile__success'] is None:
                it['actions'].extend(['backup'])

        popfiles[fn['pk']] = it
    order = [x['id'] for x in sorted(popfiles.values(), key=lambda x: x['date'], reverse=True)]
    return {'items': popfiles, 'order': order}


@login_required
@require_GET
def show_jobs(request):
    items = {}
    order = {'user': {x: [] for x in jj.JOBSTATES_ALL}, 'admin': {x: [] for x in jj.JOBSTATES_ALL}}
    if 'ids' in request.GET:
        jobids = request.GET['ids'].split(',')
        dbjobs = jm.Job.objects.filter(pk__in=jobids)
    elif userq := request.GET.get('q'):
        dbjobs = find_jobs(userq)
    else:
        dbjobs = jm.Job.objects.exclude(state__in=jj.JOBSTATES_DONE)
    for job in dbjobs.select_related('nextflowsearch__analysis__user').order_by('-timestamp'):
        ownership = jv.get_job_ownership(job, request)
        order[ownership['type']][job.state].append(job.id)
        analysis = jv.get_job_analysis(job)
        items[job.id] = {'id': job.id, 'name': job.funcname,
                         'state': job.state,
                         'canceled': job.state == jj.Jobstates.CANCELED,
                         'usr': ', '.join(ownership['usernames']),
                         'date': datetime.strftime(job.timestamp, '%Y-%m-%d'),
                         'analysis': analysis.id if analysis else False,
                         'actions': get_job_actions(job, ownership)}
        items[job.id]['fn_ids'] = [x['pk'] for x in filemodels.StoredFile.objects.filter(
            rawfile__in=job.filejob_set.all().values('rawfile')).values('pk')]
        if dss := job.kwargs.get('dss_id', False):
            dsets = dm.Dataset.objects.filter(datasetserver__pk=dss)
        elif dss := job.kwargs.get('dss_ids', False):
            dsets = dm.Dataset.objects.filter(datasetserver__pk__in=dss)
        else:
            dsets = dm.Dataset.objects.none()
        items[job.id]['dset_ids'] = [x['pk'] for x in dsets.values('pk')]
    stateorder = [
            jj.Jobstates.ERROR,
            jj.Jobstates.PROCESSING,
            jj.Jobstates.PENDING,
            jj.Jobstates.HOLD,
            jj.Jobstates.WAITING,
            jj.Jobstates.DONE,
            jj.Jobstates.CANCELED,
            ]
    return JsonResponse({'items': items, 'order': 
                         [x for u in ['user', 'admin'] for s in stateorder 
                          for x in order[u][s]]})


def parse_userquery(q, qtype):
    match qtype:
        case 'date':
            match q[-1]:
                case 'd':
                    try:
                        days = int(q[:-1])
                    except ValueError:
                        result = False
                    else:
                        result = datetime.today() - timedelta(days)
                case _:
                    match len(q):
                        case 4:
                            result = datetime.strptime(q, '%Y')
                        case 6:
                            result = datetime.strptime(q, '%Y%m')
                        case 8:
                            result = datetime.strptime(q, '%Y%m%d')
                        case _:
                            result = False
    return result


def find_jobs(userquery):
    query = Q()
    for q in [x for x in userquery.split(',') if x != '']:
        match q.split(':'):
            case ['from', date]:
                if dt := parse_userquery(date, 'date'):
                    query &= Q(timestamp__gte=dt)
            case ['to', date]:
                if dt := parse_userquery(date, 'date'):
                    query &= Q(timestamp__lte=dt)
            case ['state', 'active']:
                query &= Q(state__in=[jj.Jobstates.ERROR, *jj.JOBSTATES_WAIT])
            case ['state', 'waiting']:
                query &= Q(state__in=jj.JOBSTATES_WAIT)
            case ['state', 'queued']:
                query &= Q(state__in=jj.JOBSTATES_JOB_ACTIVE)
            case ['state', 'error']:
                query &= Q(state=jj.Jobstates.ERROR)
            case ['state', 'old']:
                query &= Q(state__in=jj.JOBSTATES_DONE)
    return jm.Job.objects.filter(query)


def get_job_actions(job, ownership):
    # FIXME define these on the Job model or wrapper
    actions = []
    jwrap = jobmap[job.funcname](job)
    if job.state in jj.JOBSTATES_RETRYABLE and (ownership['is_staff'] or ownership['owner_loggedin']):
        actions.append('retry')
    if ownership['is_staff']:
        if job.state not in [jj.Jobstates.REVOKING, jj.Jobstates.HOLD, *jj.JOBSTATES_RETRYABLE]:
            actions.append('hold')
        if jwrap.can_be_canceled and job.state in jj.JOBSTATES_PAUSABLE:
            actions.append('pause')
        elif job.state in [jj.Jobstates.WAITING, jj.Jobstates.HOLD]:
            actions.append('resume')
        if job.state == jj.Jobstates.PROCESSING:
            # Not using state==QUEUED here, because is like state == pending
            actions.append('force retry')
        if jwrap.can_be_canceled and job.state not in jj.JOBSTATES_DONE:
            actions.append('delete')
        elif job.state not in jj.JOBSTATES_DONE:
            actions.append('force delete')
    return actions


def get_ana_actions(analysis, user):
    actions = []
    if analysis['user_id'] != user.pk and not user.is_staff:
        actions.append('view')
    else:
        if analysis['editable']:
            actions.append('edit')
        else:
            actions.append('view')
        if analysis['nextflowsearch__job_id']:
            if analysis['nextflowsearch__job__state'] in jj.JOBSTATES_JOB_INACTIVE:
                actions.append('run job')
            elif analysis['nextflowsearch__job__state'] in jj.JOBSTATES_JOB_ACTIVE:
                actions.append('stop job')
    return actions


class FakeAnalysis:
    def __init__(self, name, nextflowsearch):
        self.name = name


class Throwaway:
    def __init__(self, kws):
        for kw, val in kws.items():
            setattr(self, kw, val)


def populate_analysis(analyses, user):
    ana_out, order = {}, []
    tulos_keys = ['peptides', 'proteins', 'genes', 'experiments']
    for ana in analyses.values('pk', 'name', 'user_id', 'user__username', 'date',
            'deleted', 'purged', 'editable', 'nextflowsearch__job_id', 'nextflowsearch__job__state',
            'nextflowsearch__workflow__wftype', 'nextflowsearch__workflow__name',
            'nextflowsearch__nfwfversionparamset__nfworkflow__repo',
            'nextflowsearch__nfwfversionparamset__update'):
        wf = Throwaway({'wftype': ana['nextflowsearch__workflow__wftype']})
        nfsearch = Throwaway({'workflow': wf})
        ananame = Throwaway({'name': ana['name'], 'nextflowsearch': nfsearch})
        if ana['nextflowsearch__job_id']:
            adsi = filemodels.StoredFile.objects.filter(analysisdsinputfile__dsanalysis__analysis_id=ana['pk'])
            afv = filemodels.StoredFile.objects.filter(analysisfilevalue__analysis_id=ana['pk'])
            infns = afv.union(adsi).values('pk', 'rawfile_id')
            infn_ds = dsmodels.Dataset.objects.filter(datasetrawfile__rawfile__in=[x['rawfile_id']
                for x in infns]).distinct('pk').values('pk')
            nfs = {'name': anmodels.Analysis.get_fullname(ananame),
                    'jobstate': ana['nextflowsearch__job__state'],
                    'jobid': ana['nextflowsearch__job_id'],
                    'wf': f'{ana["nextflowsearch__workflow__name"]} - {ana["nextflowsearch__nfwfversionparamset__update"]}',
                    'wflink': ana['nextflowsearch__nfwfversionparamset__nfworkflow__repo'],
                    }
        else:
            nfs = {'jobid': False, 'fn_ids': False, 'dset_ids': False, 'wflink': False}
            infn_ds, infns= [], []
        tulosq = mm.Experiment.objects.filter(analysis_id=ana['pk'])
        if tulosq.exists():
            tulos_empty = {f'{x}': [] for x in tulos_keys}
            tulos_empty['experiments'] = [[f'{tulosq.values("pk").get()["pk"]}', ana['name']]]
            tulos_filt = [tulos_empty[k] for k in tulos_keys]
            tulos_filt.extend([*['' for x in tulos_keys], *[1, 1, 1, 0], *[0, 0, 0],
                '', {'dia': True, 'dda': True}, False])
            tulos_filt = [b64encode(json.dumps(tulos_filt, separators=(',', ':')
                ).encode('utf-8')).decode('utf-8')]
        else:
            tulos_filt = False
            
        ana_out[ana['pk']] = {
            'id': ana['pk'],
            'own': ana['user_id'] == user.id,
            'usr': ana['user__username'],
            'name': nfs.get('name', ana['name']),
            'date': datetime.strftime(ana['date'], '%Y-%m-%d'),
            'deleted': ana['deleted'],
            'purged': ana['purged'],
            'dset_ids': [x['pk'] for x in infn_ds],
            'fn_ids':  [x['pk'] for x in infns],
            'mstulosq': tulos_filt,
            'actions': get_ana_actions(ana, user),
            **nfs,
        }
        order.append(ana['pk'])
    return ana_out, order


@login_required
def get_proj_info(request, proj_id):
    proj = dsmodels.Project.objects.filter(pk=proj_id).select_related('ptype', 'pi').get()
    files = filemodels.StoredFile.objects.filter(mzmlfile__isnull=True,
            rawfile__datasetrawfile__dataset__runname__experiment__project=proj)
    nr_files = files.values('filetype__name').annotate(ftcount=Count('filetype__name')).order_by()
    dsets = dsmodels.Dataset.objects.filter(runname__experiment__project=proj)
    info = {'owners': [x['datasetowner__user__username'] for x in dsets.values('datasetowner__user__username').distinct()],
            'stored_total_xbytes': getxbytes(files.aggregate(Sum('rawfile__size'))['rawfile__size__sum']),
            'nrstoredfiles': {x['filetype__name']: x['ftcount'] for x in nr_files},
            'name': proj.name,
            'pi': proj.pi.name,
            'regdate': datetime.strftime(proj.registered, '%Y-%m-%d %H:%M'),
            'type': proj.ptype.name,
            'instruments': [x['rawfile__producer__name'] for x in
                files.values('rawfile__producer__name').distinct()],
            'nrdsets': dsets.count(),
            'nrbackupfiles': filemodels.SwestoreBackedupFile.objects.filter(
                storedfile__rawfile__datasetrawfile__dataset__runname__experiment__project_id=proj_id).count() + filemodels.PDCBackedupFile.objects.filter(
                    storedfile__rawfile__datasetrawfile__dataset__runname__experiment__project_id=proj_id).count(),
        }
    return JsonResponse(info)


def populate_dset(dsids, dbdsets, user):
    dsets = OrderedDict()
    dsjobmap = defaultdict(list)
    for job in jm.Job.objects.filter(
            filejob__rawfile__datasetrawfile__dataset_id__in=dsids
            ).exclude(state__in=jj.JOBSTATES_DONE).distinct('pk').values('state', 'pk',
                    'filejob__rawfile__datasetrawfile__dataset_id'):
        dsid = job['filejob__rawfile__datasetrawfile__dataset_id']
        dsjobmap[dsid].append((str(job['pk']), job['state']))
    for dset in dbdsets.values('pk', 'deleted', 'runname__experiment__project__ptype_id', 'locked',
            'date', 'runname__experiment__name', 'runname__experiment__project__ptype__name',
            'runname__experiment__project__name', 'runname__name', 
            'runname__experiment__project_id', 'datatype__name',
            'prefractionationdataset__prefractionation__name', 'prefractionationdataset__hiriefdataset__hirief'):
        dsfiles = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=dset['pk'])
        storestate, nrfiles = get_dset_storestate(dset['pk'], dset['deleted'])
        ana_ids = [x['id'] for x in anmodels.Analysis.objects.filter(datasetanalysis__dataset_id=dset['pk']).values('id')]
        dsfiles_ids = [x['pk'] for x in dsfiles.values('pk')]
        ownerq = dsmodels.DatasetOwner.objects.filter(dataset_id=dset['pk'])
        owners = [x['user_id'] for x in ownerq.values('user_id')]
        is_owner  = check_ownership(user, dset['runname__experiment__project__ptype_id'],
                dset['deleted'], owners) 
        dsets[dset['pk']] = {
            'id': dset['pk'],
            'own': is_owner,
            'usr': ownerq.values('user__username').first()['user__username'],
            'deleted': dset['deleted'],
            'proj': dset['runname__experiment__project__name'],
            'ana_ids': ana_ids,
            'proj_ids': [dset['runname__experiment__project_id']],
            'exp': dset['runname__experiment__name'],
            'run': dset['runname__name'],
            'date': datetime.strftime(dset['date'], '%Y-%m-%d'),
            'dtype': dset['datatype__name'],
            'storestate': [storestate, nrfiles],
            'lockstate': dset['locked'],
            'fn_ids': dsfiles_ids,
            'ptype': dset['runname__experiment__project__ptype__name'],
            'prefrac': False,
            'smallstatus': [],
        }
        # Mzml/refined status
        mzmlgroups = {} 
        nrraw = dsfiles.filter(mzmlfile__isnull=True).count()
        for mzmlgroup in dsfiles.filter(mzmlfile__isnull=False).values('mzmlfile__pwiz_id', 'mzmlfile__refined', 'deleted').annotate(amount=Count('deleted')):
            if mzmlgroup['amount'] != nrraw:
                continue
            state = 'deleted' if mzmlgroup['deleted'] else 'active'
            ftypes = ['mzML', 'Refined'] if mzmlgroup['mzmlfile__refined'] else ['mzML']
            for ftype in ftypes:
                if ftype in mzmlgroups and mzmlgroups[ftype] == 'active':
                    continue
                mzmlgroups[ftype] = state
        for ftype in ['mzML', 'Refined']:
            if ftype in mzmlgroups:
                state = mzmlgroups[ftype]
                text = f'({ftype})' if state == 'deleted' else ftype
                dsets[dset['pk']]['smallstatus'].append({'text': text, 'state': state})

        # Pending files #inbox_sfl.exists() 
        dss = dsmodels.DatasetServer.objects.filter(dataset_id=dset['pk']).values('pk')
        if nrpenjobs := jm.Job.objects.filter(state=jj.Jobstates.HOLD,
                funcname='rsync_dset_files_to_servershare',
                kwargs__dss_id__in=[x['pk'] for x in dss]).count():
                # Unknowns: sfloc_ids, dstshare_id, dstsfloc_ids, cant query kwargs for those
            dsets[dset['pk']]['smallstatus'].append({'text': f'{nrpenjobs} pending raw files',
                'state': 'pending'})

        # Add job states
        dsjobs = [(jobid, state) for jobid, state in dsjobmap[dset['pk']]]
        dsets[dset['pk']]['jobstate'] = [x[1] for x in dsjobs]
        dsets[dset['pk']]['jobids'] = ','.join([x[0] for x in dsjobs])
        if dset['prefractionationdataset__prefractionation__name']:
            dsets[dset['pk']]['prefrac'] = dset['prefractionationdataset__prefractionation__name']
            if dset['prefractionationdataset__hiriefdataset__hirief']:
                dsets[dset['pk']]['hr'] = f'HiRIEF {dset["prefractionationdataset__hiriefdataset__hirief"]}'
    return dsets


# Ds page links to projects/files/analyses
# Project page links to files/analyses/datasets
# Etc CANNOT dynamically code the table too much
# Make three tables and make them share some code but not all

def get_analysis_invocation(ana):
    """Fetch parameters passed to pipeline from database. Yes, this could be fetched
    from the job, but that structure is more likely to change and things in a DB are
    more controlled. Upside of using job data would be that it doesnt get updated
    as DB data can possibly be."""

    fnmap = {}
    for x in anmodels.AnalysisFileParam.objects.filter(analysis=ana).values(
            'param__nfparam', 'sfile_id', 'sfile__filename', 'sfile__libraryfile__description', 'sfile__userfile__description',
            'sfile__analysisresultfile__analysis__name', 'sfile__analysisresultfile__analysis__nextflowsearch__id'):
        fninfo = {'fn': x['sfile__filename'],
                'fnid': x['sfile_id'],
                'desc': x['sfile__libraryfile__description'] or x['sfile__userfile__description'] or '',
                'parentanalysis': False,
                }
        # description is library file or userfile or nothing, pick
        if x['sfile__analysisresultfile__analysis__nextflowsearch__id']:
            fninfo.update({'parentanalysis': x['sfile__analysisresultfile__analysis__name'],
                    'anid': x['sfile__analysisresultfile__analysis__nextflowsearch__id'],
                    })
        try:
            fnmap[x['param__nfparam']].append(fninfo)
        except KeyError:
            fnmap[x['param__nfparam']] = [fninfo]
    invoc = {'files': [], 'multifiles': [], 'params': []}
    for param, fninfos in fnmap.items():
        invoc['files'].append({'param': param, 'multif': fninfos})
    allp_options = {}
    for x in anmodels.Param.objects.filter(ptype=anmodels.Param.PTypes.MULTI):
        for opt in x.paramoption_set.all():
            try:
                allp_options[x.nfparam][opt.id] = opt.value
            except KeyError:
                allp_options[x.nfparam] = {opt.id: opt.value}
    params = []
    for ap in anmodels.AnalysisParam.objects.select_related('param').filter(analysis=ana):
        if ap.param.ptype == anmodels.Param.PTypes.MULTI:
            vals = [allp_options[ap.param.nfparam][x] for x in ap.value]
            params.extend([ap.param.nfparam, *vals])
        elif ap.param.ptype == anmodels.Param.PTypes.FLAG and ap.value:
            params.append(ap.param.nfparam)
        else:
            params.extend([ap.param.nfparam, ap.value])

    iqparams = []
    for aiq in anmodels.AnalysisIsoquant.objects.select_related('setname').filter(analysis=ana):
        set_dsas = aiq.setname.analysisdsinputfile_set.distinct('dsanalysis').values('dsanalysis')
        qtypename = set_dsas.values('dsanalysis__dataset__quantdataset__quanttype__shortname').distinct().get()['dsanalysis__dataset__quantdataset__quanttype__shortname']
        if aiq.value['sweep']:
            calc_psm = 'sweep'
        elif aiq.value['report_intensity']:
            calc_psm = 'intensity'
        else:
            calc_psm = ':'.join([x for x, tf in aiq.value['denoms'].items() if tf])
        iqparams.append('{}:{}:{}'.format(aiq.setname.setname, qtypename, calc_psm))
    if len(iqparams):
        params.extend(['--isobaric', *iqparams])

    invoc['params'] = params
    if hasattr(ana, 'analysissampletable'):
        invoc['sampletable'] = ana.analysissampletable.samples
    else:
        invoc['sampletable'] = False
    return invoc


@login_required
def get_analysis_info(request, anid):
    ana = anmodels.Analysis.objects.filter(pk=anid).select_related('nextflowsearch__job',
        'nextflowsearch__workflow', 'nextflowsearch__nfwfversionparamset').get()
    dsets = {x.dataset for x in ana.datasetanalysis_set.all()}
    #projs = {x.runname.experiment.project for x in dsets}
    if not ana.log:
        logentry = ['Analysis without logging or not yet queued']
    else:
        logentry = [x for y in ana.log for x in y.split('\n') if x][-3:]
    errors = []
    if hasattr(ana, 'nextflowsearch'):
        try:
            errors.append(ana.nextflowsearch.job.joberror.message)
        except jm.JobError.DoesNotExist:
            pass
        for task in ana.nextflowsearch.job.task_set.filter(state=tstates.FAILURE, taskerror__isnull=False):
            # Tasks chained in a taskchain are all set to error when one errors, 
            # otherwise we cannot retry jobs since that waits for all tasks to finish.
            # This means we have to check for taskerror__isnull here
            if task.taskerror.message:
                errors.append(task.taskerror.message)
        result_parse_ok = ((hasattr(ana, 'experiment') is False or ana.experiment.upload_complete is False)
                and request.user.is_staff and
                ana.nextflowsearch.workflow.wftype == anmodels.UserWorkflow.WFTypeChoices.STD and
                ana.nextflowsearch.nfwfversionparamset.pipelineversionoutput_set.count())
        nfs_info = {'name': ana.get_fullname(), 'addToResults': result_parse_ok,
            'wf': {'fn': ana.nextflowsearch.nfwfversionparamset.filename, 
                   'name': ana.nextflowsearch.nfwfversionparamset.nfworkflow.description,
                   'update': ana.nextflowsearch.nfwfversionparamset.update,
                   'repo': ana.nextflowsearch.nfwfversionparamset.nfworkflow.repo},
            }
    else:
        nfs_info = {'name': ana.name, 'addToResults': False, 'wf': False}
    dsicount = anmodels.AnalysisDSInputFile.objects.filter(analysisset__analysis=ana).count()
    afscount = ana.analysisfilevalue_set.count()
    storeloc = filemodels.StoredFileLoc.objects.filter(sfile__analysisresultfile__analysis=ana
            ).values('servershare__name', 'path')
    linkedfiles = [(x.id, x.sfile.filename) for x in av.get_servable_files(
        ana.analysisresultfile_set.select_related('sfile'))]
    resp = {'nrdsets': len(dsets),
            'nrfiles': dsicount + afscount,
            'storage_locs': [{'share': x['servershare__name'], 'path': x['path']} for x in storeloc],
            'log': logentry, 
            'base_analysis': {'nfsid': False, 'name': False},
            'servedfiles': linkedfiles,
            'invocation': get_analysis_invocation(ana),
            'errmsg': errors if len(errors) else False,
            **nfs_info,
            }
    if anmodels.AnalysisBaseanalysis.objects.filter(analysis=ana, is_complement=True).count():
        baseana = anmodels.AnalysisBaseanalysis.objects.select_related('base_analysis').get(analysis=ana)
        old_mzml, old_dsets = aj.recurse_nrdsets_baseanalysis(baseana)
        resp['base_analysis'] = {
                'nfsid': baseana.base_analysis.nextflowsearch.pk,
                'name': baseana.base_analysis.name,
                'nrdsets': len([dset for setdsets in old_dsets.values() for dset in setdsets]),
                'nrfiles': len([fn for setfns in old_mzml.values() for fn in setfns]),
                }
    # FIXME dsets, files are already counted in the non-detailed view, so maybe frontend can reuse those
    try:
        resp['quants'] = list({x.quantdataset.quanttype.name for x in dsets})
    except dsmodels.QuantDataset.DoesNotExist:
        resp['quants'] = []
    return JsonResponse(resp)


@login_required
def refresh_job(request, job_id):
    # FIXME share with show_jobs
    job = jm.Job.objects.get(pk=job_id)
    ownership = jv.get_job_ownership(job, request)
    return JsonResponse({'state': job.state,
                         'canceled': job.state == jj.Jobstates.CANCELED,
                         'actions': get_job_actions(job, ownership)})


@login_required
def refresh_project(request, proj_id):
    dbproject = dsmodels.Project.objects.filter(pk=proj_id)
    projdict, _order = populate_proj(dbproject, request.user)
    return JsonResponse(projdict[proj_id])


@login_required
def refresh_dataset(request, dset_id):
    dbdset = dsmodels.Dataset.objects.filter(pk=dset_id)
    dsetdict = populate_dset([dset_id], dbdset, request.user)
    return JsonResponse(dsetdict[dset_id])


@login_required
def refresh_analysis(request, anid):
    ana = anmodels.Analysis.objects.filter(pk=anid)
    ana_out, _order = populate_analysis(ana, request.user)
    return JsonResponse(ana_out[anid])


@login_required
def refresh_file(request, fn_id):
    dbfns = filemodels.StoredFile.objects.filter(pk=fn_id)
    fns_out = populate_files(dbfns)
    return JsonResponse(fns_out['items'][fn_id])


@login_required
def get_job_info(request, job_id):
    job = jm.Job.objects.select_related('joberror').get(pk=job_id)
    tasks = job.task_set.all()
    fj = job.filejob_set
    analysis = jv.get_job_analysis(job)
    if analysis:
        analysis = analysis.name
    errors = []
    try:
        errors.append(job.joberror.message)
    except jm.JobError.DoesNotExist:
        pass
    for task in tasks.filter(state=tstates.FAILURE, taskerror__isnull=False):
        # Tasks chained in a taskchain are all set to error when one errors, 
        # otherwise we cannot retry jobs since that waits for all tasks to finish.
        # This means we have to check for taskerror__isnull here
        if task.taskerror.message:
            errors.append(task.taskerror.message)
    return JsonResponse({'files': fj.count(), 'dsets': 0, 
                         'analysis': analysis, 
                         'timestamp': datetime.strftime(job.timestamp, '%Y-%m-%d %H:%M:%S'),
                         'errmsg': errors if len(errors) else False,
                         'tasks': {'error': tasks.filter(state=tstates.FAILURE).count(),
                                   'procpen': tasks.filter(state=tstates.PENDING).count(),
                                   'done': tasks.filter(state=tstates.SUCCESS).count()},
                        })


@login_required
def get_dset_info(request, dataset_id):
    dset = dsmodels.Dataset.objects.filter(pk=dataset_id).select_related(
        'runname__experiment__project').get()
    info = fetch_dset_details(dset)
    return JsonResponse(info)


@login_required
def get_file_info(request, file_id):
    sfile = filemodels.StoredFile.objects.filter(pk=file_id).select_related(
        'rawfile__datasetrawfile', 'mzmlfile', 'rawfile__producer__msinstrument', 'libraryfile',
        'userfile').get()
    is_mzml = hasattr(sfile, 'mzmlfile')
    info = {'analyses': [], 'servers': [], 'storage_shares': [],
            'producer': sfile.rawfile.producer.name,
            'filename': sfile.filename,
            'renameable': False if is_mzml else True,
            'deleted': sfile.deleted,
            'ask_force_delete': False,
            'restorable': hasattr(sfile, 'pdcbackedupfile') and sfile.pdcbackedupfile.success,
            'all_storlocs': {x.id: {'name': x.name, 'id': x.pk, 'description': x.description}
                for x in sfile.get_allowed_shares()}
            }
    for x in sfile.storedfileloc_set.filter(servershare__active=True, active=True).distinct(
            'servershare__name', 'path').values('servershare__name', 'path', 'servershare_id'):
        info['servers'].append((x['servershare__name'], x['path']))
        info['storage_shares'].append(x['servershare_id'])

    if hasattr(sfile, 'libraryfile'):
        desc = sfile.libraryfile.description
    elif hasattr(sfile, 'userfile'):
        desc = sfile.userfile.description
    elif msferr := filemodels.MSFileData.objects.filter(rawfile=sfile.rawfile_id, success=False):
        desc = msferr.errmsg
    elif msf := filemodels.MSFileData.objects.filter(rawfile=sfile.rawfile_id, success=True):
        desc = f'MS time: {msf.get().mstime}'
    else:
        desc = False
    info['description'] = desc
    if hasattr(sfile.rawfile, 'datasetrawfile'):
        info['dataset'] = sfile.rawfile.datasetrawfile.dataset_id
        if is_mzml:
            anjobs = filemodels.FileJob.objects.filter(rawfile=sfile.rawfile,
                    job__nextflowsearch__isnull=False)
        elif hasattr(sfile.rawfile.producer, 'msinstrument') and not is_mzml:
            mzmls = sfile.rawfile.storedfile_set.filter(mzmlfile__isnull=False)
            anjobs = filemodels.FileJob.objects.filter(rawfile__storedfile__in=mzmls, job__nextflowsearch__isnull=False)
        else:
            anjobs = []
        info['analyses'].extend([x.job.nextflowsearch.analysis_id for x in anjobs])
    if hasattr(sfile, 'analysisresultfile') and hasattr(sfile.analysisresultfile.analysis, 'nextflowsearch'):
        info['analyses'].append(sfile.analysisresultfile.analysis.nextflowsearch.id)
    return JsonResponse(info)


def parse_mzml_pwiz(pwiz_sets, qset, numname):
    for pw in qset.annotate(nummz=Count('mzmlfile'), date=Trunc('mzmlfile__sfile__regdate', 'day')
            ).values('pk', 'active', 'mzmlfile__refined', 'version_description', 'nummz', 'date'):
        pwset_id = '{}_{}'.format(pw['pk'], pw['mzmlfile__refined'])
        try:
            pwset = pwiz_sets[pwset_id]
        except KeyError:
            pwset = {'pws_id': pwset_id, 
                    'active': pw['active'],
                    'notcreated': 0,
                    'deleted': 0,
                    'existing': 0,
                    'refined': pw['mzmlfile__refined'], 
                    'refineready': False,
                    'id': pw['pk'], 
                    'version': pw['version_description']}
        pwset[numname] += pw['nummz']
        if 'created' not in pwset:
            pwset['created'] = pw['date'].date()
        pwiz_sets[pwset['pws_id']] = pwset
    return pwiz_sets


def fetch_dset_details(dset):
    # FIXME add more datatypes and microscopy is hardcoded
    alldss = dsmodels.DatasetServer.objects.filter(dataset=dset, active=True,
            storageshare__function=filemodels.ShareFunction.RAWDATA).values('pk', 'storageshare_id')
    info = {'owners': {x.user_id: x.user.username for x in dset.datasetowner_set.select_related('user').all()},
            'allowners': {x.id: '{} {}'.format(x.first_name, x.last_name) for x in User.objects.filter(is_active=True)}, 
            'storage_shares': [x['storageshare_id'] for x in alldss],
            'all_storlocs': {x.id: {'name': x.name, 'id': x.pk, 'description': x.description}
                for x in filemodels.ServerShare.objects.filter(active=True,
                    function=filemodels.ShareFunction.RAWDATA)},
            }
    try:
        info['qtype'] = {'name': dset.quantdataset.quanttype.name, 
                         'short': dset.quantdataset.quanttype.shortname}
    except dsmodels.QuantDataset.DoesNotExist:
        info['qtype'] = False
    # FIXME Hardcoded microscopy!
    nonms_dtypes = {x.id: x.name for x in dsmodels.Datatype.objects.all()
                    if x.name in ['microscopy']}
    files = filemodels.StoredFile.objects.select_related('rawfile__producer', 'filetype').filter(
        rawfile__datasetrawfile__dataset_id=dset.id)
    info['instruments'] = list(set([x.rawfile.producer.name for x in files]))
    info['instrument_types'] = list(set([x.rawfile.producer.shortname for x in files]))
    rawfiles = files.filter(mzmlfile__isnull=True)
    # Show mzML/refine things for MS data:
    refine_dbs = filemodels.StoredFile.objects.filter(filetype__name=settings.DBFA_FT_NAME)
    if dset.datatype_id not in nonms_dtypes:
        nrstoredfiles = {'raw': rawfiles.count()}
        info.update({'refine_mzmls': [], 'convert_dataset_mzml': [], 'refine_dbs': []})
        info['refine_versions'] = [{'id': x.pk, 'name': x.update} for x in
                anmodels.NextflowWfVersionParamset.objects.filter(active=True,
                userworkflow__name__icontains='refine',
                userworkflow__wftype=anmodels.UserWorkflow.WFTypeChoices.SPEC)]
        # go through all filejobs that are not done to find current jobs and get pwiz id
        for job in jm.Job.objects.exclude(state__in=jj.JOBSTATES_DONE).filter(
                kwargs__dss_id__in=[x['pk'] for x in alldss],
                funcname__in=['refine_mzmls', 'convert_dataset_mzml']).values(
                        'funcname', 'kwargs'):
            try:
                job['kwargs']['pwiz_id']
            except KeyError:
                pass
            else:
                info[job['funcname']].append(job['kwargs']['pwiz_id'])
        pw_sets = parse_mzml_pwiz({}, anmodels.Proteowizard.objects.filter(
            mzmlfile__sfile__rawfile__datasetrawfile__dataset=dset, mzmlfile__sfile__deleted=True),
            'deleted')
        pw_sets = parse_mzml_pwiz(pw_sets, anmodels.Proteowizard.objects.filter(
            mzmlfile__sfile__rawfile__datasetrawfile__dataset=dset, 
            mzmlfile__sfile__deleted=False, mzmlfile__sfile__checked=False), 'notcreated')
        pw_sets = parse_mzml_pwiz(pw_sets, anmodels.Proteowizard.objects.filter(
            mzmlfile__sfile__rawfile__datasetrawfile__dataset=dset,
            mzmlfile__sfile__deleted=False, mzmlfile__sfile__checked=True), 'existing')
        for pwsid, pws in pw_sets.items():
            state = False
            pwpk, refined = pwsid.split('_')
            refined = refined == 'True'
            if (not refined and pws['id'] in info['convert_dataset_mzml']) or (refined and pws['id'] in info['refine_mzmls']):
                # there are jobs for this pw/refine block, either of convert or refine
                state = 'Processing'
            elif pws['existing'] == nrstoredfiles['raw']:
                # this pw/refine block is complete
                state = 'Ready'
                if not refined and f'{pwpk}_True' not in pw_sets:
                    # it's not refined and does not have another refined of same pw,
                    # so we can refine it
                    pws['refineready'] = True
                    if not len(info['refine_dbs']):
                        info['refine_dbs'] = {x.id: {'name': x.filename, 'id': x.id} for x in refine_dbs}
            elif not refined or pw_sets[f'{pwpk}_False']['existing'] == nrstoredfiles['raw']:
                # either non-refined or refined but with ok non-refined mzml
                if not refined and pws['existing'] == 0:
                    # so not refined at all
                    state = 'No mzmls'
                else:
                    # Can be either refined or not but with non-complete mzML
                    if refined and not len(info['refine_dbs']):
                        info['refine_dbs'] = {x.id: {'name': x.filename, 'id': x.id} for x in refine_dbs}
                    state = 'Incomplete'
            elif refined:
                state = 'No mzmls'
            if not state:
                raise RuntimeError('Something went wrong getting mzMLs')
            pws['state'] = state
        info['pwiz_sets'] = [x for x in pw_sets.values()]
        info['pwiz_versions'] =  {x.id: x.version_description for x in anmodels.Proteowizard.objects.exclude(
            pk__in=[x['id'] for x in info['pwiz_sets']]).exclude(active=False)}

        # Tracking pipeline
        dspipeq = cm.DatasetPipeline.objects.filter(dataset=dset)
        pipeline = {}
        if dspipeq.exists():
            dspipe = dspipeq.get()
            name = dspipeq.annotate(name=F('pipelineversion__pipeline__name'),
                    ver=F('pipelineversion__version')).values('name', 'ver').first()
            pipeline = {'dspipe_id': dspipe.pk, 'steps': [],
                    'name': f'{name["name"]} - {name["ver"]}',
                    }
            for ps in cm.PipelineStep.objects.filter(pipelineversion__datasetpipeline__dataset=dset).order_by('index').values('pk', 'step__paramopt_id', 'step__paramopt__param_id'):
                pipeline['steps'].append((ps['step__paramopt__param_id'], ps['step__paramopt_id'], ps['pk']))
            params = {}
            for p in dsmodels.SampleprepParameterOption.objects.select_related('param'):
                fill_sampleprepparam(params, p)
            pipeline.update({'prepcategories': params,
                'prepdatetrack': {cm.TrackingStages(x.stage).name: datetime.strftime(x.timestamp, '%Y-%m-%d')
                for x in cm.DatasetPrepTracking.objects.filter(dspipe__dataset=dset)},
                'prepsteptrack': {x.stage_id: x.finished for x in
                    cm.DatasetPrepTrackingNodate.objects.filter(dspipe__dataset=dset)}
                })
        info['pipeline'] = pipeline if pipeline else False

    else:
        nrstoredfiles = {nonms_dtypes[dset.datatype_id]: rawfiles.count()}
    info['nrstoredfiles'] = nrstoredfiles
    info['nrbackupfiles'] = filemodels.PDCBackedupFile.objects.filter(
        storedfile__rawfile__datasetrawfile__dataset_id=dset.id).count()
    info['compstates'] = {
dsmodels.DatasetUIComponent(x.dtcomp.component).name: x.state for x in
                          dsmodels.DatasetComponentState.objects.filter(
                              dataset_id=dset.id).select_related('dtcomp')}
    return info


@login_required
@require_POST
def create_mzmls(request):
    '''It is assumed that a dataset's files all come from the same instrument,
    and therefore need the same parameters when creating mzML files'''
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = dsmodels.Dataset.objects.get(pk=data['dsid'], deleted=False)
        pwiz = anmodels.Proteowizard.objects.get(pk=data['pwiz_id'])
    except KeyError:
        return JsonResponse({'error': 'Bad request data'}, status=400)
    except anmodels.Proteowizard.DoesNotExist:
        return JsonResponse({'error': 'Proteowizard version does not exist'}, status=400)
    except dsmodels.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist or is deleted'}, status=403)
    
    # Use proteowizard or thermorawfileparser, but must define pwiz filters and options,
    # even when trfp does not have those, dont want user to choose (yet)
    # FIXME Bruker DDA would be a case for mzML that needs to choose pwiz over trfp
    pwiz_filters = ['precursorRefine']
    pwiz_options = []
    ds_instype = dset.datasetrawfile_set.distinct('rawfile__producer__msinstrument__instrumenttype')
    if ds_instype.count() > 1:
        return JsonResponse({'error': 'Dataset contains data from multiple instrument types, cannot convert all in the same way, separate them'}, status=403)
    if ds_instype.filter(rawfile__producer__msinstrument__instrumenttype__name='timstof').exists():
        pwiz_filters.append('"scanSumming precursorTol=0.02 scanTimeTol=10 ionMobilityTol=0.1"')
        pwiz_options.append('combineIonMobilitySpectra')

    num_rawfns = filemodels.RawFile.objects.filter(datasetrawfile__dataset_id=data['dsid']).count()
    mzmls_exist_any_deleted_state = filemodels.StoredFile.objects.filter(
            rawfile__datasetrawfile__dataset=dset, checked=True, mzmlfile__isnull=False)
    mzmls_exist = mzmls_exist_any_deleted_state.filter(deleted=False, storedfileloc__active=True)
    mzmls_exist_samepw = mzmls_exist.filter(mzmlfile__pwiz=pwiz)
    if num_rawfns == mzmls_exist_samepw.count():
        return JsonResponse({'error': 'This dataset already has existing mzML files of that '
            'proteowizard version'}, status=403)
    # Get all sfl which are on a share attached to analysis server
    rawsfl = filemodels.StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset=dset,
            active=True, sfile__mzmlfile__isnull=True,
            servershare__fileservershare__server__analysisserverprofile__isnull=False,
            servershare__active=True, servershare__fileservershare__server__active=True)
    if rawsfl.count() != num_rawfns:
        return JsonResponse({'error': 'This dataset does not have (all) its raw files on a server '
            'with analysis capability, please make sure it is correctly stored'}, status=403)
    # Select possible servers
    source_ssid = rawsfl.values('servershare_id').first()['servershare_id']
    anaserver_q = filemodels.FileServer.objects.filter(fileservershare__share_id=source_ssid,
            active=True, analysisserverprofile__isnull=False)
    if not (analocalshare_q := filemodels.FileserverShare.objects.filter(server__in=anaserver_q,
            share__active=True, share__function=filemodels.ShareFunction.ANALYSISRESULTS)):
        return JsonResponse({'error': 'Analysis server does not have an output share configured'},
            status=403)

    # Saving starts here
    # Remove other pwiz mzMLs
    other_pwiz_mz = mzmls_exist.exclude(mzmlfile__pwiz=pwiz)
    if other_pwiz_mz.count():
        del_sf_pk = [x['pk'] for x in other_pwiz_mz.values('pk')]
        del_sfl_q = filemodels.StoredFileLoc.objects.filter(sfile_id__in=del_sf_pk)
        # redefine query since now all the mzmls to deleted are marked deleted=T
        for dss in dsmodels.DatasetServer.objects.filter(dataset=dset, active=True).values('pk',
                'storageshare_id'):
            sfl_pk_rm = [x['pk'] for x in del_sfl_q.filter(servershare_id=dss['storageshare_id']
                ).values('pk')]
            create_job('remove_dset_files_servershare', dss_id=dss['pk'], sfloc_ids=sfl_pk_rm)
        other_pwiz_mz.update(deleted=True)
        del_sfl_q.update(active=False)
    # Now queue the convert, then queue redistribution to other places
    # Pick one but any servershare which is reachable on an analysis server:
    # Leave out existing mzMLs in case of half-complete dataset (e.g. when 
    # broken mzMLs have been deleted by admin)
    srcsfl_pk = [x['pk'] for x in rawsfl.filter(servershare_id=source_ssid).exclude(
        sfile__rawfile__in=[x['rawfile'] for x in mzmls_exist_samepw.values('rawfile')]).values('pk')]
    dss_id = dsmodels.DatasetServer.objects.filter(dataset=dset,
            storageshare_id=source_ssid).values('pk').get()['pk']

    # we dont do error checking earlier, before deleting old mzML, because deleting those is fine
    mzjob = create_job('convert_dataset_mzml', options=pwiz_options, filters=pwiz_filters, dss_id=dss_id,
            pwiz_id=pwiz.pk, timestamp=datetime.strftime(datetime.now(), '%Y%m%d_%H.%M'),
            server_id=analocalshare_q.values('server_id').first()['server_id'], sfloc_ids=srcsfl_pk)
    if mzjob['error']:
        pass
    elif mzjob['kwargs'].get('dstsfloc_ids', False):
        dstshare_ids_dss = {x.storageshare_id: x.pk for x in  dsmodels.DatasetServer.objects.filter(
            dataset=dset, active=True)}
        share_classes = filemodels.ServerShare.classify_shares_by_rsync_reach(
                dstshare_ids_dss.keys(), mzjob['kwargs']['server_id'])
        # First put result in rsync-accessible share
        for dstshare_id in share_classes['rsync_sourcable']:
            srcjob = create_job('rsync_dset_files_to_servershare', dstshare_id=dstshare_id,
                    dss_id=dstshare_ids_dss[dstshare_id],
                    sfloc_ids=mzjob['kwargs']['dstsfloc_ids'])
            src_sflids_for_remote = srcjob['kwargs']['dstsfloc_ids']
        if share_classes['remote'] and not share_classes['rsync_sourcable']:
            # need to rsync to non-dset INBOX (as it is guaranteed to be on an rsyncing capable
            # controller (and sensitive classed)
            dstshare = filemodels.ServerShare.objects.filter(function=filemodels.ShareFunction.INBOX
                    ).values('pk').first()
            src_sflids_for_remote = []
            for sflpk in mzjob['kwargs']['dstsfloc_ids']:
                srcjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=sflpk,
                        dstshare_id=dstshare['pk'], dstpath=filemodels.ServerShare.get_inbox_path(
                            dset_id=dset.pk))
                src_sflids_for_remote.append(srcjob['kwargs']['dstsfloc_id'])
        for dstshare_id in share_classes['remote']:
            create_job('rsync_dset_files_to_servershare', dss_id=dstshare_ids_dss[dstshare_id],
                    sfloc_ids=src_sflids_for_remote, dstshare_id=dstshare_id)
    return JsonResponse({})


    



@require_POST
@login_required
def refine_mzmls(request):
    """Creates a job that runs the workflow with the latest version of the mzRefine containing NXF repo.
    Jobs and analysis entries are not created for dsets with full set of refined mzmls (403)."""
    data = json.loads(request.body.decode('utf-8'))
    try:
        dset = dsmodels.Dataset.objects.select_related('quantdataset__quanttype').get(pk=data['dsid'], deleted=False)
    except dsmodels.Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset does not exist or is deleted'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Bad request data'}, status=400)

    # TODO qe and qehf are sort of same instrument type really for MSGF (but not qehfx)
    ds_instype_q = dset.datasetrawfile_set.distinct('rawfile__producer__msinstrument__instrumenttype')
    if ds_instype_q.count() > 1:
        insts = ','.join(x.rawfile.producer.msinstrument.instrumenttype.name for x in ds_instype_q)
        return JsonResponse({'error': 'Dataset contains data from multiple instrument types: '
            f'{insts} cannot convert all in the same way, separate them'}, status=403)
    else:
        instrument = ds_instype_q.values(name=Value('rawfile__producer__msinstrument__instrumenttype__name')
                ).get()['name']

    # Check if existing normal/refined mzMLs (normal mzMLs can be deleted for this 
    # due to age, its just the number we need, but refined mzMLs should not be)
    mzmls = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset, 
            mzmlfile__isnull=False, checked=True, deleted=False)
    existing_refined = mzmls.filter(mzmlfile__refined=True, storedfileloc__active=True
            ).distinct('pk')
    normal_mzml = mzmls.filter(mzmlfile__refined=False)
    # need to distinct because join on sfl can duplicate rows
    nr_exist_mzml = normal_mzml.filter(storedfileloc__active=True).distinct('pk').count()
    nr_dsrs = dset.datasetrawfile_set.count()
    if not nr_dsrs:
        return JsonResponse({'error': 'There are no raw files in dataset'}, status=403)
    elif nr_exist_mzml < nr_dsrs:
        return JsonResponse({'error': 'Need to create normal mzMLs before refining'}, status=403)
    elif nr_exist_mzml == existing_refined.count():
        return JsonResponse({'error': 'Refined data already exists'}, status=403)

    # Check if we have files on a server with analysis
    # there should never be active sflocs of multiple pwiz versions
    mzmlsfl = filemodels.StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset=dset,
            active=True, sfile__mzmlfile__isnull=False,
            servershare__fileservershare__server__analysisserverprofile__isnull=False,
            servershare__active=True, servershare__fileservershare__server__active=True)
    # FIXME check if already exist refined files (active=True, same pipeline)
    if not mzmlsfl.count():
        return JsonResponse({'error': 'This dataset does not have its input mzML files on a server '
            'with analysis capability, please make sure it is correctly stored'}, status=403)
    # Check DB
    if dbid := data.get('dbid'):
        if not filemodels.StoredFile.objects.filter(pk=dbid, filetype__name=settings.DBFA_FT_NAME).count():
            return JsonResponse({'error': 'Wrong database to refine with'}, status=403)
    else:
        return JsonResponse({'error': 'Must pass a database to refine with'}, status=400)
    # Check WF
    if anmodels.NextflowWfVersionParamset.objects.filter(pk=data['wfid'],
           userworkflow__name__icontains='refine',
           userworkflow__wftype=anmodels.UserWorkflow.WFTypeChoices.SPEC).count() != 1:
        return JsonResponse({'error': 'Wrong workflow to refine with'}, status=403)

    # Pick one but any servershare which is reachable on an analysis server:
    srcdss = dsmodels.DatasetServer.objects.filter(dataset=dset,
            storageshare_id__in=mzmlsfl.values('servershare_id')).values('pk', 'storageshare_id').first()
    if not (anaserver_q := filemodels.FileServer.objects.filter(analysisserverprofile__isnull=False,
            active=True, fileservershare__share_id=srcdss['storageshare_id']).values('pk')):
        return JsonResponse({'error': 'Cannot find an analysis server that has access'
            'to this dataset'}, status=403)
    if anafss_q := filemodels.FileserverShare.objects.filter(server_id__in=[x['pk'] for x in anaserver_q],
            share__active=True, share__function=filemodels.ShareFunction.ANALYSISRESULTS):
        anaserver_id = anafss_q.values('server_id').first()['server_id']
    else:
        return RuntimeError('Analysis server seems to not have the output share connected')

    # Checks done, refine data, now we can start storing POST data
    # Move entire project if not on same file server (403 is checked before saving anything
    # or queueing jobs)
    # Always create new analysis - some kind of trail, otherwise you get same analysis for each
    # refining in time
    analysis = anmodels.Analysis.objects.create(user=request.user, name=f'refine_dataset_{dset.pk}',
            editable=False, securityclass=filemodels.DataSecurityClass.NOSECURITY)

    dss_id, source_ssid = srcdss['pk'], srcdss['storageshare_id']
    srcsfl_pk = [x['pk'] for x in mzmlsfl.filter(servershare_id=source_ssid).exclude(
        sfile__rawfile__in=[x['rawfile'] for x in existing_refined.values('rawfile')]).values('pk')]
    job = create_job('refine_mzmls', dss_id=dss_id, analysis_id=analysis.id, wfv_id=data['wfid'],
            sfloc_ids=srcsfl_pk, dbfn_id=dbid, qtype=dset.quantdataset.quanttype.shortname,
            instrument=instrument, anaserver_id=anaserver_id)
    if job['error']:
        analysis.delete()
        return JsonResponse({'error': 'Error trying to create a refine dataset job: {job["error"]}'},
            status=400) 
    elif job['kwargs'].get('dstsfloc_ids', False):
        dstshare_ids_dss = {x.storageshare_id: x.pk for x in  dsmodels.DatasetServer.objects.filter(
            dataset=dset, active=True)}
        share_classes = filemodels.ServerShare.classify_shares_by_rsync_reach(
                dstshare_ids_dss.keys(), job['kwargs']['server_id'])
#        # First put result in local-to-analysis-server share
#        for dstshare_id in share_classes['local']:
#            local_rsjob = create_job('rsync_dset_files_to_servershare',
#                    dss_id=dstshare_ids_dss[dstshare_id],
#                    sfloc_ids=job['kwargs']['dstsfloc_ids'], dstshare_id=dstshare_id)
        # Then put result in rsync-accessible share
        for dstshare_id in share_classes['rsync_sourcable']:
            srcjob = create_job('rsync_dset_files_to_servershare', dstshare_id=dstshare_id,
                    dss_id=dstshare_ids_dss[dstshare_id], sfloc_ids=job['kwargs']['dstsfloc_ids'])
            src_sflids_for_remote = srcjob['kwargs']['dstsfloc_ids']
        if share_classes['remote'] and not share_classes['rsync_sourcable']:
            # need to rsync to non-dset INBOX (as it is guaranteed to be on an rsyncing capable
            # controller (and sensitive classed)
            dstshare = filemodels.ServerShare.objects.filter(function=filemodels.ShareFunction.INBOX
                    ).values('pk').first()
            src_sflids_for_remote = []
            for sflpk in job['kwargs']['dstsfloc_ids']:
                srcjob = create_job('rsync_otherfiles_to_servershare', sfloc_id=sflpk,
                        dstshare_id=dstshare['pk'], dstpath=filemodels.ServerShare.get_inbox_path(
                            dset_id=dset.pk))
                src_sflids_for_remote.append(srcjob['kwargs']['dstsfloc_id'])
        for dstshare_id in share_classes['remote']:
            create_job('rsync_dset_files_to_servershare', dss_id=dstshare_ids_dss[dstshare_id],
                    sfloc_ids=src_sflids_for_remote, dstshare_id=dstshare_id)

    uwf = anmodels.UserWorkflow.objects.get(nfwfversionparamsets=data['wfid'],
            wftype=anmodels.UserWorkflow.WFTypeChoices.SPEC)
    anmodels.NextflowSearch.objects.update_or_create(analysis=analysis, defaults={
        'nfwfversionparamset_id': data['wfid'], 'job_id': job['id'], 'workflow_id': uwf.pk, 'token': ''})
    return JsonResponse({})


@login_required
def show_messages(request):
    """Shows messages for admin and possibly also for normal users"""
    # Candidate messages for admin and normal:
    # purgable files
    # analysis is done
    # refine/convert is finished
    # if we are serious about this then we need to know if the message has been read yet...
    # Im not so interested in that, because then we need to generate the messages periodically or only 
    # on completion of jobs etc.
    # Maybe three types of message: 
    #  - dynamic (resolve it and it disappear)
    #  - notification from database (remove when read) - your job is done
    #  - expiring at date - check out our new functionality, maintenance coming
    max_age_old = 30 # days
    out = {'olddef': '{} days'.format(max_age_old)}
    if request.user.is_staff:
        purgable = anmodels.Analysis.objects.select_related('nextflowsearch', 'analysisdeleted').filter(deleted=True, purged=False)
        purgable_old = purgable.filter(analysisdeleted__date__lt=datetime.today() - timedelta(max_age_old))
        if purgable:
            purgable_ana = [x.nextflowsearch.id for x in purgable if hasattr(x, 'nextflowsearch')]
        else:
            purgable_ana = False
        if purgable_old:
            purgable_ana_old = [x.nextflowsearch.id for x in purgable_old if hasattr(x, 'nextflowsearch')]
        else:
            purgable_ana_old = False
        out['purgable_analyses'] = purgable_ana
        out['old_purgable_analyses'] = purgable_ana_old
        return JsonResponse(out)
    else:
        return JsonResponse({'error': 'User is not admin'}, status=403)
