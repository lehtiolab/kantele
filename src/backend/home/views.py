from datetime import timedelta, datetime
import json
from base64 import b64encode
from celery import states as tstates

from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.models import User
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden
from django.db.models import Q, Sum, Max, Count
from django.db.models.functions import Trunc, Greatest
from collections import OrderedDict, defaultdict

from kantele import settings
from datasets import models as dsmodels
from analysis import models as anmodels
from analysis import views as av
from analysis import jobs as aj
from datasets import jobs as dsjobs
from datasets.views import check_ownership, get_dset_storestate, move_dset_project_servershare
from rawstatus import models as filemodels
from rawstatus import views as rv
from jobs import jobs as jj
from jobs import views as jv
from jobs.jobutil import create_job
from jobs import models as jm
from mstulos import models as mm


@login_required
@require_GET
def home(request):
    """Returns home view with Vue apps that will separately request"""
    context = {'tab': request.GET['tab'] if 'tab' in request.GET else 'datasets',
               'dsids': request.GET['dsids'].split(',') if 'dsids' in request.GET else [],
               'anids': request.GET['anids'].split(',') if 'anids' in request.GET else [],
               'projids': request.GET['projids'].split(',') if 'projids' in request.GET else [],
               'jobids': request.GET['jobids'].split(',') if 'jobids' in request.GET else [],
               'username': request.user.username}
    return render(request, 'home/home.html', context)


@login_required
@require_GET
def find_projects(request):
    searchterms = [x for x in request.GET['q'].split(',') if x != '']
    dsquery = Q(runname__name__icontains=searchterms[0])
    dsquery |= Q(runname__experiment__name__icontains=searchterms[0])
    dsquery |= Q(runname__experiment__project__name__icontains=searchterms[0])
    query = Q(name__icontains=searchterms[0])
    for term in searchterms[1:]:
        dssubquery = Q(runname__name__icontains=term)
        dssubquery |= Q(runname__experiment__name__icontains=term)
        dssubquery |= Q(runname__experiment__project__name__icontains=term)
        subquery |= Q(name__icontains=term)
        query &= subquery
        dsquery &= dssubquery
    dbdsets = dsmodels.Dataset.objects.filter(dsquery).select_related('runname__experiment__project').values('runname__experiment__project').distinct()
    query |= Q(pk__in=dbdsets)
    dbprojects = dsmodels.Project.objects.filter(query)
    if request.GET.get('deleted') in ['false', 'False', False]:
        dbprojects = dbprojects.filter(active=True)
    items, order = populate_proj(dbprojects, request.user)
    return JsonResponse({'items': items, 'order': order})


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
    searchterms = [x for x in request.GET['q'].split(',') if x != '']
    dbdsets = dataset_query_creator(searchterms)
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
    elif request.GET.get('userproj') in ['true', 'True', True]:
        # all active projects
        dsos = dsmodels.DatasetOwner.objects.filter(user=request.user).select_related('dataset__runname__experiment')
        dbprojects = dsmodels.Project.objects.filter(pk__in={x.dataset.runname.experiment.project_id for x in dsos})
    else:
        # all active projects
        dbprojects = dsmodels.Project.objects.all()
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


@login_required
@require_GET
def find_files(request):
    """Loop through comma-separated q-param in GET, do a lot of OR queries on
    datasets to find matches. String GET-derived q-params by AND."""
    searchterms = [x for x in request.GET['q'].split(',') if x != '']
    query = Q(filename__icontains=searchterms[0])
    query |= Q(rawfile__name__icontains=searchterms[0])
    query |= Q(rawfile__producer__name__icontains=searchterms[0])
    query |= Q(path__icontains=searchterms[0])
    for term in searchterms[1:]:
        subquery = Q(filename__icontains=term)
        subquery |= Q(rawfile__name__icontains=term)
        subquery |= Q(rawfile__producer__name__icontains=term)
        subquery |= Q(path__icontains=term)
        query &= subquery
    dbfns = filemodels.StoredFile.objects.filter(query)
    if request.GET['deleted'] == 'false':
        dbfns = dbfns.filter(deleted=False, purged=False)
    return populate_files(dbfns)


@login_required
@require_GET
def show_files(request):
    if 'ids' in request.GET:
        fnids = request.GET['ids'].split(',')
        dbfns = filemodels.StoredFile.objects.filter(pk__in=fnids)
    else:
        # last week files 
        # FIXME this is a very slow query
        dbfns = filemodels.StoredFile.objects.filter(regdate__gt=datetime.today() - timedelta(7), deleted=False)
    return populate_files(dbfns)


def populate_files(dbfns):
    popfiles = {}
    for fn in dbfns.select_related(
            'rawfile__datasetrawfile__dataset', 
            'rawfile__producer__msinstrument',
            'mzmlfile',
            'analysisresultfile__analysis', 
            'swestorebackedupfile', 
            'pdcbackedupfile', 
            'filetype').filter(checked=True):
        is_mzml = hasattr(fn, 'mzmlfile')
        if hasattr(fn.rawfile.producer, 'msinstrument') and not is_mzml:
            filedate = fn.rawfile.date
        else:
            filedate = fn.regdate 
        it = {'id': fn.id,
              'name': fn.filename,
              'date': datetime.strftime(fn.rawfile.date, '%Y-%m-%d %H:%M'),
              'size': rv.getxbytes(fn.rawfile.size) if not is_mzml else '-',
              'ftype': fn.filetype.name,
              'analyses': [],
              'dataset': [],
              'jobstate': [],
              'job_ids': [],
              'deleted': fn.deleted,
              'purged': fn.purged,
              'smallstatus': [],
             }
        # TODO make unified backup model?
        try:
            it['backup'] = fn.swestorebackedupfile.success
        except filemodels.SwestoreBackedupFile.DoesNotExist:
            try:
                it['backup'] = fn.pdcbackedupfile.success
            except filemodels.PDCBackedupFile.DoesNotExist:
                it['backup'] = False
        if not fn.rawfile.claimed:
            it['owner'] = fn.rawfile.producer.name
        elif hasattr(fn.rawfile, 'datasetrawfile'):
            it['owner'] = fn.rawfile.datasetrawfile.dataset.datasetowner_set.select_related('user').first().user.username
            dsrf = fn.rawfile.datasetrawfile
            it['dataset'] = dsrf.dataset_id
            fjobs = filemodels.FileJob.objects.select_related('job').filter(storedfile_id=fn.id)
            currentjobs = fjobs.exclude(job__state__in=jj.JOBSTATES_DONE)
            it['job_ids'] = [x.job_id for x in currentjobs]
            it['jobs'] = [x.job.state for x in currentjobs]
            if is_mzml:
                anjobs = fjobs.filter(job__nextflowsearch__isnull=False)
            elif hasattr(fn.rawfile.producer, 'msinstrument'):
                mzmls = fn.rawfile.storedfile_set.filter(mzmlfile__isnull=False)
                anjobs = filemodels.FileJob.objects.filter(storedfile__in=mzmls, job__nextflowsearch__isnull=False)
            it['analyses'].extend([x.job.nextflowsearch.analysis_id for x in anjobs.select_related('job__nextflowsearch')])
        elif hasattr(fn, 'analysisresultfile'):
            it['owner'] = fn.analysisresultfile.analysis.user.username
            it['analyses'].append(fn.analysisresultfile.analysis.id)
        else:
            # Claimed file without datasetrawfile, analysisfile, possibly file is 
            # QC or pending for dataset
            if jm.Job.objects.filter(funcname='move_files_storage', state=jj.Jobstates.HOLD, 
                    kwargs__rawfn_ids=[fn.rawfile_id]):
                it['smallstatus'].append({'text': 'dataset pending', 'state': 'active'})
        popfiles[fn.id] = it
    order = [x['id'] for x in sorted(popfiles.values(), key=lambda x: x['date'], reverse=True)]
    return JsonResponse({'items': popfiles, 'order': order})



@login_required
@require_GET
def show_jobs(request):
    items = {}
    order = {'user': {x: [] for x in jj.JOBSTATES_ALL}, 'admin': {x: [] for x in jj.JOBSTATES_ALL}}
    if 'ids' in request.GET:
        jobids = request.GET['ids'].split(',')
        dbjobs = jm.Job.objects.filter(pk__in=jobids)
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
        items[job.id]['fn_ids'] = [x.storedfile_id for x in job.filejob_set.all()]
        dsets = job.kwargs.get('dset_id', job.kwargs.get('dset_ids', []))
        if type(dsets) == int:
            dsets = [dsets]
        items[job.id]['dset_ids'] = dsets
    stateorder = [jj.Jobstates.ERROR, jj.Jobstates.PROCESSING, jj.Jobstates.PENDING, jj.Jobstates.HOLD,
            jj.Jobstates.WAITING]
    return JsonResponse({'items': items, 'order': 
                         [x for u in ['user', 'admin'] for s in stateorder 
                          for x in order[u][s]]})


def get_job_actions(job, ownership):
    actions = []
    if job.state == jj.Jobstates.ERROR and (ownership['is_staff'] or ownership['owner_loggedin']) and jv.is_job_retryable_ready(job):
        actions.append('retry')
    if ownership['is_staff']:
        if job.state in jj.JOBSTATES_PAUSABLE:
            actions.append('pause')
        elif job.state == jj.Jobstates.WAITING:
            actions.append('resume')
        if job.state == jj.Jobstates.PROCESSING:
            actions.append('force retry')
        if job.state not in jj.JOBSTATES_DONE:
            actions.append('delete')
        if job.state == jj.Jobstates.PENDING:
            actions.append('pause')
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

class Throwaway:
    pass


def populate_analysis(analyses, user):
    ana_out, order = {}, []
    tulos_keys = ['peptides', 'proteins', 'genes', 'experiments']
    for ana in analyses.values('pk', 'name', 'user_id', 'user__username', 'date',
            'deleted', 'purged', 'editable', 'nextflowsearch__job_id', 'nextflowsearch__job__state',
            'nextflowsearch__workflow__wftype', 'nextflowsearch__workflow__name',
            'nextflowsearch__nfwfversionparamset__nfworkflow__repo',
            'nextflowsearch__nfwfversionparamset__update'):
        ananame = Throwaway()
        ananame.name = ana['name'] # for get_ana_fullname
        if ana['nextflowsearch__job_id']:
            fjobs = filemodels.FileJob.objects.filter(job_id=ana['nextflowsearch__job_id']).values('storedfile_id')
            fjobdsets = dsmodels.Dataset.objects.filter(datasetrawfile__rawfile__storedfile__in=[x['storedfile_id'] for x in fjobs]).distinct('pk').values('pk')
            nfs = {'name': aj.get_ana_fullname(ananame, ana['nextflowsearch__workflow__wftype']),
                    'jobstate': ana['nextflowsearch__job__state'],
                    'jobid': ana['nextflowsearch__job_id'],
                    'wf': f'{ana["nextflowsearch__workflow__name"]} - {ana["nextflowsearch__nfwfversionparamset__update"]}',
                    'wflink': ana['nextflowsearch__nfwfversionparamset__nfworkflow__repo'],
                    }
        else:
            nfs = {'jobid': False, 'fn_ids': False, 'dset_ids': False, 'wflink': False}
            fjobs, fjobdsets = [], []
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
            'dset_ids': [x['pk'] for x in fjobdsets],
            'fn_ids': [x['storedfile_id'] for x in fjobs],
            'mstulosq': tulos_filt,
            'actions': get_ana_actions(ana, user),
            **nfs,
        }
        order.append(ana['pk'])
    return ana_out, order


@login_required
def get_proj_info(request, proj_id):
    proj = dsmodels.Project.objects.filter(pk=proj_id).select_related('projtype__ptype', 'pi').get()
    files = filemodels.StoredFile.objects.select_related('rawfile__producer', 'filetype').filter(
        rawfile__datasetrawfile__dataset__runname__experiment__project=proj)
    sfiles = {}
    for sfile in files:
        try:
            sfiles[sfile.filetype.name].append(sfile)
        except KeyError:
            sfiles[sfile.filetype.name] = [sfile]
    dsets = dsmodels.Dataset.objects.filter(runname__experiment__project=proj)
    #dsowners = dsmodels.DatasetOwner.objects.filter(dataset__runname__experiment__project_id=proj_id).distinct()
    info = {'owners': [x['datasetowner__user__username'] for x in dsets.values('datasetowner__user__username').distinct()],
            'stored_total_xbytes': rv.getxbytes(files.aggregate(Sum('rawfile__size'))['rawfile__size__sum']),
            'nrstoredfiles': {ft: len([fn for fn in fns]) for ft, fns in sfiles.items()},
            'name': proj.name,
            'pi': proj.pi.name,
            'regdate': datetime.strftime(proj.registered, '%Y-%m-%d %H:%M'),
            'type': proj.projtype.ptype.name,
            'instruments': list(set([x.rawfile.producer.name for x in files])),
            'nrdsets': dsets.count(),
            'nrbackupfiles': filemodels.SwestoreBackedupFile.objects.filter(
                storedfile__rawfile__datasetrawfile__dataset__runname__experiment__project_id=proj_id).count() + filemodels.PDCBackedupFile.objects.filter(
                    storedfile__rawfile__datasetrawfile__dataset__runname__experiment__project_id=proj_id).count(),
        }
    return JsonResponse(info)


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
            'ptype': proj.projtype.ptype.name,
            'dset_ids': [x.dataset.pk for y in proj.experiment_set.all() for x in y.runname_set.all() if hasattr(x, 'dataset')],
            'details': False,
            'selected': False,
            'lastactive': datetime.strftime(proj.greatdate, '%Y-%m-%d %H:%M') if proj.greatdate else '-',
        }
    return projs, order


def populate_dset(dsids, dbdsets, user):
    dsets = OrderedDict()
    jobmap = defaultdict(dict)
    for job in jm.Job.objects.filter(
            filejob__storedfile__rawfile__datasetrawfile__dataset_id__in=dsids
            ).exclude(state__in=jj.JOBSTATES_DONE).distinct('pk').values('state', 'pk',
                    'filejob__storedfile__rawfile__datasetrawfile__dataset_id'):
        dsid = job['filejob__storedfile__rawfile__datasetrawfile__dataset_id']
        jobmap[dsid][str(job['pk'])] = job['state']
    for dset in dbdsets.values('pk', 'deleted', 'runname__experiment__project__projtype__ptype_id',
            'runname__experiment__name', 'runname__experiment__project__projtype__ptype__name',
            'runname__experiment__project__name', 'runname__name', 'datatype__name',
            'prefractionationdataset__prefractionation__name', 'prefractionationdataset__hiriefdataset__hirief'):
        dsfiles = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset_id=dset['pk'])
        storestate = get_dset_storestate(dset['pk'], dsfiles)
        ana_ids = [x['id'] for x in anmodels.Analysis.objects.filter(datasetanalysis__dataset_id=dset['pk']).values('id')]
        dsfiles_ids = [x['pk'] for x in dsfiles.values('pk')]
        ownerq = dsmodels.DatasetOwner.objects.filter(dataset_id=dset['pk'])
        owners = [x['user_id'] for x in ownerq.values('user_id')]
        is_owner  = check_ownership(user, dset['runname__experiment__project__projtype__ptype_id'],
                dset['deleted'], owners) 
        dsets[dset['pk']] = {
            'id': dset['pk'],
            'own': is_owner,
            'usr': ownerq.values('user__username').first()['user__username'],
            'deleted': dset['deleted'],
            'proj': dset['runname__experiment__project__name'],
            'ana_ids': ana_ids,
            'exp': dset['runname__experiment__name'],
            'run': dset['runname__name'],
            'dtype': dset['datatype__name'],
            'storestate': storestate,
            'fn_ids': dsfiles_ids,
            'ptype': dset['runname__experiment__project__projtype__ptype__name'],
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

        # Pending files
        if nrpenjobs := jm.Job.objects.filter(funcname='move_files_storage', state=jj.Jobstates.HOLD, 
                kwargs__dset_id=dset['pk']).count():
            dsets[dset['pk']]['smallstatus'].append({'text': f'{nrpenjobs} pending raw files',
                'state': 'pending'})

        # Add job states
        dsjobs = [(jobid, state) for jobid, state in jobmap[dset['pk']]]
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
    storeloc = filemodels.StoredFile.objects.select_related('servershare__server').filter(
            analysisresultfile__analysis=ana)
    dsets = {x.dataset for x in ana.datasetanalysis_set.all()}
    #projs = {x.runname.experiment.project for x in dsets}
    if not ana.log:
        logentry = ['Analysis without logging or not yet queued']
    else:
        logentry = [x for y in ana.log for x in y.split('\n') if x][-3:]
    linkedfiles = [(x.id, x.sfile.filename) for x in av.get_servable_files(ana.analysisresultfile_set.select_related('sfile'))]
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
        nfs_info = {'name': aj.get_ana_fullname(ana, ana.nextflowsearch.workflow.wftype),
            'addToResults': result_parse_ok,
            'wf': {'fn': ana.nextflowsearch.nfwfversionparamset.filename, 
                   'name': ana.nextflowsearch.nfwfversionparamset.nfworkflow.description,
                   'update': ana.nextflowsearch.nfwfversionparamset.update,
                   'repo': ana.nextflowsearch.nfwfversionparamset.nfworkflow.repo},
            }
    else:
        nfs_info = {'name': ana.name, 'addToResults': False, 'wf': False}
    dsicount = anmodels.AnalysisDSInputFile.objects.filter(analysisset__analysis=ana).count()
    afscount = ana.analysisfilevalue_set.count()
    resp = {'nrdsets': len(dsets),
            'nrfiles': dsicount + afscount,
            'storage_locs': [{'server': x.servershare.server.uri, 'share': x.servershare.name, 'path': x.path}
                for x in storeloc],
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
def refresh_analysis(request, anid):
    ana = anmodels.Analysis.objects.filter(pk=anid)
    ana_out, _order = populate_analysis(ana, request.user)
    return JsonResponse(ana_out[anid])


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
                         'time': datetime.strftime(job.timestamp, '%Y-%m-%d %H:%M'),
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
        'rawfile__datasetrawfile', 'mzmlfile', 'servershare',
        'rawfile__producer__msinstrument', 'analysisresultfile__analysis', 
        'libraryfile', 'userfile').get()
    is_mzml = hasattr(sfile, 'mzmlfile')
    info = {'server': sfile.servershare.name, 'path': sfile.path, 'analyses': [],
            'producer': sfile.rawfile.producer.name,
            'filename': sfile.filename,
            'renameable': False if is_mzml else True,
            }
    if hasattr(sfile, 'libraryfile'):
        desc = sfile.libraryfile.description
    elif hasattr(sfile, 'userfile'):
        desc = sfile.userfile.description
    else:
        desc = False
    info['description'] = desc
    if hasattr(sfile.rawfile, 'datasetrawfile'):
        dsrf = sfile.rawfile.datasetrawfile
        info['dataset'] = dsrf.dataset_id
        if is_mzml:
            anjobs = filemodels.FileJob.objects.filter(storedfile_id=file_id, job__nextflowsearch__isnull=False)
        elif hasattr(sfile.rawfile.producer, 'msinstrument') and not is_mzml:
            mzmls = sfile.rawfile.storedfile_set.filter(mzmlfile__isnull=False)
            anjobs = filemodels.FileJob.objects.filter(storedfile__in=mzmls, job__nextflowsearch__isnull=False)
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
    info = {'owners': {x.user_id: x.user.username for x in dset.datasetowner_set.select_related('user').all()},
            'allowners': {x.id: '{} {}'.format(x.first_name, x.last_name) for x in User.objects.filter(is_active=True)}, 
            }
    try:
        info['qtype'] = {'name': dset.quantdataset.quanttype.name, 
                         'short': dset.quantdataset.quanttype.shortname}
    except dsmodels.QuantDataset.DoesNotExist:
        info['qtype'] = False
    # FIXME Hardcoded microscopy!
    nonms_dtypes = {x.id: x.name for x in dsmodels.Datatype.objects.all()
                    if x.name in ['microscopy']}
    files = filemodels.StoredFile.objects.select_related('rawfile__producer', 'servershare', 'filetype').filter(
        rawfile__datasetrawfile__dataset_id=dset.id)
    servers = [x[0] for x in files.distinct('servershare').values_list('servershare__server__uri')]
    info['storage_loc'] = '{} - {}'.format(';'.join(servers), dset.storage_loc)
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
        for mzj in filemodels.FileJob.objects.exclude(job__state__in=jj.JOBSTATES_DONE).filter(
                storedfile__in=files, job__funcname__in=['refine_mzmls', 'convert_dataset_mzml']).distinct(
                        'job').values('job__funcname', 'job__kwargs'):
            try:
                job_pwid = int(mzj['job__kwargs']['pwiz_id'])
            except KeyError:
                pass
            else:
                info[mzj['job__funcname']].append(job_pwid)
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
    filters = ['"peakPicking true 2"', '"precursorRefine"'] # peakPick first to operate as vendor picking
    options = []
    ds_instype = dset.datasetrawfile_set.distinct('rawfile__producer__msinstrument__instrumenttype')
    if ds_instype.count() > 1:
        return JsonResponse({'error': 'Dataset contains data from multiple instrument types, cannot convert all in the same way, separate them'}, status=403)
    if ds_instype.filter(rawfile__producer__msinstrument__instrumenttype__name='timstof').exists():
        filters.append('"scanSumming precursorTol=0.02 scanTimeTol=10 ionMobilityTol=0.1"')
        options.append('combineIonMobilitySpectra')
    num_rawfns = filemodels.RawFile.objects.filter(datasetrawfile__dataset_id=data['dsid']).count()
    mzmls_exist_any_deleted_state = filemodels.StoredFile.objects.filter(
            rawfile__datasetrawfile__dataset=dset, purged=False, checked=True,
            mzmlfile__isnull=False)
    mzmls_exist = mzmls_exist_any_deleted_state.filter(deleted=False)
    if num_rawfns == mzmls_exist.filter(mzmlfile__pwiz=pwiz).count():
        return JsonResponse({'error': 'This dataset already has existing mzML files of that '
            'proteowizard version'}, status=403)

    # Saving starts here, except for the move_dset_project_servershare which checks errors
    # before saving, allowing to return a 403 here.
    # Move entire project if not on same file server
    res_share = filemodels.ServerShare.objects.get(name=settings.MZMLINSHARENAME)
    primary_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    if dset.storageshare.server != primary_share.server:
        if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
            return JsonResponse({'error': error}, status=403)
    # Remove other pwiz mzMLs
    other_pwiz_mz = mzmls_exist.exclude(mzmlfile__pwiz=pwiz)
    if other_pwiz_mz.count():
        other_pwiz_mz.update(deleted=True)
        # redefine query since now all the mzmls to deleted are marked deleted=T
        del_pwiz_q = mzmls_exist_any_deleted_state.exclude(mzmlfile__pwiz=pwiz).filter(deleted=True)
        for sf in del_pwiz_q.distinct('mzmlfile__pwiz_id').values('mzmlfile__pwiz_id'):
            create_job('delete_mzmls_dataset', dset_id=dset.pk, pwiz_id=sf['mzmlfile__pwiz_id'])
    create_job('convert_dataset_mzml', options=options, filters=filters,
            dset_id=data['dsid'], dstshare_id=res_share.pk, pwiz_id=pwiz.pk,
            timestamp=datetime.strftime(datetime.now(), '%Y%m%d_%H.%M'))
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
    ds_instype = dset.datasetrawfile_set.distinct('rawfile__producer__msinstrument__instrumenttype')
    if ds_instype.count() > 1:
        insts = ','.join(x.rawfile.producer.msinstrument.instrumenttype.name for x in ds_instype)
        return JsonResponse({'error': 'Dataset contains data from multiple instrument types: '
            f'{insts} cannot convert all in the same way, separate them'}, status=403)

    # Check if existing normal/refined mzMLs (normal mzMLs can be deleted for this 
    # due to age, its just the number we need, but refined mzMLs should not be)
    mzmls = filemodels.StoredFile.objects.filter(rawfile__datasetrawfile__dataset=dset, 
            mzmlfile__isnull=False, checked=True)
    nr_refined = mzmls.filter(mzmlfile__refined=True, deleted=False).count()
    normal_mzml = mzmls.filter(mzmlfile__refined=False)
    nr_mzml = normal_mzml.count()
    nr_exist_mzml = normal_mzml.filter(deleted=False, purged=False).count()
    nr_dsrs = dset.datasetrawfile_set.count()
    if nr_mzml and nr_mzml == nr_refined:
        return JsonResponse({'error': 'Refined data already exists'}, status=403)
    elif not nr_exist_mzml or nr_exist_mzml < nr_dsrs:
        return JsonResponse({'error': 'Need to create normal mzMLs before refining'}, status=403)
    # Check DB
    if dbid := data.get('dbid'):
        if filemodels.StoredFile.objects.filter(pk=dbid, filetype__name=settings.DBFA_FT_NAME).count() != 1:
            return JsonResponse({'error': 'Wrong database to refine with'}, status=403)
    else:
        return JsonResponse({'error': 'Must pass a database to refine with'}, status=400)
    # Check WF
    if anmodels.NextflowWfVersionParamset.objects.filter(pk=data['wfid'],
           userworkflow__name__icontains='refine',
           userworkflow__wftype=anmodels.UserWorkflow.WFTypeChoices.SPEC).count() != 1:
        return JsonResponse({'error': 'Wrong workflow to refine with'}, status=403)

    # Checks done, refine data, now we can start storing POST data
    # Move entire project if not on same file server (403 is checked before saving anything
    # or queueing jobs)
    res_share = filemodels.ServerShare.objects.get(name=settings.MZMLINSHARENAME)
    primary_share = filemodels.ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME)
    if dset.storageshare.server != primary_share.server:
        if error := move_dset_project_servershare(dset.pk, dset.storageshare.name,
                settings.PRIMARY_STORAGESHARENAME, dset.runname.experiment.project_id):
            return JsonResponse({'error': error}, status=403)
    # FIXME get analysis if it does exist, in case someone reruns?
    analysis = anmodels.Analysis.objects.create(user=request.user, name=f'refine_dataset_{dset.pk}', editable=False)
    job = create_job('refine_mzmls', dset_id=dset.pk, analysis_id=analysis.id, wfv_id=data['wfid'],
            dstshare_id=res_share.pk, dbfn_id=dbid, qtype=dset.quantdataset.quanttype.shortname)
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
