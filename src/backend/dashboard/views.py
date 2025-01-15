from django.shortcuts import render
from django.http import JsonResponse
from django.db.models import Sum, Max
from django.db.models.functions import Trunc, Greatest

from math import isnan

from datetime import datetime, timedelta

from analysis.models import AnalysisError
from rawstatus.models import Producer, RawFile
from datasets.models import Project, AcquisistionMode
from dashboard import models
from kantele import settings


def dashboard(request):
    instruments = Producer.objects.filter(msinstrument__active=True)
    return render(request, 'dashboard/dashboard.html',
                  {'instruments': zip([x.name for x in instruments], [x.id for x in instruments]),
                  'instrument_ids': [x.id for x in instruments]})


def store_longitudinal_qc(data):
    '''Update or create new QC data'''
    qcrun, _ = models.QCData.objects.update_or_create(rawfile_id=data['rf_id'],
            defaults={'analysis_id': data['analysis_id'], 'is_ok': data['state'] == 'ok',
                'runtype': AcquisistionMode[data['acqtype']], 'message': data['msg']})
    # TODO migrate shortnames so we are in sync with QC pipeline
    plotnames = {
            'nrpsms': 'psms',
            'nrscans': 'scans',
            'nrpeptides': 'peptides',
            'nr_unique_peptides': 'unique_peptides',
            'nrproteins': 'proteins',
            'precursor_errors': 'perror',
            'sagescores': 'msgfscore',
            'retention_times': 'rt',
            'peptide_areas': 'peparea',
            'ionmobilities': 'ionmob',
            }
    for qcname, qcdata in data['plots'].items():
        try:
            name = plotnames[qcname]
        except KeyError:
            name = qcname
        if type(qcdata) == dict and 'q1' in qcdata:
            qcrun.boxplotdata_set.update_or_create(shortname=name, defaults={
                'upper': qcdata['upper'],
                'lower': qcdata['lower'],
                'q1': qcdata['q1'],
                'q2': qcdata['q2'],
                'q3': qcdata['q3'],
                })
        elif name == 'missed_cleavages':
            for num_mc, num_psm in qcdata.items():
                qcrun.lineplotdata_set.update_or_create(shortname=f'miscleav{num_mc}', defaults={
                    'value': num_psm})
        else:
            qcrun.lineplotdata_set.update_or_create(shortname=name, defaults={'value': qcdata})


def get_file_production(request, daysago, maxdays):
    def get_from_bins(allbins, value, binsize):
        for abin in allbins:
            if value < abin:
                break
            prevbin = abin
        return prevbin + binsize / 2

    # First project sizes in DB
    # get from db: list of [(size, projtype)]
    projsizelist = [(x['sizesum'] >> 30,
        x['datasetrawfile__dataset__runname__experiment__project__projtype__ptype__name']) for x in 
        RawFile.objects.filter(producer__msinstrument__isnull=False, datasetrawfile__dataset__runname__experiment__project__active=True).values('datasetrawfile__dataset__runname__experiment__project__projtype__ptype__name', 'datasetrawfile__dataset__runname__experiment__project').annotate(sizesum=Sum('size')).order_by('sizesum')]
    lowestsize, highestsize  = projsizelist[0][0], projsizelist[-1][0]
    # Need to round until the last bin of approx size
    #if len(str(lowestsize)) < 3:
    #    firstbin = lowestsize / 10 * 10
    #else:
    #    divider = 10 ** (len(str(lowestsize)) - 1)
    #    firstbin = lowestsize / divider * divider
    #if len(str(highestsize)) < 3:
    #    lastbin = round(highestsize / 10 + 0.5) * 10
    #else:
    #    divider = 10 ** (len(str(highestsize)) - 1)
    #    lastbin = round(highestsize / divider + 0.5) * divider
    firstbin, lastbin = 0, 500
    amount_bins = 30
    binsize = (lastbin - firstbin) / float(amount_bins)
    bins = [firstbin]
    for i in range(amount_bins):
        bins.append(bins[-1] + binsize)
    projdist = {binstart + binsize / 2: {} for binstart in bins}
    for size, ptype in projsizelist:
        sizebin = get_from_bins(bins, size, binsize)
        try:
            projdist[sizebin][ptype] += 1
        except KeyError:
            projdist[sizebin][ptype] = 1
    projdist = {'xkey': 'bin', 'data': [{'bin': sizebin, **vals} for sizebin, vals in projdist.items()]}
    # CF/local RAW production by date
    todate = datetime.now() - timedelta(daysago)
    lastdate = todate - timedelta(maxdays)
    projdate = {}
    for date_proj in RawFile.objects.filter(date__gt=lastdate, date__lt=todate, producer__msinstrument__isnull=False, claimed=True).annotate(day=Trunc('date', 'day')).values('day', 'datasetrawfile__dataset__runname__experiment__project__projtype__ptype__name').annotate(sizesum=Sum('size')):
        day = datetime.strftime(date_proj['day'], '%Y-%m-%d')
        key = date_proj['datasetrawfile__dataset__runname__experiment__project__projtype__ptype__name']
        try:
            projdate[day][key] = date_proj['sizesum']
        except KeyError:
            projdate[day] = {key: date_proj['sizesum']}
    projdate = {'xkey': 'day', 'data': [{'day': day, **vals} for day, vals in projdate.items()]}

    # RAW file production per instrument
    proddate = {}
    for date_instr in RawFile.objects.filter(date__gt=lastdate, date__lt=todate, producer__msinstrument__isnull=False).annotate(day=Trunc('date', 'day')).values('day', 'producer__name').annotate(sizesum=Sum('size')):
        day = datetime.strftime(date_instr['day'], '%Y-%m-%d')
        try:
            proddate[day][date_instr['producer__name']] = date_instr['sizesum']
        except KeyError:
            proddate[day] = {date_instr['producer__name']: date_instr['sizesum']}
    instruments = {z for x in proddate.values() for z in list(x.keys())}
    for day, vals in proddate.items():
        for missing_inst in instruments.difference(vals.keys()):
            vals[missing_inst] = 0
    proddate = {'xkey': 'day', 'data': [{'day': day, **vals} for day, vals in proddate.items()]}

    # Projects age and size
    proj_age = {}
    dbprojects = Project.objects.filter(active=True).select_related('projtype__ptype').annotate(
            rawsum=Sum('experiment__runname__dataset__datasetrawfile__rawfile__size'),
            dsmax=Max('experiment__runname__dataset__date'),
            anamax=Max('experiment__runname__dataset__datasetanalysis__analysis__date')).annotate(
            greatdate=Greatest('dsmax', 'anamax'))
    
    for proj in dbprojects:
        if proj.greatdate is None or proj.rawsum is None:
            continue
        day = datetime.strftime(proj.greatdate, '%Y')
        try:
            proj_age[day][proj.projtype.ptype.name] += proj.rawsum
        except KeyError:
            try:
                proj_age[day].update({proj.projtype.ptype.name: proj.rawsum})
            except KeyError:
                proj_age[day] = {proj.projtype.ptype.name: proj.rawsum}
    proj_age = {'xkey': 'day', 'data': [{'day': day, **vals} for day, vals in proj_age.items()]}

    return JsonResponse({
        'projectdistribution': projdist,
        'fileproduction': proddate,
        'projecttypeproduction': projdate,
        'projectage': proj_age,
        })


def get_line_data(qcruns, seriesnames):
    long_qc = []
    for qcrun in qcruns:
        datepoints = [{'name': lplot.shortname, 'value': lplot.value,
        'date': datetime.strftime(qcrun.day, '%Y-%m-%d')}
        for lplot in qcrun.lineplotdata_set.filter(shortname__in=seriesnames)]
        long_qc.extend(datepoints)
    #return {'xkey': 'date', 'data': long_qc}
    return long_qc
    

def get_boxplot_data(qcruns, name):
    data = []
    for qcrun in qcruns.filter(boxplotdata__shortname=name):
        bplot = qcrun.boxplotdata_set.get(shortname=name)
        dayvals = {
                'name': bplot.shortname,
                'date': datetime.strftime(qcrun.day, '%Y-%m-%d'),
                #'upper': bplot.upper,
                #'lower': bplot.lower,
                'q1': bplot.q1,
                'q2': bplot.q2,
                'q3': bplot.q3,
            }
        if not isnan(dayvals['q1']):
            data.append(dayvals)
    return data


def show_qc(request, acqmode, instrument_id, daysago, maxdays):
    todate = datetime.now() - timedelta(daysago - 1)
    fromdate = todate - timedelta(maxdays)
    qcruns = models.QCData.objects.filter(rawfile__producer=instrument_id, rawfile__date__gt=fromdate, rawfile__date__lt=todate).annotate(day=Trunc('rawfile__date', 'day')).order_by('day')
    if qcruns.count() and acqmode == 'ALL':
        runtype_q = qcruns.last().runtype
    elif acqmode != 'ALL':
        runtype_q = AcquisistionMode[acqmode]
    else:
        runtype_q = 1
    qcruns = qcruns.filter(runtype=runtype_q)
    return JsonResponse({'runtype': AcquisistionMode(runtype_q).label, 'data': {
        'ident': get_line_data(qcruns, seriesnames=['peptides', 'proteins', 'unique_peptides']),
        'psms': get_line_data(qcruns, ['scans', 'psms', 'miscleav1', 'miscleav2']),
        'fwhm': get_boxplot_data(qcruns, 'fwhms'),
        'precursorarea': get_boxplot_data(qcruns, 'peparea'),
        'prec_error': get_boxplot_data(qcruns, 'perror'),
        'rt': get_boxplot_data(qcruns, 'rt'),
        'score': get_boxplot_data(qcruns, 'msgfscore'),
        'ionmob': get_boxplot_data(qcruns, 'ionmob'),
        }})
