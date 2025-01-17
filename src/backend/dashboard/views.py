from django.shortcuts import render
from django.http import JsonResponse
from django.db.models import Sum, Max, F
from django.db.models.functions import Trunc, Greatest
from django.views.decorators.http import require_GET

from math import isnan

from datetime import datetime, timedelta

from analysis.models import AnalysisError
from rawstatus.models import Producer, RawFile
from datasets.models import Project, AcquisistionMode
from dashboard import models as dm
from dashboard.models import LineDataTypes as LDT
from dashboard.models import QuartileDataTypes as QDT
from kantele import settings


@require_GET
def dashboard(request):
    instruments = Producer.objects.filter(msinstrument__active=True)
    return render(request, 'dashboard/dashboard.html',
                  {'instruments': zip([x.name for x in instruments], [x.id for x in instruments]),
                  'instrument_ids': [x.id for x in instruments]})


def store_longitudinal_qc(data):
    '''Update or create new QC data'''
    try:
        data['rf_id'], data['analysis_id'], data['state'], data['msg'], data['plots']
        acqtype = data['acqtype']
    except KeyError:
        return {'error': True, 'msg': 'Missing parameter in request when storing QC data'}
    try:
        acqtype = AcquisistionMode[acqtype]
    except KeyError:
        return {'error': True, 'msg': 'Incorrect acquisition type in request storing QC data'}

    qcrun, _ = dm.QCData.objects.update_or_create(rawfile_id=data['rf_id'],
            defaults={'analysis_id': data['analysis_id'], 'is_ok': data['state'] == 'ok',
                'runtype': acqtype, 'message': data['msg']})
    dtypes = {
            'nrpsms': LDT.NRPSMS,
            'nrscans': LDT.NRSCANS,
            'nrpeptides': LDT.NRPEPTIDES,
            'nr_unique_peptides': LDT.NRPEPTIDES_UNI,
            'nrproteins': LDT.NRPROTEINS,
            'precursor_errors': QDT.MASSERROR,
            'scores': QDT.SCORE,
            'retention_times': QDT.RT,
            'peptide_areas': QDT.PEPMS1AREA,
            'ionmobilities': QDT.IONMOB,
            'fwhms': QDT.FWHM,
            'ioninj': QDT.IONINJ,
            'peaks_fwhm': LDT.PEAKS_FWHM,
            'matchedpeaks': QDT.MATCHED_PEAKS,
            'miscleav1': LDT.MISCLEAV1,
            'miscleav2': LDT.MISCLEAV2,
            }
    for qcname, qcdata in data['plots'].items():
        if qcname == 'missed_cleavages':
            for num_mc, num_psm in qcdata.items():
                if int(num_mc) > 0:
                    qcrun.lineplotdata_set.update_or_create(datatype=dtypes[f'miscleav{num_mc}'],
                            defaults={'value': num_psm})
        elif type(qcdata) == dict:
            qcrun.boxplotdata_set.update_or_create(datatype=dtypes[qcname], defaults={
                'q1': qcdata['q1'],
                'q2': qcdata['q2'],
                'q3': qcdata['q3'],
                })
        else:
            qcrun.lineplotdata_set.update_or_create(datatype=dtypes[qcname], defaults={'value': qcdata})
    return {'error': False}


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


def get_line_data(qcruns, dtypes):
    long_qc = []
    for qcrun in qcruns:
        datepoints = [{'key': lplot.datatype, 'value': lplot.value,
            'date': datetime.strftime(qcrun.date, '%Y-%m-%d %H:%M')}
        for lplot in qcrun.lineplotdata_set.filter(datatype__in=dtypes)]
        long_qc.extend(datepoints)
    return long_qc
    

def get_boxplot_data(qcruns, dtype):
    data = []
    for qcrun in qcruns.filter(boxplotdata__datatype=dtype):
        bplot = qcrun.boxplotdata_set.get(datatype=dtype)
        dayvals = {
                'key': dtype,
                'date': datetime.strftime(qcrun.date, '%Y-%m-%d %H:%M'),
                'q1': bplot.q1,
                'q2': bplot.q2,
                'q3': bplot.q3,
            }
        if not isnan(dayvals['q1']):
            data.append(dayvals)
    return data


@require_GET
def show_qc(request, acqmode, instrument_id, daysago, maxdays):
    todate = datetime.now() - timedelta(daysago - 1)
    fromdate = todate - timedelta(maxdays)
    qcruns = dm.QCData.objects.filter(rawfile__producer=instrument_id, rawfile__date__gt=fromdate, rawfile__date__lt=todate).annotate(date=F('rawfile__date')).order_by('date')
    if qcruns.count() and acqmode == 'ALL':
        runtype_q = qcruns.last().runtype
    elif acqmode != 'ALL':
        runtype_q = AcquisistionMode[acqmode]
    else:
        runtype_q = 1
    qcruns = qcruns.filter(runtype=runtype_q)
    psmdata = get_line_data(qcruns, dtypes=[LDT.NRSCANS, LDT.NRPSMS])
    miscleavdata = get_line_data(qcruns, dtypes=[LDT.MISCLEAV1, LDT.MISCLEAV2])

    totalpsms_date = {x['date']: x['value'] for x in psmdata if x['key'] == LDT.NRPSMS}
    mcratio = [{**x, 'value': x['value'] / totalpsms_date[x['date']]} for x in miscleavdata if x['key'] == LDT.MISCLEAV1]
    outjson = {'runtype': AcquisistionMode(runtype_q).label, 'seriesmap': {
        'line': {k: label for k, label in LDT.choices},
        'box': {k: label for k, label in QDT.choices},
        }, 'data': {
        'ident': get_line_data(qcruns, dtypes=[LDT.NRPEPTIDES,
            LDT.NRPROTEINS, LDT.NRPEPTIDES_UNI]),
        'psms': psmdata, 'miscleav': miscleavdata, 'mcratio': mcratio,
        'PEAKS_FWHM': get_line_data(qcruns, dtypes=[LDT.PEAKS_FWHM]),
        }}
    for key, name in zip(QDT.values, QDT.names):
        outjson['data'][name] = get_boxplot_data(qcruns, key)
    return JsonResponse(outjson)
