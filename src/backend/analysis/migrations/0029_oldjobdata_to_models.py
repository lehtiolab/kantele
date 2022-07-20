# Generated by Django 3.1.2 on 2020-10-21 11:48
import json
import re
from django.db import migrations


def fake_reverse(apps, sched):
    pass


def searchjobs2db(apps, sc_ed):
    Param = apps.get_model('analysis', 'Param')
    FileParam = apps.get_model('analysis', 'FileParam')
    Pset = apps.get_model('analysis', 'ParameterSet')
    Nfwf = apps.get_model('analysis', 'NextflowWfVersion')

    param_ids = {pset.pk: {param.nfparam: (param.id, param.ptype)
        for param in Param.objects.filter(psetparam__pset=pset)}
        for pset in Pset.objects.all()}
    filep_ids = {pset.pk: {fp.nfparam: fp.pk for fp in 
        FileParam.objects.filter(psetfileparam__pset=pset) | FileParam.objects.filter(psetmultifileparam__pset=pset) | FileParam.objects.filter(psetpredeffileparam__pset=pset)}
        for pset in Pset.objects.all()}
    nfwfs = {nfwf.pk: param_ids[nfwf.paramset_id] for nfwf in Nfwf.objects.all()}
    allparams = {x.nfparam: x for x in Param.objects.filter(ptype__in=['value', 'flag', 'multi'])}
    allp_options = {}
    for x in Param.objects.filter(ptype='multi'):
        for opt in x.paramoption_set.all():
            try:
                allp_options[x.nfparam][opt.value] = opt.id
            except KeyError:
                allp_options[x.nfparam] = {opt.value: opt.id}
    Job = apps.get_model('jobs', 'Job')
    Ana = apps.get_model('analysis', 'Analysis')
    ASN = apps.get_model('analysis', 'AnalysisSetname')
    SF = apps.get_model('rawstatus', 'StoredFile')
    AFS = apps.get_model('analysis', 'AnalysisFileSample')
    ADS = apps.get_model('analysis', 'AnalysisDatasetSetname')
    AnalysisSampletable = apps.get_model('analysis', 'AnalysisSampletable')
    AnalysisParam = apps.get_model('analysis', 'AnalysisParam')
    AnalysisFileParam = apps.get_model('analysis', 'AnalysisFileParam')
    AIQ = apps.get_model('analysis', 'AnalysisIsoquant')
    QTChannel = apps.get_model('datasets', 'QuantTypeChannel')
    ADSIF = apps.get_model('analysis', 'AnalysisDSInputFile')
    for job in Job.objects.filter(funcname__in=['run_ipaw_nextflow', 'run_nf_search_workflow']):
        kw = json.loads(job.kwargs)
        if job.args != '' and len(kw) == 0:
            # First gen job data
            args = json.loads(job.args)
            # if arg4 is int, it is job args with platenames in it, add 1 to indices
            if type(args[2]) == int:
                preadd, postadd = 0, 0
            elif type(args[5]) == dict:
                preadd, postadd = 1, 0
            else:
                preadd, postadd = 1, 1
            kw['analysis_id'] = args[2 + preadd]
            kw['wfv_id'] = args[3 + preadd]
            kw['inputs'] = {
                    'components': {'mzmldef': False, 'sampletable': False},
                    'params': args[4 + preadd + postadd]['params'],
                    'singlefiles': args[4 + preadd + postadd]['singlefiles'],
                    'multifiles': {},
                    }
            jobfiles = SF.objects.filter(pk__in=args[5 + preadd + postadd:]).values('pk', 'rawfile__datasetrawfile__dataset_id', 'filename')
            dsfiles = {x['pk']: x['rawfile__datasetrawfile__dataset_id'] for x in jobfiles}
            dssets = {dsid: setn for dsid, setn in zip(args[0], args[1 + preadd])}
            kw['dset_ids'] = args[0]
            kw['setnames'] = {str(sfid): dssets[dsfiles[sfid]] if dsfiles[sfid] else False for sfid in args[5 + preadd + postadd:]}
            kw['fractions'] = {x['pk']: re.sub('.*fr([0-9][0-9]).*', '\\1', x['filename']) for x in jobfiles}
        elif job.args != '' and job.kwargs != '':
            # v2: dsets in job.args
            #if 'sf_ids' in kw kw.pop('sf_ids') 
            kw['inputs']['components'] = {'mzmldef': False, 'sampletable': False}
        elif 'components' in kw and 'components' not in kw['inputs']:
            # latest older job version
            kw['inputs']['components'] = kw.pop('components')
        dset_ids = kw['dset_ids'] if 'dset_ids' in kw else kw['platenames'].keys()

        # Start fixing DB objects
        # Do not use jobs without analysis or with latest analysis
        try:
            analysis = Ana.objects.get(pk=kw['analysis_id'])
        except Ana.DoesNotExist:
            print('No analysis for job')
            continue
        if not analysis.datasetsearch_set.count() and job.state == 'done':
            raise RuntimeError('no analysis dss for {}'.format(analysis.id))
        setname_ids = {}
        for setname in set(kw['setnames'].values()):
            if setname:
                setname_ids[setname] = ASN.objects.get_or_create(analysis=analysis, setname=setname)[0].id
        files = SF.objects.filter(pk__in=kw['setnames']).values('pk', 'rawfile__datasetrawfile__dataset_id')
        if files.count() == 0:
            print('Cannot find files for dataset')
            continue
        # CHeck if datasets are multiple files/samples per dataset (e.g. IP labelfree) or not (i.e. tmt set)
        dssets = {int(x): [[], []] for x in dset_ids}
        for fn in files:
            if fn['rawfile__datasetrawfile__dataset_id'] in dssets:
                dssets[fn['rawfile__datasetrawfile__dataset_id']][0].append(fn['pk'])
                dssets[fn['rawfile__datasetrawfile__dataset_id']][1].append(kw['setnames'][str(fn['pk'])])
        for ds, (sfids, setnames) in dssets.items():
            if len(set(setnames)) > 1:
                if not analysis.analysisfileparam_set.count() and not hasattr(analysis, 'analysismzmldef'):
                    AFS.objects.bulk_create([AFS(analysis=analysis, sample=sample, sfile_id=sfid) for sfid, sample in zip(sfids, setnames)])
            elif len(setnames) > 0:
                if not analysis.analysisfileparam_set.count() and not hasattr(analysis, 'analysismzmldef'):
                    ads = ADS.objects.create(analysis=analysis, regex='.*fr([0-9][0-9]).*',
                            setname_id=setname_ids[setnames[0]], dataset_id=ds)
                else:
                    ads = ADS.objects.get(analysis=analysis, setname_id=setname_ids[setnames[0]], dataset_id=ds)
                ADSIF.objects.bulk_create([ADSIF(analysis=analysis, sfile_id=sfid, analysisdset=ads)
                    for sfid in sfids])
            elif len(setnames) == 0:
                # some datasets are lost in the analysis (files moved to another ds), make a fake set 
                if not analysis.analysisfileparam_set.count() and not hasattr(analysis, 'analysismzmldef'):
                    unknown_set, cr = ASN.objects.get_or_create(analysis=analysis,
                            setname='unknown_ds_{}'.format(ds)) 
                    ads = ADS.objects.create(analysis=analysis, regex='.*fr([0-9][0-9]).*',
                            setname=unknown_set, dataset_id=ds)
                else:
                    ads = ADS.objects.get(analysis=analysis, setname_id=setname_ids[setnames[0]], dataset_id=ds)
                ADSIF.objects.bulk_create([ADSIF(analysis=analysis, sfile_id=sfid, analysisdset=ads)
                    for sfid in sfids])
        if analysis.analysisfileparam_set.count() or hasattr(analysis, 'analysismzmldef'):
            print('Analysis already new, just added inputfiles')
            continue
        # Store params and isobaric stuff
        newiso, setdenoms = False, False
        for ix, param in enumerate(kw['inputs']['params']):
            if type(param) == str and param[:2] == '--' and param not in ['--denoms', '--instrument', '--isobaric']:
                if ix + 1 < len(kw['inputs']['params']):
                    pvals = kw['inputs']['params'][ix+1]
                    pvals = True if type(pvals) == str and pvals[:2] == '--' else pvals # flags are true
                else:
                    pvals = True
                if param in allparams:
                    p_db = allparams[param]
                else:
                    continue
                if p_db.ptype == 'multi':
                    pvals = [allp_options[param][x] for x in pvals.split(';')]
                else:
                    pvals = [pvals]
                AnalysisParam.objects.bulk_create([AnalysisParam(analysis=analysis, param=p_db, value=pval) for pval in pvals])
            elif param == '--isobaric' and ':' in kw['inputs']['params'][ix+1]:
                newiso = True
                for isop in kw['inputs']['params'][ix+1].split(' '):
                    setname, plex, *denoms = isop.split(':')
                    channels = QTChannel.objects.filter(quanttype__shortname=plex)
                    isovals = {'sweep': denoms[0] == 'sweep', 'report_intensity': False,
                            'denoms': {x.channel.name: False if x in denoms else True for x in channels}}
                    AIQ.objects.create(analysis=analysis, setname_id=setname_ids[setname], value=isovals)
            elif param == '--isobaric':
                plex = kw['inputs']['params'][ix+1]
            elif param == '--denoms':
                setdenoms = kw['inputs']['params'][ix+1]
        # For old isoquant, store denoms after going through all params since we need to know which plex is used for channels
        if not newiso and setdenoms:
            for setden in setdenoms.split(' '):
                if setden == '':
                    continue
                setname, *denoms = setden.split(':')
                channels = QTChannel.objects.filter(quanttype__shortname=plex)
                isovals = {'sweep': False, 'report_intensity': False,
                        'denoms': {x.channel.name: False if x in denoms else True for x in channels}}
                try:
                    AIQ.objects.create(analysis=analysis, setname_id=setname_ids[setname], value=isovals)
                except KeyError:
                    if analysis.id in [684, 681]:
                        pass
                    else:
                        raise

        # File input params
        sfparams = {x.nfparam: x.pk for x in FileParam.objects.all()}
        for nfparam, sfid in kw['inputs']['singlefiles'].items():
            AnalysisFileParam.objects.create(analysis=analysis, param_id=sfparams[nfparam], sfile_id=sfid)
        if 'multifiles' in kw['inputs']:
            for nfparam, sfids in kw['inputs']['multifiles'].items():
                for sfid in sfids:
                    AnalysisFileParam.objects.create(analysis=analysis, param_id=sfparams[nfparam], sfile_id=sfid)

        if 'sampletable' in kw['inputs'] and not hasattr(analysis, 'analysissampletable'):
            AnalysisSampletable.objects.create(analysis=analysis, samples=kw['inputs']['sampletable'])


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0028_auto_20210119_1344'),
    ]

    operations = [
            migrations.RunPython(searchjobs2db, fake_reverse)
    ]