from datetime import datetime
import re
import os
from uuid import uuid4

from django.utils import timezone
from django.db.models import Q

from kantele import settings
from analysis import tasks, models
from rawstatus import models as rm
from rawstatus import tasks as filetasks
from datasets import models as dm
from datasets.jobs import get_or_create_mzmlentry
from jobs.jobs import DatasetJob, SingleFileJob, BaseJob, MultiDatasetJob, MultiFileJob
from jobs import models as jm

# TODO
# rerun qc data and displaying qcdata for a given qc file, how? 
class DownloadFastaFromRepos(BaseJob):
    '''Checks ENSEMBL and uniprot if they have new versions of fasta proteome databases 
    that we havent downloaded  yet. If so, queue tasks'''
    refname = 'download_fasta_repos'
    task = tasks.check_ensembl_uniprot_fasta_download
    queue = settings.QUEUE_FILE_DOWNLOAD
    
    def process(self, **kwargs):
        self.run_tasks.append(((kwargs['db'], kwargs['version'], kwargs['organism'], 
            kwargs.get('dbtype')), {}))


class RefineMzmls(DatasetJob):
    refname = 'refine_mzmls'
    task = tasks.refine_mzmls
    queue = False
    revokable = True

    def on_create_addkwargs(self, **kwargs):
        '''Create target SFL'''
        dst_sfls = []
        for sfl in self.getfiles_query(**kwargs).select_related('sfile__mzmlfile__pwiz', 'sfile'):
            mzmlfilename = f'{os.path.splitext(sfl.sfile.filename)[0]}_refined.mzML'
            mzsf, mzsfl = get_or_create_mzmlentry(sfl, pwiz=sfl.sfile.mzmlfile.pwiz, refined=True,
                servershare_id=sfl.servershare_id, path=sfl.path, mzmlfilename=mzmlfilename)
            if mzsf:
                dst_sfls.append(mzsfl.pk)
        return {'dstsfloc_ids': dst_sfls}

    def _get_extrafiles_to_rsync(self, **kwargs):
        return [kwargs['dbfn_id']]

    def process(self, **kwargs):
        """Return all a dset mzMLs but not those that have a refined mzML associated, to not do extra work."""
        dss = dm.DatasetServer.objects.values('storageshare__name', 'storageshare_id', 'storage_loc',
                'dataset_id').get(pk=kwargs['dss_id'])
        # Pick any server capable:
        anashareserver = rm.FileserverShare.objects.filter(server__is_analysis=True,
                share_id=dss['storageshare_id']).values('server_id', 'server__name',
                        'server__scratchdir', 'path').first()
        self.queue = self.get_server_based_queue(anashareserver['server__name'], settings.QUEUE_NXF)
        sharemap = {fss['share_id']: fss['path'] for fss in
                rm.FileServer.objects.get(pk=anashareserver['server_id']).fileservershare_set.values('share_id', 'path')}

        analysis = models.Analysis.objects.get(pk=kwargs['analysis_id'])
        analysis.nextflowsearch.token = f'nf-{uuid4()}'
        analysis.nextflowsearch.save()
        nfwf = models.NextflowWfVersionParamset.objects.get(pk=kwargs['wfv_id'])

        dbfn_q = rm.StoredFileLoc.objects.filter(sfile_id=kwargs['dbfn_id'],
                servershare__fileservershare__server_id=anashareserver['server_id'])
        if dbfn_q.exists():
            dbfn = dbfn_q.values('servershare_id', 'path', 'sfile__filename').first()
        else:
            # FIXME should rsync 
            # - cannot error here, since we dont know beforehand 
            # which analysis server is being used, making that a non-actionable problem
            pass
        
        stagefiles = {'--tdb': [(os.path.join(sharemap[dbfn['servershare_id']], dbfn['path']),
            dbfn['sfile__filename'])]}

        #existing_refined = mzmlfiles.filter(sfile__mzmlfile__refined=True)
        # FIXME this in the view
        #mzml_nonrefined = mzmlfiles.exclude(sfile__rawfile__storedfile__in=existing_refined).select_related('sfile__mzmlfile__pwiz')
        timestamp = datetime.strftime(analysis.date, '%Y%m%d_%H.%M')
        runpath = re.sub('[^a-zA-Z0-9\.\-_]', '_', f'{analysis.id}_{analysis.name}_{timestamp}')
        mzmls = []
        srcpath = os.path.join(anashareserver['path'], dss['storage_loc'])
        for x in self.getfiles_query(**kwargs).values('sfile__rawfile_id', 'sfile__filename', 'path',
                ):
            ref_sfl = rm.StoredFileLoc.objects.values('pk', 'sfile__filename').get(
                    pk__in=kwargs['dstsfloc_ids'], sfile__rawfile_id=x['sfile__rawfile_id'])
            mzmls.append({'srcpath': srcpath, 'fn': x['sfile__filename'], 'refinedpk': ref_sfl['pk'],
                'refinedname': ref_sfl['sfile__filename']})
        if not mzmls:
            return
        params = ['--instrument', kwargs['instrument']]
        if kwargs['qtype'] != 'labelfree':
            params.extend(['--isobaric', kwargs['qtype']])
        run = {'timestamp': timestamp,
               'analysis_id': analysis.id,
               'token': analysis.nextflowsearch.token,
               'wf_commit': nfwf.commit,
               'nxf_wf_fn': nfwf.filename,
               'repo': nfwf.nfworkflow.repo,
               'runname':  runpath,
               'user': analysis.user.username,
               'dstsharepath': anashareserver['path'],
               'dstpath': dss['storage_loc'],
               }
        if not len(nfwf.profiles):
            profiles = ['standard', 'docker', 'lehtio']
        else:
            profiles = nfwf.profiles
        self.run_tasks.append((run, params, mzmls, stagefiles, ','.join(profiles), nfwf.nfversion,
            anashareserver['server__scratchdir']))
        # TODO replace this for general logging anyway, not necessary to keep queueing in analysis log
        analysis.log = ['[{}] Job queued'.format(datetime.strftime(timezone.now(), '%Y-%m-%d %H:%M:%S'))]
        analysis.save()


class RunLongitudinalQCWorkflow(SingleFileJob):
    refname = 'run_longit_qc_workflow'
    task = tasks.run_nextflow_longitude_qc
    queue=settings.QUEUE_QC_NXF
    revokable = True

    def process(self, **kwargs):
        """Assumes one file, one analysis"""
        analysis = models.Analysis.objects.get(pk=kwargs['analysis_id'])
        sfloc = rm.StoredFileLoc.objects.select_related('sfile__rawfile__producer', 'sfile__filetype').get(pk=kwargs['sfloc_id'])
        wf = models.UserWorkflow.objects.filter(wftype=models.UserWorkflow.WFTypeChoices.QC).last()
        nfwf = wf.nfwfversionparamsets.last()
        params = kwargs.get('params', [])
        stagefiles = {'--raw': [(sfloc.servershare.name, sfloc.path, sfloc.sfile.filename)]}
        timestamp = datetime.strftime(analysis.date, '%Y%m%d_%H.%M')
        models.NextflowSearch.objects.update_or_create(defaults={'nfwfversionparamset_id': nfwf.id, 
            'job_id': self.job_id, 'workflow_id': wf.id, 'token': f'nf-{uuid4()}'},
            analysis=analysis)
        run = {'timestamp': timestamp,
               'analysis_id': analysis.id,
               'qcrun_id': kwargs['qcrun_id'],
               'token': analysis.nextflowsearch.token,
               'wf_commit': nfwf.commit,
               'nxf_wf_fn': nfwf.filename,
               'repo': nfwf.nfworkflow.repo,
               'runname': f'{analysis.id}_longqc_{sfloc.sfile.rawfile.producer.name}_rawfile{sfloc.sfile.rawfile_id}_{timestamp}',
               }
        if kwargs['trackpeptides']:
            params.extend(['--trackedpeptides', ';'.join([f'{pep}_{ch}'
                for _, pep, ch in kwargs['trackpeptides']])])

        self.run_tasks.append(((run, params, stagefiles, ','.join(nfwf.profiles), nfwf.nfversion), {}))
        analysis.log.append('[{}] Job queued'.format(datetime.strftime(timezone.now(), '%Y-%m-%d %H:%M:%S')))
        analysis.save()


def recurse_nrdsets_baseanalysis(aba):
    """Recursively get all old mzmls from what is possibly a chain of growing analyses,
    each e.g. adding a single set fresh of the MS"""
    try:
        # if this base ana has its base ana, run the recursive func
        older_aba = models.AnalysisBaseanalysis.objects.get(
                analysis=aba.base_analysis, is_complement=True)
    except models.AnalysisBaseanalysis.DoesNotExist:
        # youve found the last base ana, dont call deeper
        old_mzmls = {}
        old_dsets = {}
    else:
        # get older analysis' old mzmls
        old_mzmls, old_dsets = recurse_nrdsets_baseanalysis(older_aba)
    # First get stripnames of old ds
    strips = {}
    for oldads in aba.base_analysis.analysisdatasetsetvalue_set.select_related('dataset__prefractionationdataset__hiriefdataset'):
        if hasattr(oldads.dataset, 'prefractionationdataset'):
            pfd = oldads.dataset.prefractionationdataset
            if hasattr(pfd, 'hiriefdataset'):
                hirief = pfd.hiriefdataset.hirief
                strips[oldads.dataset_id] = '-'.join([re.sub('.0$', '', str(float(x.strip()))) for x in str(hirief).split('-')])
            else:
                strips[oldads.dataset_id] = pfd.prefractionation.name
    # Put old files fields into the run dict, group them by set so we dont get duplicates in case an analysis chain is:
    # 1. setA + setB
    # 2. setB rerun based on 1.
    # 3. setC addition based on 2
    # This would in 3. give us all oldmzmls from 1. and 2., so setB would be double
    single_ana_oldmzml = {}
    single_ana_oldds = {}
    regexes = {x.dataset_id: x.value for x in models.AnalysisDatasetSetValue.objects.filter(
        analysis=aba.base_analysis, field='__regex')}
    for asf in models.AnalysisDSInputFile.objects.filter(
            analysisset__analysis=aba.base_analysis).select_related(
                    'sfile__rawfile__producer', 'analysisset'):
        if asf.dsanalysis.dataset_id in regexes:
            frnr = re.match(regexes[asf.dsanalysis.dataset_id], asf.sfile.filename) or False
            frnr = frnr.group(1) if frnr else 'NA'
        else:
            frnr = 'NA'
        oldasf = {'fn': asf.sfile.filename,
                'instrument': asf.sfile.rawfile.producer.name,
                'setname': asf.analysisset.setname,
                'plate': strips[asf.dsanalysis.dataset_id],
                'fraction': frnr,
                }
        try:
            single_ana_oldmzml[asf.analysisset.setname].append(oldasf)
            single_ana_oldds[asf.analysisset.setname].add(asf.dsanalysis.dataset_id)
        except KeyError:
            single_ana_oldmzml[asf.analysisset.setname] = [oldasf]
            single_ana_oldds[asf.analysisset.setname] = {asf.dsanalysis.dataset_id}
    old_mzmls.update(single_ana_oldmzml)
    old_dsets.update(single_ana_oldds)
    return old_mzmls, old_dsets


class RunNextflowWorkflow(MultiDatasetJob):
    # FIXME MultiDatasetJob for the waiting thing in runner!
    refname = 'run_nf_search_workflow'
    task = tasks.run_nextflow_workflow
    queue = False
    revokable = True

    """
    inputs is {'params': ['--isobaric', 'tmt10plex'],
               'singlefiles': {'--tdb': tdb_sf_id, ... },}
    or shoudl inputs be DB things fields flag,sf_id (how for mzmls though?)
{'params': ['--isobaric', 'tmt10plex', '--instrument', 'qe', '--hirief', '"126::set1::treat1::treat::::127::set1::treat2::treat..."
], 'mzml': ('--mzmls', '{sdir}/*.mzML'), 'singlefiles': {'--tdb': 42659, '--dbsnp': 42665, '--genome': 42666, '--snpfa': 42662, '--cosmic': 42663, '--ddb': 42664, '--blastdb': 42661, '--knownproteins': 42408, '--gtf': 42658, '--mods': 42667}}
    """

    def _get_extrafiles_to_rsync(self, **kwargs):
        return [*kwargs['inputs']['singlefiles'].values(),
                *[x for y in kwargs['inputs']['multifiles'].values() for x in y]]

    def set_error(self, job, *, errmsg):
        super().set_error(job, errmsg=errmsg)
        models.Analysis.objects.filter(pk=job.kwargs['analysis_id']).update(editable=True)

    def on_pause(self, **kwargs):
        models.Analysis.objects.filter(pk=kwargs['analysis_id']).update(editable=True)

    def process(self, **kwargs):
        analysis = models.Analysis.objects.select_related('user', 'nextflowsearch__workflow').get(pk=kwargs['analysis_id'])
        nfwf = models.NextflowWfVersionParamset.objects.select_related('nfworkflow').get(
            pk=kwargs['wfv_id'])
        fserver = rm.FileServer.objects.get(pk=kwargs['fserver_id'])
        self.queue = self.get_server_based_queue(fserver.name, settings.QUEUE_NXF)
        sharemap = {fss['share_id']: fss['path'] for fss in
                fserver.fileservershare_set.values('share_id', 'path')}
        stagefiles = {}
        for flag, sfid in kwargs['inputs']['singlefiles'].items():
            #stagefiles = {'--tdb': [(os.path.join(sharemap[dbfn['servershare_id']], dbfn['path']),
            #    dbfn['sfile__filename'])]}
            sfl_q = rm.StoredFileLoc.objects.filter(sfile_id=sfid,
                    servershare__fileservershare__server_id=kwargs['fserver_id'], active=True).values(
                            'servershare_id', 'path', 'sfile__filename')
            #.select_related('servershare', 'sfile').get(pk=sfloc_id)
            if sfl_q.exists():
                sfl = sfl_q.first()
                stagefiles[flag] = [(os.path.join(sharemap[sfl['servershare_id']], sfl['path']),
                    sfl['sfile__filename'])]
            else:
                raise RuntimeError(f'No file on analysis disk for {flag}')
        for flag, sfids in kwargs['inputs']['multifiles'].items():
            stagefiles[flag] = []
            for sfids in sfids:
                if sfl_q := rm.StoredFileLoc.objects.filter(sfile_id=sfid,
                        servershare__fileservershare__server_id=kwargs['fserver_id'],
                        active=True).values('servershare_id', 'path', 'sfile__filename'):
                    sfl = sfl_q.first()
                    stagefiles[flag].append((os.path.join(sharemap[sfl['servershare_id']], sfl['path']),
                        sfl['sfile__filename']))
                else:
                    raise RuntimeError(f'Missing file on analysis disk for {flag}')
        # re-filter dset input files in case files are removed or added to dataset
        # between a stop/error and rerun of job
        sflocs_passed = self.getfiles_query(**kwargs).values('servershare_id', 'path',
                'sfile__filename', 'sfile_id',
                'sfile__rawfile__producer__msinstrument__instrumenttype__name',
                'sfile__rawfile__datasetrawfile__dataset_id',
                'sfile__rawfile__datasetrawfile__quantfilechannel__channel__channel__name')
        is_msdata = sflocs_passed.distinct('sfile__rawfile__producer__msinstrument').count()
        job = analysis.nextflowsearch.job
        dsa = analysis.datasetanalysis_set.all()
        if dsa.filter(dataset__locked=False).exists():
            raise RuntimeError('Cannot run analysis as one or more datasets are in unlocked state')
        # First new files included:
        dsfiles_not_in_job = rm.StoredFile.objects.filter(deleted=False,
            rawfile__datasetrawfile__dataset__datasetanalysis__in=dsa).select_related(
                    'rawfile').exclude(pk__in=kwargs['infiles'].keys())
        if is_msdata:
            # Pick mzML files if the data is Mass Spec
            dsfiles_not_in_job = dsfiles_not_in_job.filter(mzmlfile__isnull=False)
        for fn_notjob in dsfiles_not_in_job:
            # check if a newer version of this file exists (e.g. mzml/refined)
            # which is instead specified in the job:
            # if fn_notjob is newer than its corresponding other file in infiles
            if fn_notjob.rawfile.storedfile_set.filter(deleted=False, pk__in=kwargs['infiles'].keys(),
                    regdate__lt=fn_notjob.regdate).count():
                # Including new files leads to problems with e.g. fraction regex
                # if they are somehow not matching 
                raise RuntimeError('Could not rerun job, there are files added to '
                    'a dataset, please edit the analysis so it is still correct, '
                    'save, and re-queue the job')

        # Now remove obsolete deleted-from-dataset files from job (e.g. corrupt, empty, etc)
        obsolete = sflocs_passed.exclude(sfile__rawfile__datasetrawfile__dataset__datasetanalysis__in=dsa).values('sfile_id')
        models.AnalysisDSInputFile.objects.filter(analysisset__analysis=analysis, sfile__in=obsolete).delete()
        analysis.analysisfilevalue_set.filter(sfile__in=obsolete).delete()
        rm.FileJob.objects.filter(job_id=job.pk, storedfile__in=obsolete).delete()
        for del_sf in obsolete:
            # FIXME setnames/frac is specific
            kwargs['setnames'].pop(str(del_sf['sfile_id']))
            kwargs['infiles'].pop(str(del_sf['sfile_id']))
        if obsolete.exists():
            job.kwargs = kwargs
            job = job.save()

        # token is unique per job run:
        analysis.nextflowsearch.token = f'nf-{uuid4()}'
        analysis.nextflowsearch.save()
        run = {'analysis_id': analysis.id,
               'token': analysis.nextflowsearch.token,
               'wf_commit': nfwf.commit,
               'nxf_wf_fn': nfwf.filename,
               'repo': nfwf.nfworkflow.repo,
               'runname': analysis.get_run_base_dir(),
               'outdir': analysis.user.username,
               'infiles': [],
               'old_infiles': False,
               'server_id': fserver.pk,
               'components': kwargs['inputs']['components'],
               }
        
        # Gather input files
        infiles = []
        # INPUTDEF is either False or [fn, set, fraction, etc]
        if inputdef_fields := run['components']['INPUTDEF']:
            for fn in sflocs_passed:
                infile = {'path': os.path.join(sharemap[fn['servershare_id']], fn['path']),
                        'fn': fn['sfile__filename']}
                if 'setname' in inputdef_fields:
                    infile['setname'] = kwargs['filesamples'].get(str(fn['sfile_id']), '')
                if 'plate' in inputdef_fields:
                    infile['plate'] = kwargs['platenames'].get(str(fn['sfile__rawfile__datasetrawfile__dataset_id']), '')
                if 'sampleID' in inputdef_fields:
                    # sampleID is for pgt / dbgenerator
                    # No fallback, is required if in header
                    infile['sampleID'] = kwargs['filesamples'][str(fn['sfile_id'])]
                if 'fraction' in inputdef_fields:
                    infile['fraction'] = kwargs['infiles'].get(str(fn['sfile_id']), {}).get('fr') 
                if 'instrument' in inputdef_fields:
                    # No fallback, instrument in header cannot be ''
                    infile['instrument'] = fn['sfile__rawfile__producer__msinstrument__instrumenttype__name'] 
                if 'channel' in inputdef_fields:
                    # For non-pooled labelcheck, cannot be ''
                    infile['channel'] = fn['sfile__rawfile__datasetrawfile__quantfilechannel__channel__channel__name']
                # Dynamic fields
                infile.update(kwargs['filefields'].get(str(fn['sfile_id']), {}))
                infiles.append(infile)
        # FIXME bigrun not hardcode, probably need to remove when new infra
        shortname = models.UserWorkflow.WFTypeChoices(analysis.nextflowsearch.workflow.wftype).name
        bigrun = shortname == 'PISEP' or len(infiles) > 500

        # COMPLEMENT/RERUN component:
        # Add base analysis stuff if it is complement and fractionated (if not it has only been used
        # for fetching parameter values and can be ignored in the job)
        ana_baserec = models.AnalysisBaseanalysis.objects.select_related('base_analysis').filter(analysis_id=analysis.id)
        try:
            ana_baserec = ana_baserec.get(Q(is_complement=True) | Q(rerun_from_psms=True))
        except models.AnalysisBaseanalysis.DoesNotExist:
            # Run with normal mzmldef input
            run['infiles'] = infiles
        else:
            # SELECT prefrac with fraction regex to get fractionated datasets in old analysis
            if ana_baserec.base_analysis.analysisdatasetsetvalue_set.filter(field='__regex').count():
                # rerun/complement runs with fractionated base analysis need --oldmzmldef parameter
                old_infiles, old_dsets = recurse_nrdsets_baseanalysis(ana_baserec)
                run['old_infiles'] = ['{}\t{}'.format(x['fn'], '\t'.join([x[key] for key in run['components']['INPUTDEF'][1:]]))
                        for setmzmls in old_infiles.values() for x in setmzmls]
            if not ana_baserec.rerun_from_psms:
                # Only mzmldef input if not doing a rerun
                run['infiles'] = infiles

        if not len(nfwf.profiles):
            profiles = ['standard', 'docker', 'lehtio']
        else:
            profiles = nfwf.profiles
        params = [str(x) for x in kwargs['inputs']['params']]
        # Runname defined when run executed (FIXME can be removed, no reason to not do that here)
        # RunID is probably only used in a couple of pipelines but it's nice to use "our" analysis ID here
        # and needs to be coupled here, cannot have user make it
        params.extend(['--name', 'RUNNAME__PLACEHOLDER', '--runid', f'run_{analysis.pk}'])
        self.run_tasks.append((run, params, {}, stagefiles, ','.join(profiles), nfwf.nfversion, fserver.scratchdir))

        analysis.log.append('[{}] Job queued'.format(datetime.strftime(timezone.now(), '%Y-%m-%d %H:%M:%S')))
        analysis.save()


class PurgeAnalysis(MultiFileJob):
    refname = 'purge_analysis'
    task = filetasks.delete_file
    queue = settings.QUEUE_STORAGE

    """Queues tasks for deleting files from analysis from disk, then queues 
    job for directory removal"""

    def getfiles_query(self, **kwargs):
        return super().getfiles_query(**kwargs).values('path', 'sfile__filename', 'servershare',
                'servershare__name', 'sfile_id')

    def process(self, **kwargs):
        webshare = rm.ServerShare.objects.get(name=settings.WEBSHARENAME)
        for fn in self.getfiles_query(**kwargs):
            fullpath = os.path.join(fn['path'], fn['sfile__filename'])
            print(f'Purging {fullpath} from analysis {kwargs["analysis_id"]}')
            if fn['servershare'] != webshare.pk:
                # Files on web share live locally, deleted by the purge view itself
                self.run_tasks.append(((fn['servershare__name'], fullpath, fn['sfile_id']), {}))
