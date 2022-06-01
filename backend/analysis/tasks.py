import os
import json
import shutil
import requests
import subprocess
from time import sleep
from datetime import datetime
from urllib.parse import urljoin, urlsplit
from ftplib import FTP
from dulwich.porcelain import clone, reset, pull
from tempfile import NamedTemporaryFile
from gzip import GzipFile

from django.urls import reverse
from django.utils import timezone
from celery import shared_task

from jobs.post import update_db, taskfail_update_db
from kantele import settings
from rawstatus.tasks import calc_md5


@shared_task(bind=True, queue=settings.QUEUE_PXDOWNLOAD)
def check_ensembl_uniprot_fasta_download(self):
    """Checks if there is a new version of ENSEMBL data,
    downloads it to system over FTP"""
    def register_transfer_libfile(regresp, dstfn, description, ftype_id):
        # register transfer, will fire md5 check and create libfile of it
        postdata = {'fn_id': regresp['file_id'],
                    'client_id': settings.ADMIN_APIKEY,
                    'ftype_id': ftype_id,
                    'filename': dstfn,
                    }
        resp = requests.post(url=urljoin(settings.KANTELEHOST, reverse('files:transferred')), data=postdata)
        resp.raise_for_status()
        postdata = {'fn_id': regresp['file_id'],
                    'client_id': settings.ADMIN_APIKEY,
                    'desc': description,
                    }
        resp = requests.post(url=urljoin(settings.KANTELEHOST, reverse('files:setlibfile')), data=postdata)
        resp.raise_for_status()
        finished = False
        while True:
            r = requests.get(url=urljoin(settings.KANTELEHOST, 
                reverse('files:checklibfile')), params={'fn_id': regresp['file_id']})
            r.raise_for_status()
            if r.json()['ready']:
                break
            print('Libfile not finished yet, checking in 10 sec')
            sleep(10)

    def register_and_copy_lib_fasta_db(dstfn, wfp):
        postdata = {'fn': dstfn,
                    'client_id': settings.ADMIN_APIKEY,
                    'md5': calc_md5(wfp.name),
                    'size': os.path.getsize(wfp.name),
                    'date': str(os.path.getctime(wfp.name)),
                    'claimed': True,
                    }
        resp = requests.post(url=urljoin(settings.KANTELEHOST, reverse('files:register')), data=postdata)
        try:
            resp.raise_for_status()
        except:
            self.retry(countdown=3600)
        regresp = resp.json()
        # edgecase: if we already have this file due to parallel download, dont proceed.
        # otherwise, copy it to tmp
        if not regresp['stored']:
            shutil.copy(wfp.name, os.path.join(settings.SHAREMAP[settings.TMPSHARENAME], dstfn))
            os.chmod(os.path.join(settings.SHAREMAP[settings.TMPSHARENAME], dstfn), 0o640)
        return regresp
    # TODO check sum(fn) to validate ENSEMBL checksum
    # First check ENSEMBL and uniprot
    r = requests.get(settings.ENSEMBL_API, headers={'Content-type': 'application/json'})
    ens_version = r.json()['release'] if r.ok else ''
    r = requests.get(settings.UNIPROT_API.format(settings.UP_ORGS['Homo sapiens'], ''), stream=True)
    up_version = r.headers['X-UniProt-Release'] if r.ok else ''
    # verify releases with Kantele
    dbstates = requests.get(url=urljoin(settings.KANTELEHOST, reverse('analysis:checkfastarelease')),
            params={'ensembl': ens_version, 'uniprot': up_version}).json()['dbstates']
    done_url = urljoin(settings.KANTELEHOST, reverse('analysis:setfastarelease'))
    for dbstate in dbstates:
        if dbstate['state']:
            print('{} - {} version {} is already stored'.format(dbstate['db'], dbstate['organism'], dbstate['version']))
            continue
        else:
            print('Downloading {} - {} version {}'.format(dbstate['db'], dbstate['organism'], dbstate['version']))
        if dbstate['db'] == 'uniprot':
            with requests.get(settings.UNIPROT_API.format(settings.UP_ORGS[dbstate['organism']], '&include=yes' if dbstate['isoforms'] else ''),
                    stream=True) as req, NamedTemporaryFile(mode='wb') as wfp:
                for chunk in req.iter_content(chunk_size=8192):
                    if chunk:
                        wfp.write(chunk)
                dstfn = 'Swissprot_{}_{}_canonical{}.fa'.format(dbstate['organism'].replace(' ', '_'), dbstate['version'], '_isoforms' if dbstate['isoforms'] else '')
                regresp = register_and_copy_lib_fasta_db(dstfn, wfp)
            register_transfer_libfile(regresp, dstfn,
                    'Uniprot {} release {} swiss canonical{}.fasta'.format(dbstate['organism'], dbstate['version'], '/isoforms' if dbstate['isoforms'] else ''),
                    settings.DATABASE_FTID)
            requests.post(done_url,
                    json={'type': 'uniprot', 'version': dbstate['version'],
                        'organism': dbstate['organism'], 'isoforms': dbstate['isoforms'],
                        'fn_id': regresp['file_id']})
        elif dbstate['db'] == 'ensembl':
            # Download db, use FTP to get file, download zipped via HTTPS and unzip in stream
            url = urlsplit(settings.ENSEMBL_DL_URL.format(ens_version, dbstate['organism'].lower().replace(' ', '_')))
            with FTP(url.netloc) as ftp:
                ftp.login()
                fn = [x for x in ftp.nlst(url.path) if 'pep.all.fa.gz' in x][0]
            # make sure to specify no compression (identity) to server, otherwise the raw object will be gzipped and when unzipped contain a gzipped file
            with requests.get(urljoin('http://' + url.netloc, fn), headers={'Accept-Encoding': 'identity'}, stream=True).raw as reqfp:
                with NamedTemporaryFile(mode='wb') as wfp, GzipFile(fileobj=reqfp) as gzfp:
                    for line in gzfp:
                        wfp.write(line)
                    dstfn = 'ENS{}_{}'.format(ens_version, os.path.basename(fn).replace('.gz', ''))
                    # Now register download in Kantele, still in context manager
                    # since tmp file will be deleted on close()
                    regresp = register_and_copy_lib_fasta_db(dstfn, wfp)
            register_transfer_libfile(regresp, dstfn, 'ENSEMBL {} release {} pep.all fasta'.format(dbstate['organism'], dbstate['version']),
                    settings.DATABASE_FTID)
        # Download biomart for ENSEMBL
#        martxml = '<?xml version="1.0" encoding="UTF-8"?> <!DOCTYPE Query> <Query  header="1" completionStamp="1" virtualSchemaName = "default" formatter = "TSV" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" > <Dataset name = "hsapiens_gene_ensembl" interface = "default" > <Attribute name = "ensembl_gene_id" /> <Attribute name = "ensembl_peptide_id" /> <Attribute name = "description" /> <Attribute name = "external_gene_name" /> </Dataset> </Query>'
#        with requests.get(settings.BIOMART_URL, params={'query': martxml}, stream=True) as reqfp, NamedTemporaryFile(mode='wb') as wfp:
#            for chunk in reqfp.iter_content(chunk_size=8192):
#                if chunk:
#                    wfp.write(chunk)
#            dstfn = 'Biomart_table_ENS{}'.format(ens_version)
#            # Now register download in Kantele, still in context manager
#            # since tmp file will be deleted on close()
#            regresp = register_and_copy_lib_fasta_db(dstfn, wfp)
#        register_transfer_libfile(regresp, dstfn, 'Biomart map for ENSEMBL release {}'.format(ens_version),
#                settings.MARTMAP_FTID)
            requests.post(done_url, json={'type': 'ensembl', 'version': dbstate['version'], 
                        'organism': dbstate['organism'], 'fn_id': regresp['file_id']})
        print('Finished downloading {} {} version {} database'.format(dbstate['db'], dbstate['organism'], dbstate['version']))
        


def run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version):
    """Fairly generalized code for kantele celery task to run a WF in NXF"""
    print('Starting nextflow workflow {}'.format(run['nxf_wf_fn']))
    outdir = os.path.join(rundir, 'output')
    try:
        clone(run['repo'], gitwfdir, checkout=run['wf_commit'])
    except FileExistsError:
        pull(gitwfdir, run['repo'])
        reset(gitwfdir, 'hard', run['wf_commit'])
    # FIXME dulwich does not seem to checkout anything, use this until it does
    subprocess.run(['git', 'checkout', run['wf_commit']], check=True, cwd=gitwfdir)
    print('Checked out repo {} at commit {}'.format(run['repo'], run['wf_commit']))
    # There will be files inside data dir of WF repo so we must be in
    # that dir for WF to find them
    cmd = ['nextflow', 'run', run['nxf_wf_fn'], *params, '--outdir', outdir, '-profile', profiles, '-with-trace', '-resume']
    env = os.environ
    env['NXF_VER'] = nf_version
    if 'token' in run and run['token']:
        nflogurl = urljoin(settings.KANTELEHOST, reverse('analysis:nflog'))
        cmd.extend(['-name', run['token'], '-with-weblog', nflogurl])
    if 'analysis_id' in run:
        log_analysis(run['analysis_id'], 'Running command {}, nextflow version {}'.format(' '.join(cmd), env.get('NXF_VER', 'default')))
    subprocess.run(cmd, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=gitwfdir, env=env)
    return rundir


def get_dir_content_size(fpath):
    return sum([os.path.getsize(f) for f in os.listdir(fpath) if os.path.isfile(f)])


def stage_files(stagedir, stagefiles, params=False):
    for flag, files in stagefiles.items():
        stagefiledir = os.path.join(stagedir, flag.replace('--', ''))
        if not os.path.exists(stagefiledir):
            os.makedirs(stagefiledir)
        not_needed_files = set(os.listdir(stagefiledir))
        if len(files) > 1:
            dst = os.path.join(stagefiledir, '*')
        else:
            dst = os.path.join(stagefiledir, files[0][2])
        if params:
            params.extend([flag, dst])
        # now actually copy
        for fdata in files:
            fpath = os.path.join(settings.SHAREMAP[fdata[0]], fdata[1], fdata[2])
            fdst = os.path.join(stagefiledir, fdata[2])
            try:
                not_needed_files.remove(fdata[2])
            except KeyError:
                pass
            if os.path.exists(fdst) and not os.path.isdir(fpath) and os.path.getsize(fdst) == os.path.getsize(fpath):
                # file already there
                continue
            elif os.path.exists(fdst) and os.path.isdir(fpath) and get_dir_content_size(fpath) == get_dir_content_size(fdst):
                # file is a dir but also ready
                continue
            elif os.path.exists(fdst):
                # file/dir exists but is not correct, delete first
                os.remove(fdst)
            if os.path.isdir(fpath):
                shutil.copytree(fpath, fdst)
            else:
                shutil.copy(fpath, fdst)
        # Remove obsolete files from stage-file-dir
        for fn in not_needed_files:
            fpath = os.path.join(stagefiledir, fn)
            if os.path.isdir(fpath):
                shutil.rmtree(fpath)
            else:
                os.remove(fpath)
    return params


def log_analysis(analysis_id, message):
    logurl = urljoin(settings.KANTELEHOST, reverse('analysis:appendlog'))
    update_db(logurl, json={'analysis_id': analysis_id, 'message': message})


@shared_task(bind=True, queue=settings.QUEUE_NXF)
def run_nextflow_workflow(self, run, params, stagefiles, profiles, nf_version):
    print('Got message to run nextflow workflow, preparing')
    postdata = {'client_id': settings.APIKEY,
                'analysis_id': run['analysis_id'], 'task': self.request.id,
                'name': run['name'], 'user': run['outdir']}
    rundir = create_runname_dir(run)
    # write sampletable if it is present
    sampletable = False
    try:
        st_ix = params.index('SAMPLETABLE')
    except ValueError:
        pass
    else:
        sampletable = params[st_ix + 1]
        params = params[0:st_ix] + params[st_ix + 2:]
        print('Sampletable found')
    # stage files, create dirs etc
    params, gitwfdir, stagedir = prepare_nextflow_run(run, self.request.id, rundir, stagefiles,
            run['mzmls'], params)
    if sampletable:
        sampletable_fn = os.path.join(rundir, 'sampletable.txt')
        with open(sampletable_fn, 'w') as fp:
            for sample in sampletable:
                # FIXME remove the replace after dda pipeline is updated to 2.7 (added FS to awk in commit Nov.15 2021)
                fp.write('\t'.join(sample).replace(' ', '_'))
                fp.write('\n')
        params.extend(['--sampletable', sampletable_fn])
    # create input file of filenames
    # depends on mzmldef component, which is always in our dda but maybe not in other pipelines
    if len(run['mzmls']):
        with open(os.path.join(rundir, 'mzmldef.txt'), 'w') as fp:
            for fn in run['mzmls']:
                fnpath = os.path.join(stagedir, 'mzmls', fn['fn'])
                mzstr = '{}\t{}\n'.format(fnpath, fn['mzmldef'])
                fp.write(mzstr)
        params.extend(['--mzmldef', os.path.join(rundir, 'mzmldef.txt')])
    if run['old_mzmls']:
        with open(os.path.join(rundir, 'oldmzmldef.txt'), 'w') as fp:
            for fn in run['old_mzmls']:
                mzstr = '{}\n'.format(fn)
                fp.write(mzstr)
        params.extend(['--oldmzmldef', os.path.join(rundir, 'oldmzmldef.txt')])
    params = [x if x != 'RUNNAME__PLACEHOLDER' else run['runname'] for x in params]
    outfiles = execute_normal_nf(run, params, rundir, gitwfdir, self.request.id, nf_version, profiles)
    postdata.update({'state': 'ok'})
    outfiles_db = register_resultfiles(outfiles)
    fileurl = urljoin(settings.KANTELEHOST, reverse('jobs:analysisfile'))
    fn_ids = transfer_resultfiles((settings.ANALYSISSHARENAME, run['outdir']), rundir, outfiles_db, fileurl, self.request.id, run['analysis_id'])
    reporturl = urljoin(settings.KANTELEHOST, reverse('jobs:analysisdone'))
    report_finished_run(reporturl, postdata, stagedir, rundir, run['analysis_id'])
    return run


@shared_task(bind=True, queue=settings.QUEUE_NXF)
def refine_mzmls(self, run, params, mzmls, stagefiles, profiles, nf_version):
    print('Got message to run mzRefine workflow, preparing')
    rundir = create_runname_dir(run)
    params, gitwfdir, stagedir = prepare_nextflow_run(run, self.request.id, rundir, stagefiles, mzmls, params)
    with open(os.path.join(rundir, 'mzmldef.txt'), 'w') as fp:
        for fn in mzmls:
            # FIXME not have set, etc, pass rawfnid here!
            mzstr = '{fpath}\t{refined_sfid}\n'.format(fpath=os.path.join(stagedir, 'mzmls', fn['fn']), refined_sfid=fn['sfid'])
            fp.write(mzstr)
    params.extend(['--mzmldef', os.path.join(rundir, 'mzmldef.txt')])
    outfiles = execute_normal_nf(run, params, rundir, gitwfdir, self.request.id, nf_version, profiles)
    # TODO ideally do this:
    # stage mzML with {dbid}___{filename}.mzML
    # This keeps dbid / ___ split out of the NF workflow 
    outfiles_db = {}
    for fn in outfiles:
        sf_id, newname = os.path.basename(fn).split('___')
        outfiles_db[fn] = {'file_id': sf_id, 'newname': newname, 'md5': calc_md5(fn)}
    fileurl = urljoin(settings.KANTELEHOST, reverse('jobs:mzmlfiledone'))
    fn_ids = transfer_resultfiles((settings.ANALYSISSHARENAME, run['outdir']), rundir, outfiles_db, fileurl, self.request.id, run['analysis_id'])
    reporturl = urljoin(settings.KANTELEHOST, reverse('jobs:analysisdone'))
    postdata = {'client_id': settings.APIKEY, 'analysis_id': run['analysis_id'],
            'task': self.request.id, 'name': run['name'], 'user': run['outdir'], 'state': 'ok'}
    report_finished_run(reporturl, postdata, stagedir, rundir, run['analysis_id'])
    return run


def create_runname_dir(run):
    runname = '{}_{}_{}'.format(run['analysis_id'], run['name'], run['timestamp'])
    run['runname'] = runname
    rundir = settings.NF_RUNDIRS[run.get('nfrundirname', 'small')]
    return os.path.join(rundir, runname).replace(' ', '_')


def prepare_nextflow_run(run, taskid, rundir, stagefiles, mzmls, params):
    if 'analysis_id' in run:
        log_analysis(run['analysis_id'], 'Got message to run workflow, preparing')
    # runname is set in task so timestamp corresponds to execution start and not job queue
    gitwfdir = os.path.join(rundir, 'gitwfs')
    if not os.path.exists(rundir):
        try:
            os.makedirs(rundir)
        except (OSError, PermissionError):
            taskfail_update_db(taskid, 'Could not create workdir on analysis server')
            raise
    stagedir = os.path.join(settings.ANALYSIS_STAGESHARE, run['runname'])
    if 'analysis_id' in run:
        log_analysis(run['analysis_id'], 'Checked out workflow repo, staging files')
    print('Staging files to {}'.format(stagedir))
    try:
        params = stage_files(stagedir, stagefiles, params)
        if len(mzmls):
            stage_files(stagedir, {'mzmls': [(x['servershare'], x['path'], x['fn']) for x in mzmls]})
    except Exception:
        taskfail_update_db(taskid, 'Could not stage files for analysis')
        raise
    return params, gitwfdir, stagedir


def process_error_from_nf_log(logfile):
    """Assume the log contains only a traceback as error (and log lines), 
    if not process the NF specific error output"""
    nf_swap_err = 'WARNING: Your kernel does not support swap limit capabilities or the cgroup is not mounted. Memory limited without swap.'
    errorlines = []
    is_traceback = True
    part_of_error = False
    tracelines = []
    with open(logfile) as fp:
        for line in fp:
            if line.strip() == 'Caused by:':
                is_traceback = False
                part_of_error = True
                break
            elif 'Cause:' in line or 'Exception' in line:
                tracelines.append(line.strip())
        if is_traceback:
            errorlines = tracelines[:]
        else:
            for line in fp:
                if line.startswith('  ') and part_of_error:
                    if line.strip() != nf_swap_err:
                        errorlines.append(line.strip())
                elif line.strip() == 'Command error:':
                    part_of_error = True
                elif line[:5] == 'Tip: ':
                    break
                else:
                    part_of_error = False
        return '\n'.join(errorlines)


def execute_normal_nf(run, params, rundir, gitwfdir, taskid, nf_version, profiles):
    log_analysis(run['analysis_id'], 'Staging files finished, starting analysis')
    if not profiles:
        profiles = 'standard'
    try:
        run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version)
    except subprocess.CalledProcessError as e:
        errmsg = process_error_from_nf_log(os.path.join(gitwfdir, '.nextflow.log'))
        log_analysis(run['analysis_id'], 'Workflow crashed, reporting errors')
        taskfail_update_db(taskid, errmsg)
        raise RuntimeError('Error occurred running nextflow workflow '
                           '{}\n\nERROR MESSAGE:\n{}'.format(rundir, errmsg))
    #TODO use nextflows -weblog functionality to do logging
    with open(os.path.join(gitwfdir, 'trace.txt')) as fp:
        nflog = fp.read()
    log_analysis(run['analysis_id'], 'Workflow finished, transferring result and'
                 ' cleaning. Full NF trace log: \n{}'.format(nflog))
    outfiles = [os.path.join(rundir, 'output', x) for x in os.listdir(os.path.join(rundir, 'output'))]
    outfiles = [x for x in outfiles if not os.path.isdir(x)]
    reportfile = os.path.join(rundir, 'output', 'Documentation', 'pipeline_report.html')
    if os.path.exists(reportfile):
        outfiles.append(reportfile)
    return outfiles


@shared_task(bind=True, queue=settings.QUEUE_QC_NXF)
def run_nextflow_longitude_qc(self, run, params, stagefiles, nf_version):
    print('Got message to run QC workflow, preparing')
    reporturl = urljoin(settings.KANTELEHOST, reverse('jobs:storelongqc'))
    postdata = {'client_id': settings.APIKEY, 'rf_id': run['rf_id'],
                'analysis_id': run['analysis_id'], 'task': self.request.id,
                'instrument': run['instrument'], 'filename': run['filename']}
    rundir = create_runname_dir(run)
    params, gitwfdir, stagedir = prepare_nextflow_run(run, self.request.id, rundir, stagefiles, [], params)
    # FIXME temp code tmp to remove when all QC is run in new QC WF (almost)
    # dont forget to update the nf_version in teh DB before deleting this!
    if '--raw' in stagefiles:
        nf_version = '19.10.0' 
        profiles = 'qc,docker,lehtio'
    else:
        nf_version = False
        profiles = 'qc'
    try:
        run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version)
    except subprocess.CalledProcessError:
        errmsg = process_error_from_nf_log(os.path.join(gitwfdir, '.nextflow.log'))
        log_analysis(run['analysis_id'], 'QC Workflow crashed')
        with open(os.path.join(gitwfdir, 'trace.txt')) as fp:
            header = next(fp).strip('\n').split('\t')
            exitfield, namefield = header.index('exit'), header.index('name')
            for line in fp:
                # exit code 3 -> not enough PSMs, but maybe
                # CANT FIND THIS IN PIPELINE OR MSSTITCH!
                # it would be ok to have pipeline output
                # this message instead
                line = line.strip('\n').split('\t')
                if line[namefield] == 'createPSMPeptideTable' and line[exitfield] == '3':
                    postdata.update({'state': 'error', 'errmsg': 'Not enough PSM data found in file to extract QC from, possibly bad run'})
                    report_finished_run(reporturl, postdata, stagedir, rundir, run['analysis_id'])
                    raise RuntimeError('QC file did not contain enough quality PSMs')
        taskfail_update_db(self.request.id, errmsg)
        raise RuntimeError('Error occurred running QC workflow '
                           '{}'.format(rundir))
    with open(os.path.join(rundir, 'output', 'qc.json')) as fp:
        qcreport = json.load(fp)
    log_analysis(run['analysis_id'], 'QC Workflow finished')
    postdata.update({'state': 'ok', 'plots': qcreport})
    report_finished_run(reporturl, postdata, stagedir, rundir, run['analysis_id'])
    return run


def report_finished_run(url, postdata, stagedir, rundir, analysis_id):
    print('Reporting and cleaning up after workflow in {}'.format(rundir))
    # If deletion fails, rerunning will be a problem? TODO wrap in a try/taskfail block
    postdata.update({'log': 'Analysis task completed.', 'analysis_id': analysis_id})
    update_db(url, json=postdata)
    shutil.rmtree(rundir)
    shutil.rmtree(stagedir)


def register_resultfiles(outfiles):
    # First register files, check md5, prune those
    reg_url = urljoin(settings.KANTELEHOST, reverse('files:register'))
    outfiles_db = {}
    for fn in outfiles:
        fname = os.path.basename(fn)
        postdata = {'fn': fname,
                    'client_id': settings.APIKEY,
                    'md5': calc_md5(fn),
                    'size': os.path.getsize(fn),
                    'date': str(os.path.getctime(fn)),
                    'claimed': True,
                    }
        resp = requests.post(url=reg_url, data=postdata, verify=settings.CERTFILE)
        resp.raise_for_status()
        rj = resp.json()
        if not rj['stored']:
            outfiles_db[fn] = resp.json()
            outfiles_db[fn].update({'newname': fname, 'md5': postdata['md5']})
    return outfiles_db


def transfer_resultfiles(baselocation, rundir, outfiles_db, url, task_id, analysis_id=False):
    """Copies analysis results to data server, calculates MD5 on destination. After that
    URL is called which"""
    outpath = os.path.join(baselocation[1], os.path.split(rundir)[-1])
    outdir = os.path.join(settings.SHAREMAP[baselocation[0]], outpath)
    try:
        if not os.path.exists(outdir):
            os.makedirs(outdir)
    except (OSError, PermissionError):
        taskfail_update_db(task_id, 'Could not create output directory for analysis results')
        raise
    for fn in outfiles_db:
        dst = os.path.join(outdir, outfiles_db[fn]['newname'])
        try:
            shutil.copy(fn, dst)
        except:
            taskfail_update_db(task_id, 'Errored when trying to copy files to analysis result destination')
            raise
        postdata = {'client_id': settings.APIKEY, 'fn_id': outfiles_db[fn]['file_id'],
                    'outdir': outpath, 'filename': outfiles_db[fn]['newname']}
        if calc_md5(dst) != outfiles_db[fn]['md5']:
            msg = 'Copying error, MD5 of src and dst are different'
            taskfail_update_db(task_id, msg)
            raise RuntimeError(msg)
        else:
            postdata['md5'] = outfiles_db[fn]['md5']
        if analysis_id:
            postdata.update({'ftype': 'analysis_output', 'analysis_id': analysis_id})
        response = update_db(url, form=postdata)
        response.raise_for_status()
    return {x['file_id']: False for x in outfiles_db.values()}