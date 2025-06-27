import os
import json
import shutil
import requests
import subprocess
from time import sleep
from datetime import datetime
from urllib.parse import urljoin, urlsplit
from ftplib import FTP
from hashlib import md5
from dulwich.porcelain import clone, reset, fetch
from tempfile import NamedTemporaryFile
from gzip import GzipFile

from django.urls import reverse
from django.utils import timezone
from celery import shared_task, exceptions
from celery import states as taskstates

from jobs.post import update_db, taskfail_update_db
from kantele import settings
from rawstatus.tasks import calc_md5
from analysis.models import UniProtFasta


@shared_task(bind=True)
def check_ensembl_uniprot_fasta_download(self, dbname, version, organism, dbtype, shareid, sharepath,
        sflpath, ftype_id):
    """Checks if there is a new version of ENSEMBL data,
    downloads it to system over FTP"""
    doneurl = urljoin(settings.KANTELEHOST, reverse('jobs:internalfiledone'))

    fa_data = {'dbname': dbname, 'organism': organism, 'version': version, 'dbtype': dbtype}
    dstpath = os.path.join(sharepath, sflpath)
    try:
        os.makedirs(dstpath, exist_ok=True)
    except Exception:
        msg = 'Cannot mkdir for download fasta'
        taskfail_update_db(self.request.id, msg)
        raise

    if dbname == 'uniprot':
        uptype = UniProtFasta.UniprotClass[dbtype]
        url = settings.UNIPROT_API.format(settings.UP_ORGS[organism],
                UniProtFasta.url_addons[dbtype])
        desc = f'Uniprot release {version}, {organism}, {uptype.label} fasta'
        dighash = md5()
        dstfn = f'Uniprot_{version}_{organism.replace(" ", "_")}_{uptype.label}.fa'
        dst = os.path.join(dstpath, dstfn)
        with requests.get(url, stream=True) as req, NamedTemporaryFile(mode='wb') as wfp:
            for chunk in req.iter_content(chunk_size=8192):
                if chunk:
                    wfp.write(chunk)
                dighash.update(chunk)
            # stay in context until copied, else tempfile is deleted
            wfp.seek(0)
            dighash = dighash.hexdigest() 
            shutil.copy(wfp.name, dst)
        # FIXME first copy to a tmp file and register, then move into proper place?

    elif dbname == 'ensembl':
        # Download db, use FTP to get file, download zipped via HTTPS and unzip in stream
        url = urlsplit(settings.ENSEMBL_DL_URL.format(version, organism.lower().replace(' ', '_')))
        desc = f'ENSEMBL {organism} release {version} pep.all fasta'
        dstfn = f'ENS{version}_{organism.replace(" ", "_")}.fa'
        dst = os.path.join(dstpath, dstfn)
        with FTP(url.netloc) as ftp:
            ftp.login()
            fn = [x for x in ftp.nlst(url.path) if 'pep.all.fa.gz' in x][0]
        # make sure to specify no compression (identity) to server, otherwise the 
        # raw object will be gzipped and when unzipped contain a gzipped file
        with requests.get(urljoin('http://' + url.netloc, fn), 
                headers={'Accept-Encoding': 'identity'}, stream=True).raw as reqfp:
            with NamedTemporaryFile(mode='wb') as wfp, GzipFile(fileobj=reqfp) as gzfp:
                for line in gzfp:
                    wfp.write(line)
                # Now register download in Kantele, still in context manager
                # since tmp file will be deleted on close()
                wfp.seek(0)
                shutil.copy(wfp.name, dst)
        dighash = False

    fa_data['desc'] = desc
    regresp = register_resultfile(dstfn, sflpath, md5sum=dighash, fasta=fa_data, share_id=shareid, sharepath=sharepath, is_library=True, filetype_id=ftype_id)

    # went home!
    if errmsg := regresp.get('error'):
        print(f'Something went wrong registering downloaded fasta {dst} - {errmsg}')
    else:
        update_db(urljoin(settings.KANTELEHOST, reverse('jobs:settask')), json={'task_id': self.request.id,
        'client_id': settings.APIKEY, 'state': taskstates.SUCCESS})
    print(f'Finished downloading {desc}')
        


def run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version, scratchdir):
    """Fairly generalized code for kantele celery task to run a WF in NXF"""
    print('Starting nextflow workflow {}'.format(run['nxf_wf_fn']))
    outdir = os.path.join(rundir, 'output')
    try:
        clone(run['repo'], gitwfdir, checkout=run['wf_commit'])
    except FileExistsError:
        fetch(gitwfdir, run['repo'])
        reset(gitwfdir, 'hard', run['wf_commit'])
    # FIXME dulwich does not seem to checkout anything, use this until it does
    subprocess.run(['git', 'checkout', run['wf_commit']], check=True, cwd=gitwfdir)
    print('Checked out repo {} at commit {}'.format(run['repo'], run['wf_commit']))
    # There will be files inside data dir of WF repo so we must be in
    # that dir for WF to find them
    cmd = [*settings.NXF_COMMAND, run['nxf_wf_fn'], *params, '--outdir', outdir, '-profile', profiles, '-with-trace', '-resume']
    env = os.environ
    env['NXF_VER'] = nf_version
    if scratchdir:
        env['TMPDIR'] = scratchdir
    if 'token' in run and run['token']:
        nflogurl = urljoin(settings.KANTELEHOST, reverse('analysis:nflog'))
        cmd.extend(['-name', run['token'], '-with-weblog', nflogurl])
    if 'analysis_id' in run:
        log_analysis(run['analysis_id'], 'Running command {}, nextflow version {}'.format(' '.join(cmd), env.get('NXF_VER', 'default')))
    nxf_sub = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=gitwfdir, env=env)
    try:
        stdout, stderr = nxf_sub.communicate()
    except exceptions.SoftTimeLimitExceeded:
        # celery has killed the job (task revoked) make sure nextflow is stopped and not left
        nxf_sub.kill()
        print('Job has been revoked, stopping analysis')
        raise
    else:
        # raise any exceptions nextflow has caused
        if nxf_sub.returncode != 0:
            raise subprocess.CalledProcessError(nxf_sub.returncode, cmd, stdout, stderr=stderr)
    return outdir


def get_dir_content_size(fpath):
    return sum([os.path.getsize(f) for f in os.listdir(fpath) if os.path.isfile(f)])


def log_analysis(analysis_id, message):
    logurl = urljoin(settings.KANTELEHOST, reverse('analysis:appendlog'))
    update_db(logurl, json={'analysis_id': analysis_id, 'message': message})


@shared_task(bind=True)
def run_nextflow_workflow(self, run, params, paramfiles, stagefiles, profiles, nf_version, scratchbasedir):
    print('Got message to run nextflow workflow, preparing')
    # Init
    rundir = create_runname_dirname(run)

    # stage files, create dirs etc
    params, gitwfdir, stagedir, scratchdir = prepare_nextflow_run(run, self.request.id, rundir,
            stagefiles, params, scratchbasedir)
    if sampletable := run['components']['ISOQUANT_SAMPLETABLE']:
        sampletable_fn = os.path.join(stagedir, 'sampletable.txt')
        with open(sampletable_fn, 'w') as fp:
            for sample in sampletable:
                fp.write('\t'.join(sample))
                fp.write('\n')
        params.extend(['--sampletable', sampletable_fn])

    # create input file of filenames
    # depends on inputdef component, which is always in ours but maybe not in other pipelines
    if run['components']['INPUTDEF'] and len(run['infiles']):
        with open(os.path.join(stagedir, 'inputdef.txt'), 'w') as fp:
            fp.write('\t'.join(run['components']['INPUTDEF']))
            for fn in run['infiles']:
                fnpath = os.path.join(fn['path'], fn['fn'])
                fn_metadata = '\t'.join(fn[x] or '' for x in run['components']['INPUTDEF'][1:])
                fp.write(f'\n{fnpath}\t{fn_metadata}')
        params.extend(['--input', os.path.join(stagedir, 'inputdef.txt')])

    if 'COMPLEMENT_ANALYSIS' in run['components'] and run['old_infiles']:
        with open(os.path.join(stagedir, 'oldinputdef.txt'), 'w') as fp:
            fp.write('\t'.join(run['components']['INPUTDEF']))
            for fn in run['old_infiles']:
                fp.write(f'\n{fn}')
        params.extend(['--oldmzmldef', os.path.join(stagedir, 'oldinputdef.txt')])
    params = [x if x != 'RUNNAME__PLACEHOLDER' else run['runname'] for x in params]
    outfiles = execute_normal_nf(run, params, rundir, gitwfdir, self.request.id, nf_version, profiles, scratchdir)

    # Register output files to web host
    token = False
    for ofile in outfiles:
        # FIXME sharepath == NF_RUNDIR? Can we pass from job/db?
        full_path, fn = os.path.split(ofile)
        path = os.path.relpath(full_path, settings.NF_RUNDIR)
        regfile = register_resultfile(fn, path, server_id=run['server_id'],
                analysis_id=run['analysis_id'], sharepath=settings.NF_RUNDIR)
        # FIXME this also, sometimes files with same MD5 arrive twice - can that be handled??:
        # Yes - analysis/sfile can possibly have multiple analysis per sfile
        if not regfile:
            continue
        checksrvurl = urljoin(settings.KANTELEHOST, reverse('analysis:checkfileupload'))
        resp = requests.post(checksrvurl, json={'fname': fn, 'client_id': settings.APIKEY})
        if resp.status_code == 200:
            # Servable file found, upload also to web server
            # Somewhat complex POST to get JSON and files in same request
            postdata = {'client_id': settings.APIKEY, 'sfid': regfile['sfid'], 'fname': fn,
                    'path': os.path.basename(rundir)}
            response = requests.post(checksrvurl, files={
                'ana_file': (fn, open(ofile, 'rb'), 'application/octet-stream'),
                'json': (None, json.dumps(postdata), 'application/json')})
    reporturl = urljoin(settings.KANTELEHOST, reverse('jobs:analysisdone'))
    postdata = {'client_id': settings.APIKEY, 'analysis_id': run['analysis_id'],
            'task': self.request.id}
    report_finished_run_and_cleanup(reporturl, postdata, scratchdir, stagedir, gitwfdir, run['analysis_id'])
    return run


@shared_task(bind=True)
def refine_mzmls(self, run, params, mzmls, stagefiles, profiles, nf_version, stagescratchdir):
    print('Got message to run mzRefine workflow, preparing')
    basedir = create_runname_dirname(run)
    params, gitwfdir, stagedir, scratchdir = prepare_nextflow_run(run, self.request.id, basedir,
            stagefiles, params, stagescratchdir)
    mzmls_def = [os.path.join(x['srcpath'], x['fn']) for x in mzmls]

    with open(os.path.join(stagedir, 'mzmldef.txt'), 'w') as fp:
        for fn in mzmls_def:
            fp.write(f'{fn}\n')
    params.extend(['--input', os.path.join(stagedir, 'mzmldef.txt')])
    outfiles = execute_normal_nf(run, params, basedir, gitwfdir, self.request.id, nf_version, profiles, scratchdir)
    outfiles_db = {}
    for outfn in outfiles:
        path, fn = os.path.split(outfn)
        outfiles_db[fn] = (path, outfn)
    for non_ref_mzfn in mzmls:
        path, reffn = outfiles_db[non_ref_mzfn['refinedname']]
        regfile = register_mzmlfile(non_ref_mzfn['refinedpk'], reffn, path, run['server_id'])
    reporturl = urljoin(settings.KANTELEHOST, reverse('jobs:analysisdone'))
    postdata = {'client_id': settings.APIKEY, 'analysis_id': run['analysis_id'],
            'task': self.request.id}
    report_finished_run_and_cleanup(reporturl, postdata, scratchdir, stagedir, gitwfdir, run['analysis_id'])
    return run


def create_runname_dirname(run):
    return os.path.join(settings.NF_RUNDIR, run['runname']).replace(' ', '_')


def prepare_nextflow_run(run, taskid, rundir, stageparamfiles, params, stagescratch_basedir):
    '''Creates run dirs, stage dir if needed, and stages files to either of those.
    IF a stagescratch_basedir is passed (e.g. /opt/nfruns, we will output
    a stagedir and a scratchdir there /opt/nfruns/stage /opt/nfruns/scratch
    ELSE we will have /path/to/run/stage and False
    
    '''
    if 'analysis_id' in run:
        log_analysis(run['analysis_id'], 'Got message to run workflow, preparing')
    gitwfdir = os.path.join(rundir, 'gitwfs')
    try:
        os.makedirs(rundir, exist_ok=True)
    except (OSError, PermissionError):
        taskfail_update_db(taskid, 'Could not create workdir on analysis server')
        raise
    if 'analysis_id' in run:
        log_analysis(run['analysis_id'], 'Staging files')
    if stagescratch_basedir:
        stagedir = scratchdir = os.path.join(stagescratch_basedir, os.path.basename(rundir))
        try:
            os.makedirs(stagedir, exist_ok=True)
        except (OSError, PermissionError):
            taskfail_update_db(taskid, 'Could not create stage/scratch dirs on analysis server')
            raise
    else:
        stagedir = os.path.join(rundir, 'stage')
        scratchdir = False
    for flag, files in stageparamfiles.items():
        # Always stage these so they end up in workdirs of their own. In case of multifile
        # on a param, or when having a scratchdir in place
        stagedir_param = os.path.join(stagedir, flag.replace('--', ''))
        if len(files) > 1:
            dst = os.path.join(stagedir_param, '*')
        else:
            dst = os.path.join(stagedir_param, files[0][1])
        params.extend([flag, dst])
        try:
            os.makedirs(stagedir_param, exist_ok=True)
        except Exception:
            taskfail_update_db(taskid, f'Could not create dir to stage files for {flag}')
            raise
        try:
            copy_stage_files(stagedir_param, files)
        except Exception:
            taskfail_update_db(taskid, f'Could not stage files for {flag} for analysis')
            raise
    return params, gitwfdir, stagedir, scratchdir


def copy_stage_files(stagefiledir, files):
    not_needed_files = set(os.listdir(stagefiledir))
    for fdata in files:
        fpath = os.path.join(fdata[0], fdata[1])
        fdst = os.path.join(stagefiledir, fdata[1])
        try:
            not_needed_files.remove(fdata[1])
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
        # Defensive checking if exist, they come from a listdir op above
        if os.path.exists(fpath) and os.path.isdir(fpath):
            shutil.rmtree(fpath)
        elif os.path.exists(fpath):
            os.remove(fpath)


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


def execute_normal_nf(run, params, rundir, gitwfdir, taskid, nf_version, profiles, scratchdir):
    log_analysis(run['analysis_id'], 'Staging files finished, starting analysis')
    if not profiles:
        profiles = 'standard'
    try:
        outdir = run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version, scratchdir)
    except subprocess.CalledProcessError as e:
        errmsg = process_error_from_nf_log(os.path.join(gitwfdir, '.nextflow.log'))
        log_analysis(run['analysis_id'], 'Workflow crashed, reporting errors')
        taskfail_update_db(taskid, errmsg)
        raise RuntimeError('Error occurred running nextflow workflow '
                           '{}\n\nERROR MESSAGE:\n{}'.format(rundir, errmsg))
    # Revoked jobs do not need DB-updating, but need just to be stopped, 
    # so do not catch SoftTimeLimit exception

    # Get log
    env = os.environ
    env['NXF_VER'] = nf_version
    logfields = ('task_id,hash,native_id,name,status,exit,submit,duration,realtime,pcpu,peak_rss'
            ',peak_vmem,rchar,wchar')
    cmd = [settings.NXF_COMMAND[0], 'log', run['token'], '-f', logfields]
    nxf_log = subprocess.run(cmd, capture_output=True, text=True, cwd=gitwfdir, env=env)
    log_analysis(run['analysis_id'], 'Workflow finished, transferring result and'
                 f' cleaning. Full NF trace log: \n{nxf_log.stdout}')
    outfiles = [os.path.join(outdir, x) for x in os.listdir(outdir)]
    outfiles = [x for x in outfiles if not os.path.isdir(x)]
    reportfile = os.path.join(rundir, 'output', 'Documentation', 'pipeline_report.html')
    if os.path.exists(reportfile):
        outfiles.append(reportfile)
    return outfiles


@shared_task(bind=True)
def run_nextflow_longitude_qc(self, run, params, stagefiles, profiles, nf_version, scratchbasedir):
    print('Got message to run QC workflow, preparing')
    reporturl = urljoin(settings.KANTELEHOST, reverse('jobs:storelongqc'))
    postdata = {'client_id': settings.APIKEY, 'qcrun_id': run['qcrun_id'], 'plots': {},
            'task': self.request.id}
    rundir = create_runname_dirname(run)
    params, gitwfdir, no_stagedir, scratchdir = prepare_nextflow_run(run, self.request.id, rundir,
            stagefiles, params, scratchbasedir)
    # QC has no stagedir, we put the raw in rundir to stage
    try:
        outdir = run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version, scratchdir)
    except subprocess.CalledProcessError:
        errmsg = process_error_from_nf_log(os.path.join(gitwfdir, '.nextflow.log'))
        log_analysis(run['analysis_id'], 'QC Workflow crashed')
        with open(os.path.join(gitwfdir, 'trace.txt')) as fp:
            header = next(fp).strip('\n').split('\t')
            exitfield, namefield = header.index('exit'), header.index('name')
            for line in fp:
                # exit code 3 -> not enough PSMs, but maybe
                # CANT FIND THIS IN PIPELINE OR MSSTITCH! FIXME
                # it would be ok to have pipeline output
                # this message instead
                line = line.strip('\n').split('\t')
                if line[namefield] == 'createPSMPeptideTable' and line[exitfield] == '3':
                    postdata.update({'state': 'error', 'msg': 'Not enough PSM data found in file to extract QC from, possibly bad run'})
                    report_finished_run_and_cleanup(reporturl, postdata, scratchdir, no_stagedir, rundir, run['analysis_id'])
                    return run
        taskfail_update_db(self.request.id, errmsg)
        raise RuntimeError('Error occurred running QC workflow {rundir}')
    # FIXME state error can also happen here, if no CalledProcessError?
    with open(os.path.join(outdir, 'qc.json')) as fp:
        qcreport = json.load(fp)
    log_analysis(run['analysis_id'], 'QC Workflow finished')
    postdata.update({'state': 'ok', 'plots': qcreport, 'msg': 'QC run OK'})
    report_finished_run_and_cleanup(reporturl, postdata, scratchdir, no_stagedir, rundir, run['analysis_id'])
    return run


def report_finished_run_and_cleanup(url, postdata, scratchdir, stagedir, rundir, analysis_id):
    print(f'Reporting and cleaning up after workflow in {rundir}')
    # If deletion fails, rerunning will be a problem? TODO wrap in a try/taskfail block
    postdata.update({'log': 'Analysis task completed.', 'analysis_id': analysis_id})
    update_db(url, json=postdata)
    shutil.rmtree(rundir)
    if stagedir and os.path.exists(stagedir):
        shutil.rmtree(stagedir)
    if scratchdir and os.path.exists(scratchdir):
        shutil.rmtree(scratchdir)


def check_in_transfer_client(task_id, token, filetype):
    url = urljoin(settings.KANTELEHOST, reverse('files:check_in'))
    resp = requests.post(url, json={'client_id': settings.APIKEY, 'token': token,
        'task_id': task_id, 'ftype': filetype})
    resp.raise_for_status()
    response = resp.json()
    if response.get('newtoken', False):
        return response['newtoken']
    else:
        return token


def register_resultfile(fname, sflpath, *, sharepath, server_id=False, share_id=False,
        analysis_id=False, md5sum=False, filetype_id=False, fasta=False, is_library=False):
    '''This is for registering output from nextflow runs, automatic downloads
    of fasta files, and possibly more in the future'''
    fullpath = os.path.join(sharepath, sflpath, fname)
    md5sum = md5sum or calc_md5(fullpath)
    reg_url = urljoin(settings.KANTELEHOST, reverse('files:uploaded_file'))
    postdata = {'fn': fname,
                'client_id': settings.APIKEY,
                'md5': md5sum,
                'size': os.path.getsize(fullpath),
                'date': str(os.path.getctime(fullpath)),
                'claimed': True,
                'sharepath': sharepath,
                'path': sflpath,
                'server_id': server_id,
                'share_id': share_id,
                'filetype_id': filetype_id,
                'is_library': is_library,
                'analysis_id': analysis_id,
                'is_fasta': fasta,
                }
    resp = requests.post(url=reg_url, json=postdata)
    if resp.status_code != 500:
        rj = resp.json()
    else:
        rj = False
    resp.raise_for_status()
    return rj
 

def register_mzmlfile(sflpk, fname, path, server_id):
    fullpath = os.path.join(path, fname)
    reg_url = urljoin(settings.KANTELEHOST, reverse('files:uploaded_mzml'))
    postdata = {'sflpk': sflpk,
                'client_id': settings.APIKEY,
                'md5': calc_md5(fullpath),
                'size': os.path.getsize(fullpath),
                'date': str(os.path.getctime(fullpath)),
                'claimed': True,
                'sharepath': settings.NF_RUNDIR,
                'path': os.path.relpath(path, settings.NF_RUNDIR),
                'server_id': server_id,
                }
    resp = requests.post(url=reg_url, json=postdata)
    if resp.status_code != 500:
        rj = resp.json()
    else:
        rj = False
    resp.raise_for_status()
    return rj
 

def transfer_resultfile(dstsharepath, dstpath, fn, url, token, task_id,
        sfl_id, reg_md5, newname, analysis_id=False, checksrvurl=False, is_fasta=False):
    '''Copies files from analyses to outdir on result storage.
    outfullpath is absolute destination dir for file
    dstpath is the path stored in Kantele DB (for users on the share of outfullpath)
    fn is absolute path to src file
    '''
    dst = os.path.join(dstsharepath, dstpath, newname)
    try:
        shutil.copy(fn, dst)
    except:
        taskfail_update_db(task_id, 'Errored when trying to copy files to analysis result destination')
        raise
    os.chmod(dst, 0o640)

    postdata = {'client_id': settings.APIKEY, 'sfloc_id': sfl_id, 'dst_path': dstpath,
            'filename': newname, 'token': token, 
            'analysis_id': analysis_id, 'is_fasta': is_fasta}
    if calc_md5(dst) != reg_md5:
        msg = 'Copying error, MD5 of src and dst are different'
        taskfail_update_db(task_id, msg)
        raise RuntimeError(msg)
    else:
        postdata['md5'] = reg_md5
    if analysis_id:
        # Not for refine mzMLs
        postdata.update({'ftype': settings.ANALYSIS_FT_NAME, 'analysis_id': analysis_id})
        # first check if upload file is OK:
        resp = requests.post(checksrvurl, json={'fname': newname, 'client_id': settings.APIKEY})
        if resp.status_code == 200:
            # Servable file found, upload also to web server
            # Somewhat complex POST to get JSON and files in same request
            response = requests.post(url, files={
                'ana_file': (newname, open(fn, 'rb'), 'application/octet-stream'),
                'json': (None, json.dumps(postdata), 'application/json')})
        else:
            response = update_db(url, files={
                'json': (None, json.dumps(postdata), 'application/json')})
    # Also here, somewhat complex POST to get JSON and files in same request
    else:
        response = update_db(url, json=postdata)
    response.raise_for_status()
