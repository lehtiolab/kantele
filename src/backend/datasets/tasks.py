import os
import shutil
import subprocess
from urllib.parse import urljoin

from django.urls import reverse
from celery import shared_task
from celery.exceptions import MaxRetriesExceededError

from kantele import settings
from jobs.post import update_db, taskfail_update_db
from analysis.tasks import create_runname_dirname, prepare_nextflow_run, run_nextflow, register_mzmlfile, process_error_from_nf_log, copy_stage_files
from rawstatus.tasks import calc_md5, delete_empty_dir

# Updating stuff in tasks happens over the API, assume no DB is touched. This
# avoids setting up auth for DB



@shared_task(bind=True)
def run_convert_mzml_nf(self, run, params, raws, ftype_name, nf_version, profiles, stagescratchdir):
    basedir = create_runname_dirname(run)
    params, gitwfdir, stagedir, scratchdir = prepare_nextflow_run(run, self.request.id, basedir,
            {}, params, stagescratchdir)
    # Stage raws if there is a stagedir
    if scratchdir:
        rawdir = os.path.join(scratchdir, 'raws')
        try:
            os.makedirs(rawdir, exist_ok=True)
        except (OSError, PermissionError):
            taskfail_update_db(taskid, 'Could not create workdir on analysis server')
            raise
        copy_stage_files(rawdir, raws)
        params.extend(['--raws', os.path.join(rawdir, '*')])
    else:
        cmdraws = ';'.join([os.path.join(x[0], x[1]) for x in raws])
        params.extend(['--raws', cmdraws])
    try:
        run_outdir = run_nextflow(run, params, basedir, gitwfdir, profiles, nf_version, scratchdir)
    except subprocess.CalledProcessError as e:
        errmsg = process_error_from_nf_log(os.path.join(gitwfdir, '.nextflow.log'))
        taskfail_update_db(self.request.id, errmsg)
        raise RuntimeError('Error occurred converting mzML files: '
                           '{}\n\nERROR MESSAGE:\n{}'.format(rundir, errmsg))
    # Technically we can dump straight to infile paths
    token = False
    transfer_url = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorage'))
    for raw in raws:
        regfile = register_mzmlfile(raw[2], raw[3], run_outdir, run['server_id'])
    url = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorage'))
    postdata = {'client_id': settings.APIKEY, 'task': self.request.id}
    update_db(url, json=postdata)
    if scratchdir:
        shutil.rmtree(scratchdir)
    shutil.rmtree(gitwfdir)


@shared_task(bind=True)
def rename_dset_storage_location(self, sharepath, srcpath, dstpath, sfloc_ids, dss_id):
    """This expects one dataset per dir, as it will rename the whole dir"""
    print(f'Renaming dataset storage {srcpath} to {dstpath}')
    srcfull = os.path.join(sharepath, srcpath)
    dstfull = os.path.join(sharepath, dstpath)
    try:
        # os.renames can rename recursively so not only leaf node
        # can change, but we can rename a project directly for each
        # dataset e.g. proj1/exp/ds1 -> proj2/exp/ds1
        os.renames(srcfull, dstfull)
    except NotADirectoryError:
        taskfail_update_db(self.request.id, msg=f'Failed renaming project {srcfull} is a directory '
                f'but {dstfull} is a file')
        raise
    except Exception:
        taskfail_update_db(self.request.id, msg=f'Failed renaming dataset location {srcfull} '
        f'to {dstfull} for unknown reason')
        raise
    # Go through dirs in path and delete empty ones caused by move
    splitpath = srcpath.split(os.sep)
    for pathlen in range(0, len(splitpath))[::-1]:
        # no rmdir on the leaf dir (would be pathlen+1) since that's been moved
        checkpath = os.path.join(sharepath, os.sep.join(splitpath[:pathlen]))
        if os.path.exists(checkpath) and os.path.isdir(checkpath) and not os.listdir(checkpath):
            try:
                os.rmdir(checkpath)
            except:
                taskfail_update_db(self.request.id)
                raise
    postdata = {'sfloc_ids': sfloc_ids, 'dst_path': dstpath, 'client_id': settings.APIKEY,
            'dss_id': dss_id, 'task': self.request.id}
    url = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorageds'))
    try:
        update_db(url, json=postdata)
    except RuntimeError:
        # FIXME cannot move back shutil.move(dst, src)
        raise
