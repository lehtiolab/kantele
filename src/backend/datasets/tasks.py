import os
import shutil
import subprocess
from urllib.parse import urljoin

from django.urls import reverse
from celery import shared_task
from celery.exceptions import MaxRetriesExceededError

from kantele import settings
from jobs.post import update_db, taskfail_update_db
from analysis.tasks import create_runname_dir, prepare_nextflow_run, run_nextflow, transfer_resultfile, check_in_transfer_client
from rawstatus.tasks import calc_md5, delete_empty_dir

# Updating stuff in tasks happens over the API, assume no DB is touched. This
# avoids setting up auth for DB


@shared_task(bind=True)
def run_convert_mzml_nf(self, run, params, raws, ftype_name, nf_version, profiles, **kwargs):
    postdata = {'client_id': settings.APIKEY, 'task': self.request.id}
    rundir = create_runname_dir(run)
    params, gitwfdir, stagedir = prepare_nextflow_run(run, self.request.id, rundir, {'--raws': raws}, [], params)
    try:
        run_outdir = run_nextflow(run, params, rundir, gitwfdir, profiles, nf_version)
    except subprocess.CalledProcessError as e:
        # FIXME report stderr with e
        errmsg = 'OUTPUT:\n{}\nERROR:\n{}'.format(e.stdout, e.stderr)
        taskfail_update_db(self.request.id, errmsg)
        raise RuntimeError('Error occurred converting mzML files: '
                           '{}\n\nERROR MESSAGE:\n{}'.format(rundir, errmsg))
    transfer_url = urljoin(settings.KANTELEHOST, reverse('jobs:mzmlfiledone'))
    outpath = os.path.split(rundir)[-1]
    outfullpath = os.path.join(settings.SHAREMAP[run['dstsharename']], outpath)
    try:
        os.makedirs(outfullpath, exist_ok=True)
    except (OSError, PermissionError):
        taskfail_update_db(self.request.id, 'Could not create output directory for analysis results')
        raise
    token = False
    for raw in raws:
        token = check_in_transfer_client(self.request.id, token, ftype_name)
        srcpath = os.path.join(run_outdir, raw[4])
        transfer_resultfile(outfullpath, outpath, srcpath, run['dstsharename'],
                transfer_url, token, self.request.id, raw[3], calc_md5(srcpath), raw[4])
    # FIXME first check tstate so no dup transfers used?
    # TODO we're only reporting task finished in this POST call, but there is no specific route
    # for that.
    url = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorage'))
    update_db(url, json=postdata)
    shutil.rmtree(rundir)
    if stagedir:
        shutil.rmtree(stagedir)


@shared_task(bind=True)
def rename_top_level_project_storage_dir(self, projsharename, srcname, newname, proj_id, sfloc_ids):
    """Renames a project, including the below experiments/datasets"""
    msg = False
    projectshare = settings.SHAREMAP[projsharename]
    srcpath = os.path.join(projectshare, srcname)
    dstpath = os.path.join(projectshare, newname)
    if not os.path.exists(srcpath):
        msg = f'Cannot move project name {srcname} to {newname}, does not exist'
    elif not os.path.isdir(srcpath):
        msg = f'Cannot move project name {srcname} to {newname}, {srcname} is not a directory'
    elif os.path.exists(dstpath):
        msg = f'Cannot move project name {srcname} to {newname}, already exists'
    if msg:
        taskfail_update_db(self.request.id, msg)
        raise RuntimeError(msg)
    try:
        os.rename(srcpath, dstpath)
    except NotADirectoryError:
        taskfail_update_db(self.request.id, msg=f'Failed renaming project {srcpath} is a directory '
                f'but {dstpath} is a file')
        raise
    except Exception:
        taskfail_update_db(self.request.id, msg='Failed renaming project for unknown reason')
        raise
    postdata = {'proj_id': proj_id, 'newname': newname, 'sfloc_ids': sfloc_ids,
            'task': self.request.id, 'client_id': settings.APIKEY}
    url = urljoin(settings.KANTELEHOST, reverse('jobs:renameproject'))
    update_db(url, json=postdata)


@shared_task(bind=True)
def rename_dset_storage_location(self, ds_sharename, srcpath, dstpath, sfloc_ids):
    """This expects one dataset per dir, as it will rename the whole dir"""
    print(f'Renaming dataset storage {srcpath} to {dstpath}')
    ds_share = settings.SHAREMAP[ds_sharename]
    srcfull = os.path.join(ds_share, srcpath)
    dstfull = os.path.join(ds_share, dstpath)
    try:
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
        checkpath = os.path.join(ds_share, os.sep.join(splitpath[:pathlen]))
        if os.path.exists(checkpath) and os.path.isdir(checkpath) and not os.listdir(checkpath):
            try:
                os.rmdir(checkpath)
            except:
                taskfail_update_db(self.request.id)
                raise
    fn_postdata = {'sfloc_ids': sfloc_ids, 'dst_path': dstpath, 'client_id': settings.APIKEY}
    fnurl = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorage'))
    try:
        update_db(fnurl, json=fn_postdata)
    except RuntimeError:
        # FIXME cannot move back shutil.move(dst, src)
        raise


@shared_task(bind=True)
def move_file_storage(self, fn, srcshare, srcpath, dstpath, sfloc_id, dstsharename, newname=False):
    '''Moves file across server shares, dirs, or as rename'''
    src = os.path.join(settings.SHAREMAP[srcshare], srcpath, fn)
    dstfn = newname or fn
    dst = os.path.join(settings.SHAREMAP[dstsharename], dstpath, dstfn)
    url = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorage'))
    if src == dst:
        print('Source and destination are identical, not moving file')
        update_db(url, json={'client_id': settings.APIKEY, 'task': self.request.id})
    print('Moving file {} to {}'.format(src, dst))
    dstdir = os.path.split(dst)[0]
    try:
        os.makedirs(dstdir, exist_ok=True)
    except Exception:
            taskfail_update_db(self.request.id)
            raise
    if not os.path.isdir(dstdir):
        taskfail_update_db(self.request.id)
        raise RuntimeError('Directory {} is already on disk as a file name. '
                           'Not moving files.')
    try:
        shutil.move(src, dst)
    except Exception as e:
        taskfail_update_db(self.request.id)
        raise RuntimeError('Could not move file tot storage:', e)
    postdata = {'sfloc_id': sfloc_id, 'servershare': dstsharename, 'dst_path': dstpath,
            'client_id': settings.APIKEY, 'task': self.request.id}
    if newname:
        postdata['newname'] = newname
    try:
        update_db(url, json=postdata)
    except RuntimeError:
        shutil.move(dst, src)
        raise


@shared_task(bind=True)
def move_stored_file_tmp(self, sharename, fn, path, sfloc_id):
    src = os.path.join(settings.SHAREMAP[sharename], path, fn)
    dst = os.path.join(settings.TMPSHARE, fn)
    print(f'Moving stored file {sfloc_id} to tmp')
    try:
        shutil.move(src, dst)
    except Exception:
        taskfail_update_db(self.request.id)
        raise
    postdata = {'sfloc_id': sfloc_id, 'servershare': settings.TMPSHARENAME,
                'dst_path': '', 'client_id': settings.APIKEY,
                'task': self.request.id}
    url = urljoin(settings.KANTELEHOST, reverse('jobs:updatestorage'))
    try:
        update_db(url, json=postdata)
    except RuntimeError:
        shutil.move(dst, src)
        raise
