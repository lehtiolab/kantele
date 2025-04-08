import os

from django.db.models import F
from django.urls import reverse

from kantele import settings
from rawstatus.models import StoredFile, ServerShare, StoredFileType, StoredFileLoc
from datasets.models import Dataset, DatasetRawFile, Project
from analysis.models import Proteowizard, MzmlFile, NextflowWfVersionParamset
from datasets import tasks
from rawstatus import tasks as filetasks
from jobs.jobs import DatasetJob, ProjectJob
from rawstatus import jobs as rsjobs


class RenameProject(ProjectJob):
    '''Uses task that does os.rename on project lvl dir. Needs also to update
    dsets / storedfiles to new path post job'''
    refname = 'rename_top_lvl_projectdir'
    task = tasks.rename_top_level_project_storage_dir
    retryable = False

    def getfiles_query(self, **kwargs):
        '''Get all files with same path as project_dsets.storage_locs, used to update
        path of those files post-job. NB this is not ALL sfids in this project, but only
        those confirmed to be in correct location'''
        dsets = Dataset.objects.filter(runname__experiment__project_id=kwargs['proj_id'],
                deleted=False, purged=False)
        return StoredFileLoc.objects.filter(sfile__deleted=False, purged=False,
                servershare__in=[x.storageshare for x in dsets.distinct('storageshare')],
                path__in=[x.storage_loc for x in dsets.distinct('storage_loc')])

    def check_error(self, **kwargs):
        # Since proj name is unique, this wont do much, but its ok to check here instead of in view 
        # just in case of weird race between view/job
        '''Duplicate check'''
        if Project.objects.filter(name=kwargs['newname']).exclude(pk=kwargs['proj_id']).exists():
            return f'There is already a project by that name {kwargs["newname"]}'
        else:
            return False

    def process(self, **kwargs):
        """Fetch fresh project name here, then queue for move from there"""
        new_is_oldname = True
        for ds in Dataset.objects.select_related('storageshare').filter(
                runname__experiment__project_id=kwargs['proj_id']):
            ds_toploc = ds.storage_loc.split(os.path.sep)[0]
            ssharename = ds.storageshare.name
            if ds_toploc != kwargs['newname']:
                new_is_oldname = False
                break
        if not new_is_oldname:
            self.run_tasks = [((ssharename, ds_toploc, kwargs['newname'], kwargs['proj_id'],
                [x.id for x in self.getfiles_query(**kwargs)]), {})]


class RenameDatasetStorageLoc(DatasetJob):
    '''Renames dataset, then updates storage_loc of it and path of all dataset storedfiles
    which have same path as dataset.storage_loc, including any deleted files, but not newly
    added files from tmp.
    Calling this job needs to be checked for forbidden duplicate storage locs
    '''
    refname = 'rename_dset_storage_loc'
    task = tasks.rename_dset_storage_location
    retryable = False

    def check_error(self, **kwargs):
        '''Duplicate check, not only check other storage loc, but also any files with the same name'''
        if Dataset.objects.filter(storage_loc=kwargs['dstpath']):
            return f'There is already a dataset at the location {kwargs["dstpath"]} you are renaming to'
        else:
            return False

    def process(self, **kwargs):
        """Fetch fresh storage_loc src dir here, then queue for move from there"""
        dset = Dataset.objects.get(pk=kwargs['dset_id'])
        if dset.storage_loc != kwargs['dstpath']:
            self.run_tasks = [((dset.storageshare.name, dset.storage_loc, kwargs['dstpath'],
                kwargs['dset_id'], [x.id for x in self.getfiles_query(**kwargs)]), {})]


class MoveDatasetServershare(DatasetJob):
    '''Moves all files associated to a dataset to another servershare, in one task.
    After all the files are done, delete them from src, and update dset.storage_loc
    '''
    refname = 'move_dset_servershare'
    task = tasks.rsync_dset_servershare

    def check_error(self, **kwargs):
        dset = Dataset.objects.values('pk', 'storage_loc').get(pk=kwargs['dset_id'])
        sfs = self.getfiles_query(**kwargs).values('path', 'servershare__name', 'sfile__filename', 'pk')
        if sfs.count() == 0:
            # Do not error on empty dataset, just skip
            return
        # FIXME due to getfiles_query being .filter(path=x, ..) this distinct and the error after
        # will never occur
        paths = sfs.distinct('path')
        if paths.count() > 1:
            return (f'Dataset {dset["pk"]} live files are spread over multiple paths and cannot '
                    f'be consolidated to {kwargs["dstsharename"]} under one path. '
                    f'Please group files first, to dset storage location {dset["storage_loc"]}')
        if paths.exists() and paths.get()['path'] != dset['storage_loc']:
            return (f'Dataset {dset["pk"]} storage location is different from paths of dset live '
                    'files. Please make sure files are in correct location, {dset["storage_loc"]}')
        sharename = sfs.first()['servershare__name']
        if sharename == kwargs['dstsharename']:
            return f'Cannot move dataset {dset["pk"]} to same share as its files are on, using this job'
        split_loc = os.path.split(dset['storage_loc'])
        if StoredFileLoc.objects.filter(path=split_loc[0], sfile__filename=split_loc[1],
                servershare__name=kwargs['dstsharename']).exists():
            return (f'Cannot move dataset {dset["pk"]} to {dset["storage_path"]} as there is already '
                    'a file by that name there')
        if StoredFileLoc.objects.exclude(pk__in=[x['pk'] for x in sfs]).filter(
                path=dset['storage_loc'], servershare__name=kwargs['dstsharename']).exists():
            return (f'Cannot move dataset {dset["pk"]} to {dset["storage_path"]} as there is already '
                    'file(s) stored in that exact directory. Please resolve first.')
        return False

    def process(self, **kwargs):
        dset = Dataset.objects.values('storage_loc').get(pk=kwargs['dset_id'])
        sfs = self.getfiles_query(**kwargs).values('path', 'servershare__name', 
                'servershare__share', 'servershare__server__fqdn', 'sfile__filename', 'pk')
        if sfs.count() == 0:
            # Do not error on empty dataset, just skip
            return
        rsync_sf = sfs.filter(sfile__deleted=False, purged=False, sfile__checked=True)
        firstsf = sfs.first()
        sharename = firstsf['servershare__name']
        src_controller_url = firstsf['servershare__server__fqdn']
        src_controller_share = firstsf['servershare__share']
        dst_controller_url = ServerShare.objects.get(name=kwargs['dstsharename']).server.fqdn
        self.run_tasks.append(((kwargs['dset_id'], sharename, dset['storage_loc'],
            src_controller_url, src_controller_share, dst_controller_url,
            kwargs['dstsharename'], [x['sfile__filename'] for x in rsync_sf], [x['pk'] for x in sfs]), {}))


class MoveFilesToStorage(DatasetJob):
    refname = 'move_files_storage'
    task = tasks.move_file_storage

    def getfiles_query(self, **kwargs):
        '''Get all files going to dataset (passed ids), but only those with 
        identical md5 as registered raw file (i.e. no mzMLs).
        Since these are pre-dataset files, we need to overwrite this getfiles method'''
        dset = Dataset.objects.values('storage_loc', 'storageshare_id').get(pk=kwargs['dset_id'])
        try:
            StoredFileLoc.objects.exclude(servershare_id=dset['storageshare_id'],
                path=dset['storage_loc']).filter(sfile__checked=True,
                sfile__rawfile__source_md5=F('sfile__md5'), sfile__rawfile_id__in=kwargs['rawfn_ids'])
        except Exception as e:
            print(e)
        return StoredFileLoc.objects.exclude(servershare_id=dset['storageshare_id'],
                path=dset['storage_loc']).filter(sfile__checked=True,
                sfile__rawfile__source_md5=F('sfile__md5'), sfile__rawfile_id__in=kwargs['rawfn_ids'])

    def check_error(self, **kwargs):
        dset = Dataset.objects.values('storage_loc', 'storageshare_id').get(pk=kwargs['dset_id'])
        dset_files = self.getfiles_query(**kwargs).values('sfile__filename')
        # check if dset with name of file to be passed there already exists
        # (e.g. dset run name ends in .raw)
        fpaths = [os.path.join(dset['storage_loc'], sf['sfile__filename']) for sf in dset_files]
        err_ds = Dataset.objects.filter(storage_loc__in=fpaths,
                storageshare_id=dset['storageshare_id'])
        if err_ds.exists():
            err_ds = err_ds.get()
            return (f'Cannot move selected files to path {dset["storage_loc"]} as there is an '
                    f'actual dataset (id:{err_ds.pk}, path:{err_ds.storage_loc} on that path '
                    'Consider renaming the dataset.')
        split_loc = os.path.split(dset['storage_loc'])
        if StoredFileLoc.objects.filter(path=split_loc[0], sfile__filename=split_loc[1],
                servershare_id=dset['storageshare_id']).exists():
            return (f'Cannot create dataset with path {dset["storage_loc"]} as there is an actual file '
                    'with that name there')
#        # check if dset creation possible (no file there with path/name == storageloc)
#        # FIXME this should be in the dsets basics storage
#        split_loc = os.path.split(dset['storage_loc'])
#        if StoredFile.objects.filter(path=split_loc[0], filename=split_loc[1],
#                servershare_id=dset['storageshare_id']).exists():
#            return (f'Cannot create dataset with path {dset["storage_loc"]} as there is an actual file '
#                    'with that name there')
        # check if already file in a dataset by that name:
        if StoredFileLoc.objects.filter(sfile__filename__in=[x['sfile__filename'] for x in dset_files],
                path=dset['storage_loc'], servershare_id=dset['storageshare_id']).exists():
            return (f'Cannot move files selected to dset {dset["storage_loc"]} as there is '
                    'already a file with that name there')
        return False

    def process(self, **kwargs):
        dstpath = Dataset.objects.values('storage_loc').get(pk=kwargs['dset_id'])['storage_loc']
        #dset_files = self.getfiles_query(**kwargs).exclude(path=dstpath)
        for fn in self.getfiles_query(**kwargs):
            # TODO check for diff os.path.join(sevrershare, dst_path), not just
            # path? Only relevant in case of cross-share moves.
            self.run_tasks.append(((fn.sfile.rawfile.name, fn.servershare.name, fn.path, dstpath, 
                fn.id, settings.PRIMARY_STORAGESHARENAME), {}))


class MoveFilesStorageTmp(DatasetJob):
    """Moves file from a dataset back to a tmp/inbox-like share"""
    refname = 'move_stored_files_tmp'
    task = False

    def getfiles_query(self, **kwargs):
        '''Select all files which are in dataset path, but not the purged ones, and
        filter out passed rawfiles'''
        return super().getfiles_query(**kwargs).select_related('sfile__filetype').filter(purged=False,
            sfile__rawfile_id__in=kwargs['fn_ids'])

    def check_error(self, **kwargs):
        sfs = self.getfiles_query(**kwargs).values('pk', 'sfile__filename')
        if StoredFileLoc.objects.exclude(pk__in=[x['pk'] for x in sfs]).filter(
                servershare__name=settings.TMPSHARENAME, path='',
                sfile__filename__in=[x['sfile__filename'] for x in sfs]):
            return (f'Cannot move files from dataset {kwargs["dset_id"]} to tmp storage, as there '
                    'exist file(s) with the same name there. Please resolve this before retrying')
        return False

    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).select_related('sfile__mzmlfile', 'servershare'):
            if hasattr(fn.sfile, 'mzmlfile'):
                fullpath = os.path.join(fn.path, fn.sfile.filename)
                self.run_tasks.append(((fn.servershare.name, fullpath, fn.id), {}, filetasks.delete_file))
            else:
                self.run_tasks.append(((fn.servershare.name, fn.sfile.filename, fn.path, fn.id), {}, tasks.move_stored_file_tmp))

    def queue_tasks(self):
        for task in self.run_tasks:
            args, kwargs, taskfun = task[0], task[1], task[2]
            tid = taskfun.delay(*args, **kwargs)
            self.create_db_task(tid, self.job_id, *args, **kwargs)


class ConvertDatasetMzml(DatasetJob):
    refname = 'convert_dataset_mzml'
    task = tasks.run_convert_mzml_nf
    revokable = True

    def getfiles_query(self, **kwargs):
        '''Return raw files only (from dset path)'''
        return super().getfiles_query(**kwargs).exclude(sfile__mzmlfile__isnull=False).select_related(
                'servershare', 'sfile__rawfile__datasetrawfile__dataset', 'sfile__filetype')

    def process(self, **kwargs):
        dset = Dataset.objects.get(pk=kwargs['dset_id'])
        pwiz = Proteowizard.objects.get(pk=kwargs['pwiz_id'])
        res_share = ServerShare.objects.get(pk=kwargs['dstshare_id'])
        nf_raws = []
        runpath = f'{dset.id}_convert_mzml_{kwargs["timestamp"]}'
        for fn in self.getfiles_query(**kwargs):
            mzmlfilename = os.path.splitext(fn.sfile.filename)[0] + '.mzML'
            mzsf, mzsfl = get_or_create_mzmlentry(fn, pwiz=pwiz, refined=False,
                    servershare_id=res_share.pk, path=runpath, mzmlfilename=mzmlfilename)
            if mzsf.checked and not mzsfl.purged:
                continue
            nf_raws.append((fn.servershare.name, fn.path, fn.sfile.filename, mzsfl.id, mzmlfilename))
        if not nf_raws:
            return
        # FIXME last file filetype decides mzml input filetype, we should enforce
        # same filetype files in a dataset if possible
        ftype = mzsf.filetype.name
        print(f'Queuing {len(nf_raws)} raw files for conversion')
        nfwf = NextflowWfVersionParamset.objects.select_related('nfworkflow').get(
                pk=pwiz.nf_version_id)
        run = {'timestamp': kwargs['timestamp'],
               'dset_id': dset.id,
               'wf_commit': nfwf.commit,
               'nxf_wf_fn': nfwf.filename,
               'repo': nfwf.nfworkflow.repo,
               'dstsharename': res_share.name,
               'runname': runpath,
               }
        params = ['--container', pwiz.container_version]
        for pname in ['options', 'filters']:
            p2parse = kwargs.get(pname, [])
            if len(p2parse):
                params.extend(['--{}'.format(pname), ';'.join(p2parse)])
        profiles = ','.join(nfwf.profiles)
        self.run_tasks.append(((run, params, nf_raws, ftype, nfwf.nfversion, profiles), {'pwiz_id': pwiz.id}))


class DeleteDatasetMzml(DatasetJob):
    """Removes dataset mzml files from active storage"""
    refname = 'delete_mzmls_dataset'
    task = filetasks.delete_file

    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).filter(sfile__deleted=True, purged=False, checked=True,
                mzmlfile__pwiz_id=kwargs['pwiz_id']):
            fullpath = os.path.join(fn.path, fn.sfile.filename)
            print('Queueing deletion of mzML file {fullpath} from dataset {kwargs["dset_id"]}')
            self.run_tasks.append(((fn.servershare.name, fullpath, fn.id), {}))


class DeleteActiveDataset(DatasetJob):
    """Removes dataset from active storage"""
    refname = 'delete_active_dataset'
    # FIXME need to be able to delete directories
    task = filetasks.delete_file

    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).select_related('sfile__filetype').filter(purged=False):
            fullpath = os.path.join(fn.path, fn.sfile.filename)
            print(f'Purging {fullpath} from dataset {kwargs["dset_id"]}')
            if hasattr(fn.sfile, 'mzmlfile'):
                is_folder = False
            else:
                is_folder = fn.sfile.filetype.is_folder
            self.run_tasks.append(((fn.servershare.name, fullpath, fn.id, is_folder), {}))


class BackupPDCDataset(DatasetJob):
    """Transfers all raw files in dataset to backup"""
    refname = 'backup_dataset'
    task = filetasks.pdc_archive
    
    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).exclude(sfile__mzmlfile__isnull=False).exclude(
                sfile__pdcbackedupfile__success=True, sfile__pdcbackedupfile__deleted=False).filter(
                        sfile__rawfile__datasetrawfile__dataset_id=kwargs['dset_id']):
            isdir = hasattr(fn.sfile.rawfile.producer, 'msinstrument') and fn.sfile.filetype.is_folder
            self.run_tasks.append((rsjobs.upload_file_pdc_runtask(fn, isdir=isdir), {}))


class ReactivateDeletedDataset(DatasetJob):
    refname = 'reactivate_dataset'
    task = filetasks.pdc_restore

    def getfiles_query(self, **kwargs):
        '''Reactivation will only be relevant for old datasets that will not just happen to get
        some new files, or old files removed. For this job to be retryable, fetch all files regardless
        of their servershare (which may change in server migrations).'''
        return StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset=kwargs['dset_id'])

    def process(self, **kwargs):
        for sfloc in self.getfiles_query(**kwargs).exclude(sfile__mzmlfile__isnull=False).filter(
                purged=True, sfile__pdcbackedupfile__isnull=False):
            self.run_tasks.append((rsjobs.restore_file_pdc_runtask(sfloc), {}))
        # Also set archived/archivable files which are already active (purged=False) to not deleted in UI
        # FIXME deleted=False should be done in view probably, higher up
        sfs = self.getfiles_query(**kwargs).filter(purged=False, sfile__deleted=True,
                sfile__pdcbackedupfile__isnull=False).values('sfile_id')
        StoredFile.objects.filter(pk__in=sfs).update(deleted=False)
        Dataset.objects.filter(pk=kwargs['dset_id']).update(
                storageshare=ServerShare.objects.get(name=settings.PRIMARY_STORAGESHARENAME))


class DeleteDatasetPDCBackup(DatasetJob):
    refname = 'delete_dataset_coldstorage'
    # TODO this job is not ready
    # should be agnostic of files in PDC, eg if no files found, loop length is zero
    # this for e.g empty or active-only dsets


def get_or_create_mzmlentry(fn, pwiz, refined, servershare_id, path, mzmlfilename):
    '''This also resets the path of the mzML file in case it's deleted'''
    new_md5 = f'mzml_{fn.rawfile.source_md5[5:]}'
    mzsf, cr = StoredFile.objects.get_or_create(mzmlfile__pwiz=pwiz, mzmlfile__refined=refined,
            rawfile_id=fn.rawfile_id, filetype_id=fn.filetype_id, defaults={'md5': new_md5,
                'filename': mzmlfilename})
    sfl, _ = StoredFileLoc.objects.get_or_create(sfile=mzsf, defaults={'servershare_id': servershare_id, 'path': path})
    if cr:
        MzmlFile.objects.create(sfile=mzsf, pwiz=pwiz, refined=refined)
    elif sfl.purged or not mzsf.checked:
        # Any previous mzML files which are deleted or otherwise odd need resetting
        # Only update in case of purged/non-checked, so cannot use update_or_create
        sfl.purged = False
        sfl.servershare_id = servershare_id
        sfl.path = path
        sfl.save()
        mzsf.checked = False
        mzsf.deleted = False
        mzsf.md5 = new_md5
        mzsf.save()
    return mzsf, sfl
