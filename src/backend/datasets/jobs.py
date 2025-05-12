import os

from django.db.models import F, Q
from django.urls import reverse

from kantele import settings
from rawstatus.models import StoredFile, ServerShare, StoredFileType, StoredFileLoc
from datasets.models import Dataset, DatasetRawFile, Project, DatasetServer
from analysis.models import Proteowizard, MzmlFile, NextflowWfVersionParamset
from datasets import tasks
from rawstatus import tasks as filetasks
from jobs.jobs import DatasetJob, ProjectJob, MultiFileJob
from rawstatus import models as rm


class RenameProject(ProjectJob):
    '''Uses task that does os.rename on project lvl dir. Needs also to update
    dsets / storedfiles to new path post job'''
    refname = 'rename_top_lvl_projectdir'
    task = tasks.rename_top_level_project_storage_dir
    queue = settings.QUEUE_STORAGE
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
    '''Renames dataset path in specific share, then updates storedfileloc for that dataset
    Calling this job needs to be checked for forbidden duplicate storage locs
    '''
    refname = 'rename_dset_storage_loc'
    task = tasks.rename_dset_storage_location
    retryable = False

    def process(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)
        srcloc = srcsfs.distinct('path', 'servershare__name').values('path', 'servershare__name')

        # Error check too many src locations for dataset:
        if srcloc.count() > 1:
            raise RuntimeError('Dataset source files are spread over more than one location, please '
                    'contact admin to make sure files are consolicated before renaming path')

        srcloc = srcloc.get()
        self.queue = f'{srcloc["servershare__name"]}__{settings.QUEUE_STORAGE}'
        self.run_tasks = [((srcloc["servershare__name"], srcloc['path'], kwargs['newpath'], 
            [x['pk'] for x in srcsfs.values('pk')], kwargs['dss_id']), {})]


class RsyncDatasetServershare(DatasetJob):
    '''Moves files associated to a dataset to another servershare, in one task.
    After all the files are done,, update dset.storage_loc
    '''
    refname = 'rsync_dset_files_to_servershare'
    queue = settings.QUEUE_FILE_DOWNLOAD
    task = filetasks.rsync_files_to_servershares

    def check_error_on_creation(self, **kwargs):
        '''This should check errors on creation, i.e. files crashing with other files etc,
        which means we cannot check for database fields reflecting an immediate current state (e.g. purged sfl)
        '''
        dst_dss = DatasetServer.objects.values('storage_loc_ui').get(pk=kwargs['dss_id'])
        return self._check_error_either(dst_dss['storage_loc_ui'], **kwargs)

    def _check_error_either(self, dstpath, **kwargs):
        srcsfl = self.getfiles_query(**kwargs).values('sfile__filename')
        for sfl in srcsfl:
            # Check for storage_loc_ui in the on_creation error check
            err_fpath = os.path.join(dstpath, sfl['sfile__filename'])
            err_dss = DatasetServer.objects.filter(storageshare_id=kwargs['dstshare_id'],
                    storage_loc_ui=err_fpath)
            if err_dss.exists():
                return (f'There is already a dataset with the exact path as the target file {err_fpath}, '
                        f'namely dataset {err_dss.values("pk").get()["pk"]}. Consider renaming the dataset.')
        if StoredFileLoc.objects.exclude(pk__in=kwargs['dstsfloc_ids']).filter(
                sfile__filename__in=[x['sfile__filename'] for x in srcsfl],
                path=dstpath, servershare_id=kwargs['dstshare_id'], active=True).exists():
            return ('There is already a file existing with the same name as a the target file'
                    f' in path {dstpath}')
        return False
            
    def check_error_on_running(self, **kwargs):
        dst_dss = DatasetServer.objects.values('storage_loc').get(pk=kwargs['dss_id'])
        return self._check_error_either(dst_dss['storage_loc'], **kwargs)

    def process(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)
        srcvals = ['servershare__name', 'path', 'servershare__server__fqdn']
        srcloc = srcsfs.distinct(*srcvals)

        # Error check too many src locations for dataset:
        if srcloc.count() > 1:
            raise RuntimeError('Dataset source files are spread over more than one location, please '
                    'contact admin to make sure files are consolicated before sync to a new location')
        dstshare = ServerShare.objects.values('pk', 'server__fqdn', 'name', 'share',
                'server__rsyncusername', 'server__rsynckeyfile').get(pk=kwargs['dstshare_id'])
        dst_dss = DatasetServer.objects.values('storage_loc').get(pk=kwargs['dss_id'])
        # Check if target sflocs already exist in a nonpurged state in wrong path?
        or_wrongloc_q = Q(path=dst_dss['storage_loc']) | Q(servershare_id=dstshare['pk'])
        all_dstsfs = StoredFileLoc.objects.filter(pk__in=kwargs['dstsfloc_ids']) 
        if all_dstsfs.filter(purged=False).exclude(or_wrongloc_q).exists():
            raise RuntimeError('There are existing target files in another location than the dataset'
                    ' - contact admin, make sure files are consolidated before sync to a new location')
        # Now run job
        if all_dstsfs.count() == 0:
            # Do not error on empty dataset, just skip
            return
        dstsfs = all_dstsfs.values('sfile__filename', 'pk')
        srcloc_vals = [srcloc.values(*srcvals).get()[x] for x in srcvals]
        self.run_tasks.append(((*srcloc_vals, dstshare['server__fqdn'], dstshare['name'],
            dst_dss['storage_loc'], dstshare['share'], dstshare['server__rsyncusername'], 
            dstshare['server__rsynckeyfile'], 
            [x['sfile__filename'] for x in srcsfs.values('sfile__filename')], kwargs['dstsfloc_ids']), {}))


class RemoveDatasetFilesFromServershare(DatasetJob):
    '''Moves all files associated to a dataset to another servershare, in one task.
    After all the files are done, delete them from src, and update dset.storage_loc
    '''
    refname = 'remove_dset_files_servershare'
    queue = False
    task = filetasks.delete_file

    def check_error(self, **kwargs):
        # FIXME testing wo errcheck
        return False

    def process(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)

        # Check if target sflocs already exist in a nonpurged state in wrong path?
        if srcsfs.filter(purged=False).count() < len(kwargs['sfloc_ids']):
            raise RuntimeError(f'Some files asked to delete for dataset/server {kwargs["dss_id"]} '
                    'do not exist, please contact admin')

        fields = ['servershare__name', 'path', 'sfile__filename', 'pk', 'sfile__mzmlfile',
                'sfile__filetype__is_folder']
        for sfl in srcsfs.values(*fields):
            queue = f'{sfl["servershare__name"]}__{settings.QUEUE_STORAGE}'
            if sfl['sfile__mzmlfile'] is not None:
                is_folder = False
            else:
                is_folder = sfl['sfile__filetype__is_folder']
            self.run_tasks.append(((*[sfl[x] for x in fields[:-2]], is_folder), {}, queue))



class ConvertDatasetMzml(DatasetJob):
    refname = 'convert_dataset_mzml'
    task = tasks.run_convert_mzml_nf
    queue = settings.QUEUE_NXF
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
    queue = settings.QUEUE_STORAGE

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
    queue = settings.QUEUE_STORAGE

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
    queue = settings.QUEUE_BACKUP
    
    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).exclude(sfile__mzmlfile__isnull=False).exclude(
                sfile__pdcbackedupfile__success=True, sfile__pdcbackedupfile__deleted=False).filter(
                        sfile__rawfile__datasetrawfile__dataset_id=kwargs['dset_id']):
            isdir = hasattr(fn.sfile.rawfile.producer, 'msinstrument') and fn.sfile.filetype.is_folder
            self.run_tasks.append((rsjobs.upload_file_pdc_runtask(fn, isdir=isdir), {}))


class ReactivateDeletedDataset(DatasetJob):
    refname = 'reactivate_dataset'
    task = filetasks.pdc_restore
    queue = settings.QUEUE_BACKUP

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
    queue = settings.QUEUE_BACKUP

    # TODO this job is not ready
    # should be agnostic of files in PDC, eg if no files found, loop length is zero
    # this for e.g empty or active-only dsets


def get_or_create_mzmlentry(fn, pwiz, refined, servershare_id, path, mzmlfilename):
    '''This also resets the path of the mzML file in case it's deleted'''
    new_md5 = f'mzml_{fn.sfile.rawfile.source_md5[5:]}'
    mzsf, cr = StoredFile.objects.get_or_create(mzmlfile__pwiz=pwiz, mzmlfile__refined=refined,
            rawfile_id=fn.sfile.rawfile_id, filetype_id=fn.sfile.filetype_id, defaults={'md5': new_md5,
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
