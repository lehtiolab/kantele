import os

from django.db.models import F, Q
from django.urls import reverse

from kantele import settings
from rawstatus.models import StoredFile, ServerShare, StoredFileType, StoredFileLoc, FileServer, FileserverShare
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
    After all the files are done, update dset.storage_loc
    - dss_id is TARGET (destination) datasetserver, some files do not come from a dset
    '''
    refname = 'rsync_dset_files_to_servershare'
    queue = settings.QUEUE_FILE_DOWNLOAD
    task = filetasks.rsync_files_to_servershares

    def on_create_addkwargs(self, **kwargs):
        '''Create destination sfloc db rows'''
        dstdss = DatasetServer.objects.values('storage_loc_ui', 'storageshare_id').get(pk=kwargs['dss_id'])
        dstsfls = []
        for sfl in self.oncreate_getfiles_query(**kwargs):
            dstsfl, _ = StoredFileLoc.objects.update_or_create(sfile=sfl.sfile,
                    servershare_id=dstdss['storageshare_id'],
                    defaults={'path': dstdss['storage_loc_ui'], 'active': True})
            dstsfls.append(dstsfl.pk)
        return {'dstsfloc_ids': dstsfls}

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
        srcvals = ['path', 'servershare_id']
        srcloc = srcsfs.distinct(*srcvals)

        # Error check too many src locations for dataset:
        # FIXME move to error checker
        if srcloc.count() > 1:
            raise RuntimeError('Dataset source files are spread over more than one location, please '
                    'contact admin to make sure files are consolicated before sync to a new location')
        dstshare = ServerShare.objects.values('pk', 'name').get(pk=kwargs['dstshare_id'])
        dst_dss = DatasetServer.objects.values('storage_loc').get(pk=kwargs['dss_id'])
        # Check if target sflocs already exist in a nonpurged state in wrong path?
        or_wrongloc_q = Q(path=dst_dss['storage_loc']) | Q(servershare_id=dstshare['pk'])
        all_dstsfs = StoredFileLoc.objects.filter(pk__in=kwargs['dstsfloc_ids']) 
        if all_dstsfs.filter(purged=False).exclude(or_wrongloc_q).exists():
            raise RuntimeError('There are existing target files in another location than the dataset'
                    ' - contact admin, make sure files are consolidated before sync to a new location')
        if all_dstsfs.count() == 0:
            # Do not error on empty dataset, just skip
            return
        # Select file servers to rsync from/to
        srcloc_one = srcloc.values(*srcvals).get()
        servers = FileserverShare.objects.filter(share_id__in=[srcloc_one['servershare_id'],
                kwargs['dstshare_id']])
        rsync_server_q = servers.filter(server__can_rsync=True)
        if singleserver := FileServer.objects.filter(fileservershare__share=kwargs['dstshare_id']
                ).filter(fileservershare__share=srcloc_one['servershare_id'], can_rsync=True):
            # Try to get both shares from same server? (Rsync can skip SSH then)
            srcserver = FileserverShare.objects.filter(server_id__in=singleserver,
                    share=srcloc_one['servershare_id']
                    ).values('server__fqdn', 'server__name', 'path').first()
            dstserver = FileserverShare.objects.filter(server_id__in=singleserver,
                    share=kwargs['dstshare_id']
                    ).values('server__fqdn', 'server__name', 'path').first()
            src_user = dst_user = rskey = False
            rsyncservername = srcserver['server__name']
        elif rsync_server_q.filter(share_id=srcloc_one['servershare_id']).exists():
            # rsyncing server has src file, push to remote
            srcserver = rsync_server_q.filter(share_id=srcloc_one['servershare_id']).values(
                    'server__fqdn', 'path', 'server__name').first()
            dstserver = servers.filter(share_id=kwargs['dstshare_id']).values('server__fqdn'
                    , 'path', 'server__rsynckeyfile', 'server__rsyncusername', 'pk').first()
            rsyncservername = srcserver['server__name']
            dst_user, rskey = dstserver['server__rsyncusername'], dstserver['server__rsynckeyfile']
            src_user = False
        elif rsync_server_q.filter(share_id=kwargs['dstshare_id']).exists():
            # rsyncing server is the dst, pull from remote
            dstserver = rsync_server_q.values('server__fqdn', 'path', 'server__name', 'pk').first()
            srcserver = servers.filter(share_id=srcloc_one['servershare_id']).values('server__fqdn',
                    'path', 'server__rsynckeyfile', 'server__rsyncusername').first()
            rsyncservername = dstserver['server__name']
            src_user, rskey = srcserver['server__rsyncusername'], srcserver['server__rsynckeyfile']
            dst_user = False
        else:
            # FIXME error needs finding in error check already
            raise RuntimeError('Could not get file share on any rsync capable controller server')
        self.queue = self.get_server_based_queue(rsyncservername, settings.QUEUE_STORAGE)
        # Now run job
        srcpath = os.path.join(srcserver['path'], srcloc_one['path'])
        dstpath = os.path.join(dstserver['path'], dst_dss['storage_loc'])
        self.run_tasks.append((src_user, srcserver['server__fqdn'], srcpath, dst_user,
            dstserver['server__fqdn'], dstserver['path'], dst_dss['storage_loc'], rskey,
            [x['sfile__filename'] for x in srcsfs.values('sfile__filename')], kwargs['dstsfloc_ids']))


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
                'sfile__filetype__is_folder', 'servershare__pk']
        for sfl in srcsfs.values(*fields):
            server = FileServer.objects.filter(fileservershare__share=sfl['servershare__pk']
                    ).values('name').first()
            queue = self.get_server_based_queue(server['name'], settings.QUEUE_FASTSTORAGE)
            if sfl['sfile__mzmlfile'] is not None:
                is_folder = False
            else:
                is_folder = sfl['sfile__filetype__is_folder']
            self.run_tasks.append(((*[sfl[x] for x in fields[:-3]], is_folder), queue))


class ConvertDatasetMzml(DatasetJob):
    refname = 'convert_dataset_mzml'
    task = tasks.run_convert_mzml_nf
    queue = False
    revokable = True

    def on_create_addkwargs(self, **kwargs):
        # FIXME create dst sfloc ids etc
        pwiz = Proteowizard.objects.get(pk=kwargs['pwiz_id'])
        dst_sfls = []
        for sfl in self.getfiles_query(**kwargs):
            mzmlfilename = os.path.splitext(sfl.sfile.filename)[0] + '.mzML'
            mzsf, mzsfl = get_or_create_mzmlentry(sfl, pwiz=pwiz, refined=False,
                servershare_id=sfl.servershare_id, path=sfl.path, mzmlfilename=mzmlfilename)
            if mzsf:
                dst_sfls.append(mzsfl.pk)
        return {'dstsfloc_ids': dst_sfls}

    def process(self, **kwargs):
        dss = DatasetServer.objects.values('storageshare__name', 'storageshare_id', 'storage_loc',
                'dataset_id').get(pk=kwargs['dss_id'])
        ana_server = FileServer.objects.filter(is_analysis=True,
                fileservershare__share__id=dss['storageshare_id']).values('name', 'scratchdir').first()
        pwiz = Proteowizard.objects.get(pk=kwargs['pwiz_id'])
        runpath = f'{dss["dataset_id"]}_convert_mzml_{kwargs["timestamp"]}'
        nf_raws = []
        for fn in self.getfiles_query(**kwargs).values('sfile__rawfile_id', 'sfile__filename'):
            # Have to line up the sfl with their dst mzml sfl ids, so we cant just oneline it
            mzsfl = StoredFileLoc.objects.values('pk', 'sfile__filename', 'sfile__filetype__name'
                    ).get(pk__in=kwargs['dstsfloc_ids'], sfile__rawfile_id=fn['sfile__rawfile_id'])
            nf_raws.append((dss['storageshare__name'], dss['storage_loc'], fn['sfile__filename'],
                mzsfl['pk'], mzsfl['sfile__filename']))
        if not nf_raws:
            return
        print(f'Queuing {len(nf_raws)} raw files for conversion')
        nfwf = NextflowWfVersionParamset.objects.select_related('nfworkflow').get(
                pk=pwiz.nf_version_id)
        run = {
               'wf_commit': nfwf.commit,
               'nxf_wf_fn': nfwf.filename,
               'repo': nfwf.nfworkflow.repo,
               'dstsharename': dss['storageshare__name'],
               'dstpath': dss['storage_loc'],
               'runname': runpath,
               }
        params = ['--container', pwiz.container_version]
        for pname in ['options', 'filters']:
            p2parse = kwargs.get(pname, [])
            if len(p2parse):
                params.extend(['--{}'.format(pname), ';'.join(p2parse)])
        self.queue = self.get_server_based_queue(ana_server['name'], settings.QUEUE_NXF)
        self.run_tasks.append((run, params, nf_raws, mzsfl['sfile__filetype__name'],
            nfwf.nfversion, ','.join(nfwf.profiles), ana_server['scratchdir']))


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
    sfl, sfl_cr = StoredFileLoc.objects.get_or_create(sfile=mzsf, servershare_id=servershare_id,
            defaults={'path': path})
    if cr:
        MzmlFile.objects.create(sfile=mzsf, pwiz=pwiz, refined=refined)
    elif not sfl_cr and sfl.active:
        # Old file exists or is going to be made in a job, skip it
        # FIXME now we really need an on_delete/on_create thing on the job, to reset dst_sflocs
        return False, False
    elif not sfl_cr:
        # Any previous mzML files which are (set to be) deleted or otherwise odd (not checked)
        # need resetting. Only update in case of non-active/checked, so cannot use update_or_create
        sfl.active = True
        sfl.save()
        mzsf.checked = False
        mzsf.deleted = False
        mzsf.md5 = new_md5
        mzsf.save()
    return mzsf, sfl
