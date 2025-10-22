import os

from django.db.models import Q

from kantele import settings
from rawstatus.models import StoredFile, ServerShare, StoredFileLoc, FileServer, FileserverShare
from datasets.models import Dataset, DatasetServer
from analysis.models import Proteowizard, MzmlFile, NextflowWfVersionParamset
from datasets import tasks
from rawstatus import tasks as filetasks
from jobs.jobs import DatasetJob
from rawstatus import models as rm


class RenameDatasetStorageLoc(DatasetJob):
    '''Renames dataset path in specific share, then updates storedfileloc for that dataset
    Calling this job needs to be checked for forbidden duplicate storage locs
    '''
    refname = 'rename_dset_storage_loc'
    task = tasks.rename_dset_storage_location
    retryable = False

    def process(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)
        srcloc = srcsfs.distinct('path', 'servershare__name').values('path', 'servershare__pk')

        # Error check too many src locations for dataset:
        if srcloc.count() > 1:
            raise RuntimeError('Dataset source files are spread over more than one location, please '
                    'contact admin to make sure files are consolicated before renaming path')

        srcloc = srcloc.get()
        fss = FileserverShare.objects.filter(share=srcloc['servershare__pk'], server__active=True
                ).values('server__name', 'path').first()
        self.queue = self.get_server_based_queue(fss['server__name'], settings.QUEUE_STORAGE)
        self.run_tasks = [(fss['path'], srcloc['path'], kwargs['newpath'], 
            [x['pk'] for x in srcsfs.values('pk')], kwargs['dss_id'])]


class RsyncDatasetServershare(DatasetJob):
    '''Moves files associated to a dataset to another servershare, in one task.
    After all the files are done, update dset.storage_loc
    - dss_id is TARGET (destination) datasetserver, some files do not come from a dset
    '''
    refname = 'rsync_dset_files_to_servershare'
    queue = False
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
        srcsfl = self.oncreate_getfiles_query(**kwargs).values('sfile__filename')
        if StoredFileLoc.objects.filter(sfile__filename__in=[x['sfile__filename'] for x in srcsfl],
                path=dst_dss['storage_loc_ui'], servershare_id=kwargs['dstshare_id'], active=True).exists():
            return ('There is already a file existing with the same name as a target file'
                    f' in path {dst_dss["storage_loc_ui"]}')
        return self._check_error_either(srcsfl, dst_dss['storage_loc_ui'], **kwargs)

    def _check_error_either(self, srcsfl, dstpath, **kwargs):
        for sfl in srcsfl:
            # Check for storage_loc_ui in the on_creation error check
            err_fpath = os.path.join(dstpath, sfl['sfile__filename'])
            err_dss = DatasetServer.objects.filter(storageshare_id=kwargs['dstshare_id'],
                    storage_loc_ui=err_fpath)
            if err_dss.exists():
                return (f'There is already a dataset with the exact path as the target file {err_fpath}, '
                        f'namely dataset {err_dss.values("pk").get()["pk"]}. Consider renaming the dataset.')
        return False
            
    def check_error_on_running(self, **kwargs):
        srcsfl = self.getfiles_query(**kwargs).values('sfile__filename')
        dst_dss = DatasetServer.objects.values('storage_loc').get(pk=kwargs['dss_id'])
        if StoredFileLoc.objects.exclude(pk__in=kwargs['dstsfloc_ids']).filter(
                sfile__filename__in=[x['sfile__filename'] for x in srcsfl],
                path=dst_dss['storage_loc'], servershare_id=kwargs['dstshare_id'],
                active=True).exists():
            return ('There is already a file existing with the same name as a the target file'
                    f' in path {dst_dss["storage_loc"]}')
        return self._check_error_either(srcsfl, dst_dss['storage_loc'], **kwargs)

    def process(self, **kwargs):
        try:
            srcsfs = self.getfiles_query(**kwargs)
        except rm.StoredFileLoc.DoesNotExist:
            raise RuntimeError('Cannot find source files, maybe the storage share has been deactivated?')
        try:
            dstshare = ServerShare.objects.values('pk', 'name').get(pk=kwargs['dstshare_id'], active=True)
        except rm.ServerShare.DoesNotExist:
            raise RuntimeError('Cannot find specified destination share with ID '
                    f'{kwargs["dstshare_id"]}, maybe it is not configured correctly')

        srcvals = ['path', 'servershare_id']
        srcloc = srcsfs.distinct(*srcvals)

        # Error check too many src locations for dataset:
        # FIXME move to error checker
        if srcloc.count() > 1:
            raise RuntimeError('Dataset source files are spread over more than one location, please '
                    'contact admin to make sure files are consolicated before sync to a new location')
        dstshare = ServerShare.objects.values('pk', 'name').get(pk=kwargs['dstshare_id'])
        dst_dss = DatasetServer.objects.values('storage_loc', 'storageshare_id').get(pk=kwargs['dss_id'])
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
        srcserver, dstserver, rsyncservername, src_user, dst_user, rskey = self._select_rsync_server(srcloc_one['servershare_id'], kwargs['dstshare_id'])

        # Now run job
        self.queue = self.get_server_based_queue(rsyncservername, settings.QUEUE_STORAGE)
        srcpath = os.path.join(srcserver['path'], srcloc_one['path'])
        dstpath = os.path.join(dstserver['path'], dst_dss['storage_loc'])
        fns = {}
        for srcsfl in srcsfs.values('sfile__filename', 'sfile__mzmlfile', 'sfile__filetype__is_folder'):
            if srcsfl['sfile__mzmlfile']:
                isdir = False
            else:
                isdir = srcsfl['sfile__filetype__is_folder']
            fns[srcsfl['sfile__filename']] = isdir
        self.run_tasks.append((src_user, srcserver['server__fqdn'], srcpath, dst_user,
            dstserver['server__fqdn'], dstserver['path'], dst_dss['storage_loc'], rskey, fns,
            kwargs['dstsfloc_ids']))


class RemoveDatasetFilesFromServershare(DatasetJob):
    '''Delete all files associated to a dataset on a servershare'''
    refname = 'remove_dset_files_servershare'
    queue = False
    task = filetasks.delete_file

    def update_sourcefns_lastused(self, **kwargs):
        '''Normally this updates timestamp on files, but since they
        get deleted here, we do nothing'''
        pass

    def check_error_on_creation(self, **kwargs):
        srcsfs = self.oncreate_getfiles_query(**kwargs)
        if srcsfs.filter(active=True).count() < len(kwargs['sfloc_ids']):
            return (f'Some files asked to delete for dataset/server {kwargs["dss_id"]} '
                    'are marked as deleted already, please contact admin')
        return False

    def check_error_on_running(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)
        if srcsfs.filter(purged=False).count() < len(kwargs['sfloc_ids']):
            return (f'Some files asked to delete for dataset/server {kwargs["dss_id"]} '
                    'do not exist, please contact admin')
        return False

    def process(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)
        fields = ['servershare__name', 'path', 'sfile__filename', 'pk', 'sfile__mzmlfile',
                'sfile__filetype__is_folder', 'servershare__pk']
        for sfl in srcsfs.values(*fields):
            fss = FileserverShare.objects.filter(share=sfl['servershare__pk'], server__active=True,
                    share__active=True).values('server__name', 'path').first()
            queue = self.get_server_based_queue(fss['server__name'], settings.QUEUE_FASTSTORAGE)
            if sfl['sfile__mzmlfile'] is not None:
                is_folder = False
            else:
                is_folder = sfl['sfile__filetype__is_folder']
            self.run_tasks.append(((fss['path'], sfl['path'], sfl['sfile__filename'], sfl['pk'],
                is_folder), queue))


class ConvertDatasetMzml(DatasetJob):
    refname = 'convert_dataset_mzml'
    task = tasks.run_convert_mzml_nf
    queue = False
    revokable = True

    def on_create_addkwargs(self, **kwargs):
        '''Create target SFLs on local analysis server and final destination 
        dataset source. This is needed because the final sflocs (which are rsynced
        in the src dataset dir) will after that be rsynced to all the other 
        shares where the dataset is, and we need the ids for that job'''
        local_dst_sfls, remote_dst_sfls = [], []
        dst_sfls = []
        dss = DatasetServer.objects.values('storageshare_id', 'storage_loc', 'dataset_id').get(
                pk=kwargs['dss_id'])
        anasrcshareonserver = FileserverShare.objects.filter(server_id=kwargs['server_id'],
                server__active=True, share__active=True,
                share_id=dss['storageshare_id']).values('share_id', 'path').first()

        runpath = f'{dss["dataset_id"]}_convert_mzml_{kwargs["timestamp"]}'
        dstpath = os.path.join(anasrcshareonserver['path'], dss['storage_loc'])

        pwiz = Proteowizard.objects.get(pk=kwargs['pwiz_id'])
        for sfl in self.oncreate_getfiles_query(**kwargs):
            mzmlfilename = os.path.splitext(sfl.sfile.filename)[0] + '.mzML'
            mzsfl = get_or_create_mzmlentry(sfl.sfile, pwiz=pwiz, refined=False, path=dss['storage_loc'],
                    servershare_id=anasrcshareonserver['share_id'], mzmlfilename=mzmlfilename)
            if mzsfl:
                dst_sfls.append(mzsfl.pk)
            else:
                # This goes to system log, not user
                raise RuntimeError('Trying to create mzML that already seems to exist, '
                        f'{mzmlfilename}')
        return {'dstsfloc_ids': dst_sfls, 'runpath': runpath, 'dsspath': dss['storage_loc'],
                'srcsharepath': anasrcshareonserver['path']}

    def process(self, **kwargs):
        dss = DatasetServer.objects.values('storage_loc').get(pk=kwargs['dss_id'])
        try:
            anaserver = rm.AnalysisServerProfile.objects.get(server_id=kwargs['server_id'],
                server__active=True)
        except rm.AnalysisServerProfile.DoesNotExist:
            raise RuntimeError('Processing server requested does not exist or is not active or is '
                    'not capable of analysis')
        self.queue = self.get_server_based_queue(anaserver.queue_name, settings.QUEUE_NXF)
        sharemap = {fss['share_id']: fss['path'] for fss in rm.FileserverShare.objects.filter(
            share__active=True, server_id=kwargs['server_id']).values('share_id', 'path')}
        pwiz = Proteowizard.objects.get(pk=kwargs['pwiz_id'])
        nf_raws = []
        srcpath = os.path.join(kwargs['srcsharepath'], dss['storage_loc'])
        for fn in self.getfiles_query(**kwargs).values('sfile__rawfile_id', 'sfile__filename'):
            # Have to line up the sfl with their dst mzml sfl ids, so we cant just oneline it
            mzsfl = StoredFileLoc.objects.values('pk', 'sfile__filename', 'sfile__filetype__name',
                    'servershare_id').get(pk__in=kwargs['dstsfloc_ids'],
                            sfile__rawfile_id=fn['sfile__rawfile_id'])
            nf_raws.append((srcpath, fn['sfile__filename'], mzsfl['pk'], mzsfl['sfile__filename']))
        if not nf_raws:
            return
        print(f'Queuing {len(nf_raws)} raw files for conversion')
        nfwf = NextflowWfVersionParamset.objects.select_related('nfworkflow').get(
                pk=pwiz.nf_version_id)
        run = {
               'wf_commit': nfwf.commit,
               'nxf_wf_fn': nfwf.filename,
               'repo': nfwf.nfworkflow.repo,
               'runname': kwargs['runpath'],
               'server_id': anaserver.server_id,
               'outsharepath': sharemap[mzsfl['servershare_id']],
               'dsspath': kwargs['dsspath'],
               }
        params = ['--md5out']
        params.extend([x for y in [(f'--{k}', v) for k,v in pwiz.params.items()] for x in y])
        for pname in ['options', 'filters']:
            p2parse = kwargs.get(pname, [])
            if len(p2parse):
                params.extend(['--{}'.format(pname), ';'.join(p2parse)])
        self.run_tasks.append((run, params, nf_raws, mzsfl['sfile__filetype__name'],
            nfwf.nfversion, ','.join(anaserver.nfprofiles), anaserver.scratchdir))


class DeleteDatasetPDCBackup(DatasetJob):
    refname = 'delete_dataset_coldstorage'
    queue = settings.QUEUE_BACKUP

    # TODO this job is not ready
    # should be agnostic of files in PDC, eg if no files found, loop length is zero
    # this for e.g empty or active-only dsets


def get_or_create_mzmlentry(sfile, pwiz, refined, servershare_id, path, mzmlfilename):
    '''Called when creating mzML files, will return (sfile, sfloc) tuple. The sfl will
    be on an analysis output servershare with an analysis runpath.
    '''
    new_md5 = f'mzml_{sfile.rawfile.source_md5[5:]}'
    mzsf, cr = StoredFile.objects.get_or_create(mzmlfile__pwiz=pwiz, mzmlfile__refined=refined,
            rawfile_id=sfile.rawfile_id, filetype_id=sfile.filetype_id, defaults={'md5': new_md5,
                'filename': mzmlfilename})
    # Update sfl path if old (inactive) exists, since new job -> new runpath
    # Do not set active here, first check if that is already the case below, to detect
    # existing files to skip
    sfl, sfl_cr = StoredFileLoc.objects.update_or_create(sfile=mzsf, servershare_id=servershare_id,
            defaults={'path': path})
    if cr:
        MzmlFile.objects.create(sfile=mzsf, pwiz=pwiz, refined=refined)
    elif not sfl_cr and sfl.active:
        # Old file exists or is going to be made in a job, skip it. This will make job error
        # but it probably should do that, as it may occur when you delete a job
        # FIXME now we really need an on_delete/on_create thing on the job, to reset dst_sflocs
        return False
    elif not sfl_cr:
        # Any previous mzML files which are (set to be) deleted or otherwise odd (not checked)
        # need resetting. Only update in case of non-active/checked, so cannot use update_or_create
        sfl.active = True
        sfl.save()
        mzsf.checked = False
        mzsf.deleted = False
        mzsf.md5 = new_md5
        mzsf.save()
    return sfl
