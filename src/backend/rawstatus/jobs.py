import os
import requests
from urllib.parse import urlsplit, urlparse, urljoin
import ftplib
from datetime import datetime

from rawstatus import tasks
from rawstatus import models as rm
from datasets import tasks as dstasks
from datasets import models as dm
from datasets.jobs import RsyncDatasetServershare, RemoveDatasetFilesFromServershare
from kantele import settings
from jobs.jobs import SingleFileJob, MultiFileJob, DatasetJob


def create_upload_dst_web(rfid, ftype):
    '''To create path for uploaded files'''
    return os.path.join(settings.TMP_UPLOADPATH, f'{rfid}.{ftype}')

def get_host_upload_dst(web_dst):
    '''To get path for uploaded files on the host (not the container)'''
    return web_dst.replace(settings.TMP_UPLOADPATH, settings.HOST_UPLOADDIR, 1)


class RsyncOtherFileServershare(SingleFileJob):
    '''Does the same as rsync dset, but for when files are not in a dataset

    Dst path will be identical to src path, if not given.
    '''
    refname = 'rsync_otherfiles_to_servershare'
    queue = False
    task = tasks.rsync_files_to_servershares

    def get_dsids_jobrunner(self, **kwargs):
        return []

    def get_rf_ids_for_filejobs(self, **kwargs):
        """This is run before running job, to define files used by
        the job (so it cant run if if files are in use by other job)"""
        return [x['sfile__rawfile_id'] for x in self.oncreate_getfiles_query(**kwargs).values('sfile__rawfile_id')]

    def on_create_addkwargs(self, **kwargs):
        '''Create destination sfloc db rows'''
        if kwargs.get('dstsfloc_id', False):
            dstsfl = rm.StoredFileLoc.objects.get(pk=kwargs['dstsfloc_id'])
            return {'dstshare_id': dstsfl.servershare_id}
        else:
            sfl = self.oncreate_getfiles_query(**kwargs).values('sfile_id', 'path').get()
            dstpath = kwargs.get('dstpath', sfl['path'])
            dstsfl, _ = rm.StoredFileLoc.objects.update_or_create(sfile_id=sfl['sfile_id'],
                    servershare_id=kwargs['dstshare_id'], defaults={'path': dstpath, 'active': True})
            return {'dstsfloc_id': dstsfl.pk, 'dstshare_id': dstsfl.servershare_id}

    def check_error_on_creation(self, **kwargs):
        '''This should check errors on creation, i.e. files crashing with other files etc,
        which means we cannot check for database fields reflecting an immediate current state (e.g. purged sfl)
        '''
        srcsfl = self.oncreate_getfiles_query(**kwargs).values('sfile__filename', 'path').get()
        dstpath = kwargs.get('dstpath', srcsfl['path'])
        if rm.StoredFileLoc.objects.filter(sfile__filename=srcsfl['sfile__filename'],
                path=dstpath, servershare_id=kwargs['dstshare_id'], active=True).exists():
            return ('There is already a file existing with the same name as a the target file'
                    f' in path {srcsfl["path"]}')
        return self._check_error_either(srcsfl, **kwargs)

    def _check_error_either(self, srcsfl, **kwargs):
        # Check for storage_loc_ui in the on_creation error check
        err_fpath = os.path.join(srcsfl['path'], srcsfl['sfile__filename'])
        err_dss = dm.DatasetServer.objects.filter(storageshare_id=kwargs['dstshare_id'],
                storage_loc_ui=err_fpath)
        if err_dss.exists():
            return (f'There is already a dataset with the exact path as the target file {err_fpath}, '
                    f'namely dataset {err_dss.values("pk").get()["pk"]}. Consider renaming the dataset.')
        else:
            return False
            
    def check_error_on_running(self, **kwargs):
        srcsfl = self.getfiles_query(**kwargs).values('sfile__filename', 'path').get()
        if rm.StoredFileLoc.objects.exclude(pk=kwargs['dstsfloc_id']).filter(
                sfile__filename=srcsfl['sfile__filename'], path=srcsfl['path'],
                servershare_id=kwargs['dstshare_id'], active=True).exists():
            return ('There is already a file existing with the same name as a the target file'
                    f' in path {srcsfl["path"]}')
        return self._check_error_either(srcsfl, **kwargs)

    def process(self, **kwargs):
        srcsfl = self.getfiles_query(**kwargs).values('sfile__filename', 'path', 'servershare_id').get()
        dstshare = rm.ServerShare.objects.values('pk', 'name').get(pk=kwargs['dstshare_id'])

        # Check if target sflocs already exist in a nonpurged state in wrong path?
        dstsfl = rm.StoredFileLoc.objects.filter(pk=kwargs['dstsfloc_id'])
        if dstsfl.filter(purged=False).exclude(servershare_id=dstshare['pk']).exists():
            raise RuntimeError('There is an existing target file in another location than the '
            'dataset'
                    ' - contact admin, make sure files are consolidated before sync to a new location')
        if not dstsfl.exists():
            # skip FIXME is this ok? please document why
            return

        # Select file servers to rsync from/to - FIXME share code!
        servers = rm.FileserverShare.objects.filter(share_id__in=[srcsfl['servershare_id'],
                kwargs['dstshare_id']])
        rsync_server_q = servers.filter(server__can_rsync_remote=True)
        if singleserver := rm.FileServer.objects.filter(fileservershare__share=kwargs['dstshare_id']
                ).filter(fileservershare__share=srcsfl['servershare_id'], can_rsync_remote=True):
            # Try to get both shares from same server, rsync can skip SSH then
            srcserver = rm.FileserverShare.objects.filter(server_id__in=singleserver,
                    share=srcsfl['servershare_id']
                    ).values('server__fqdn', 'server__name', 'path').first()
            dstserver = rm.FileserverShare.objects.filter(server_id__in=singleserver,
                    share=kwargs['dstshare_id']
                    ).values('server__fqdn', 'server__name', 'path').first()
            src_user = dst_user = rskey = False
            rsyncservername = srcserver['server__name']
        elif rsync_server_q.filter(share_id=srcsfl['servershare_id']).exists():
            # rsyncing server has src file, push to remote
            srcserver = rsync_server_q.filter(share_id=srcsfl['servershare_id']).values(
                    'server__fqdn', 'path', 'server__name').first()
            dstserver = servers.filter(share_id=kwargs['dstshare_id']).values('server__fqdn'
                    , 'path', 'server__rsynckeyfile', 'server__rsyncusername', 'pk').first()
            rsyncservername = srcserver['server__name']
            dst_user, rskey = dstserver['server__rsyncusername'], dstserver['server__rsynckeyfile']
            src_user = False
        elif rsync_server_q.filter(share_id=kwargs['dstshare_id']).exists():
            # rsyncing server is the dst, pull from remote
            dstserver = rsync_server_q.values('server__fqdn', 'path', 'server__name', 'pk').first()
            srcserver = servers.filter(share_id=srcsfl['servershare_id']).values('server__fqdn',
                    'path', 'server__rsynckeyfile', 'server__rsyncusername').first()
            rsyncservername = dstserver['server__name']
            src_user, rskey = srcserver['server__rsyncusername'], srcserver['server__rsynckeyfile']
            dst_user = False
        else:
            # FIXME error needs finding in error check already
            raise RuntimeError('Could not get file share on any rsync capable controller server')
        self.queue = self.get_server_based_queue(rsyncservername, settings.QUEUE_STORAGE)
        # Now run job
        srcpath = os.path.join(srcserver['path'], srcsfl['path'])
        dstpath = dstsfl.values('path').get()['path']
        self.run_tasks.append((src_user, srcserver['server__fqdn'], srcpath, dst_user,
            dstserver['server__fqdn'], dstserver['path'], dstpath, rskey,
            [srcsfl['sfile__filename']], [kwargs['dstsfloc_id']]))


class RemoveFilesFromServershare(RemoveDatasetFilesFromServershare):
    '''Does the same as rsync dset, but for when files are not in a dataset
    '''
    refname = 'purge_files'

    def check_error_on_creation(self, **kwargs):
        srcsfs = self.oncreate_getfiles_query(**kwargs)
        if srcsfs.filter(active=True).count() < len(kwargs['sfloc_ids']):
            return ('Some files asked to delete are marked as deleted already, please contact admin')
        return False

    def check_error_on_running(self, **kwargs):
        srcsfs = self.getfiles_query(**kwargs)
        if srcsfs.filter(purged=False).count() < len(kwargs['sfloc_ids']):
            return ('Some files asked to delete for dataset/server do not exist, please contact admin')
        return False

    def get_dsids_jobrunner(self, **kwargs):
        return []

    def get_rf_ids_for_filejobs(self, **kwargs):
        """This is run before running job, to define files used by
        the job (so it cant run if if files are in use by other job)"""
        return [x['sfile__rawfile_id'] for x in self.getfiles_query(**kwargs).values('sfile__rawfile_id')]


class RsyncFileTransferFromWeb(SingleFileJob):
    refname = 'rsync_transfer_fromweb'
    task = tasks.rsync_transfer_file_web
    queue = settings.QUEUE_WEB_RSYNC

    def getfiles_query(self, **kwargs):
        return self.oncreate_getfiles_query(**kwargs).filter(purged=True)

    def check_error(self, **kwargs):
        src_sfloc = self.getfiles_query(**kwargs).values('pk', 'path', 'sfile__filename',
                'servershare_id', 'servershare__name').get()
        if rm.StoredFileLoc.objects.exclude(pk=src_sfloc['pk']).filter(
                sfile__filename=src_sfloc['sfile__filename'], path=src_sfloc['path'],
                servershare_id=src_sfloc['servershare_id']).exists():
            return (f'Cannot rsync file {src_sfloc["pk"]} to location {src_sfloc["servershare__name"]} / '
                    f'{src_sfloc["path"]} as another file with the same name is already there')
        else:
            return False

    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs).select_related('sfile').get()
        dstpath = os.path.join(sfloc.path, sfloc.sfile.filename)
        fss = rm.FileserverShare.objects.filter(share=sfloc.servershare).first()
        self.run_tasks.append((sfloc.pk, get_host_upload_dst(kwargs['src_path']),
            fss.path, dstpath, sfloc.servershare.name, sfloc.sfile.filetype.is_folder,
            sfloc.sfile.filetype.stablefiles))


class CreatePDCArchive(SingleFileJob):
    '''Archiving of newly arrived files - full datasets can also be archived if they
    are not - then we use the BackupDataset job instead'''
    refname = 'create_pdc_archive'
    task = tasks.pdc_archive
    queue = settings.QUEUE_BACKUP

    def process(self, **kwargs):
        taskargs = upload_file_pdc_runtask(self.getfiles_query(**kwargs).get(), isdir=kwargs['isdir'])
        if taskargs:
            self.run_tasks.append(taskargs)
            print('PDC archival task queued')


class RestoreFromPDC(SingleFileJob):
    '''For restoring files which are not in a dataset'''
    refname = 'restore_from_pdc_archive'
    task = tasks.pdc_restore
    queue = settings.QUEUE_BACKUP

    def check_error_on_creation(self, **kwargs):
        if not self.oncreate_getfiles_query(**kwargs).filter(
                servershare__fileservershare__server__can_backup=True):
            return 'Cannot retrieve file to specified location: no backup server connected there'

    def process(self, **kwargs):
        '''Path must come from storedfile itself, not its dataset, since it
        can be a file without a dataset'''
        sfloc = self.getfiles_query(**kwargs)
        self.run_tasks.append(restore_file_pdc_runtask(sfloc))
        print('PDC restore task queued')


class BackupPDCDataset(DatasetJob):
    # Deprecate, files are backed up when incoming
    """Transfers all raw files in dataset to backup"""
    refname = 'backup_dataset'
    task = tasks.pdc_archive
    queue = settings.QUEUE_BACKUP
    
    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).exclude(sfile__mzmlfile__isnull=False).exclude(
                sfile__pdcbackedupfile__success=True, sfile__pdcbackedupfile__deleted=False):
            isdir = hasattr(fn.sfile.rawfile.producer, 'msinstrument') and fn.sfile.filetype.is_folder
            self.run_tasks.append(upload_file_pdc_runtask(fn, isdir=isdir))


class ReactivateDeletedDataset(DatasetJob):
    '''Reactivation to fetch a dataset from the backup.
    Datasets are placed in whatever is passed but that is likely
    a sens tmp share from which they can be uploaded to e.g. PX,
    rsynced to open, or to analysis cluster'''

    refname = 'reactivate_dataset'
    task = tasks.pdc_restore
    queue = settings.QUEUE_BACKUP

    def getfiles_query(self, **kwargs):
        '''In this case, the sflocs are dst files and will have purged=True instead of False'''
        return self.oncreate_getfiles_query(**kwargs).filter(purged=True)

    def process(self, **kwargs):
        for sfloc in self.getfiles_query(**kwargs).select_related('sfile'):
            self.run_tasks.append(restore_file_pdc_runtask(sfloc))


class RenameFile(SingleFileJob):
    refname = 'rename_file'
    task = tasks.rename_file
    queue = False
    retryable = False
    """Only renames file inside same path/server. Does not move cross directories. Does not change extensions.
    Updates RawFile in job instead of view since jobs are processed in a single queue. StoredFile names are
    updated in the post job view.
    It will also rename all mzML attached converted files.
    """

    def check_error_on_creation(self, **kwargs):
        src_sf = self.oncreate_getfiles_query(**kwargs).get()
        fullnewpath = os.path.join(src_sf.path, kwargs["newname"])
        if rm.StoredFileLoc.objects.filter(sfile__filename=kwargs['newname'], path=src_sf.path,
                servershare_id=src_sf.servershare_id).exists():
            return (f'A file in path {src_sf.path} with name {kwargs["newname"]} already exists. '
                    'Please choose another name.')
        elif dm.DatasetServer.objects.filter(storage_loc=fullnewpath,
                storageshare_id=src_sf.servershare_id).exists():
            return f'A dataset with the same directory name as your new file name {fullnewpath} already exists'
        else:
            return False
        
    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs).select_related('sfile').get()
        fss = rm.FileserverShare.objects.filter(share=sfloc.servershare).values('server__name',
                'path').first()
        self.queue = self.get_server_based_queue(fss['server__name'], settings.QUEUE_FASTSTORAGE)
        self.run_tasks.append((sfloc.sfile.filename, fss['path'], sfloc.path,
            sfloc.path, sfloc.id, kwargs["newname"]))


class ClassifyMSRawFile(SingleFileJob):
    refname = 'classify_msrawfile'
    task = tasks.classify_msrawfile

    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs).values('pk', 'sfile__filetype__name',
                'servershare_id', 'path', 'sfile__filename').get()
        fss = rm.FileserverShare.objects.filter(share=sfloc['servershare_id']).values('server__name',
                'path').first()

        self.queue = self.get_server_based_queue(fss['server__name'], settings.QUEUE_FASTSTORAGE)
        self.run_tasks.append((kwargs['token'], sfloc['pk'], sfloc['sfile__filetype__name'],
            fss['path'], sfloc['path'], sfloc['sfile__filename']))


class MoveSingleFile(SingleFileJob):
    '''Move file from one share/path to another. Technically the same as rename, as you can
    also specify a new filename, but this job is not exposed to the user, and only used
    internally for moving files to a predestined path (i.e. incoming mzML, QC raw files, library etc'''
    refname = 'move_single_file'
    task = dstasks.move_file_storage
    queue = settings.QUEUE_STORAGE

    def check_error(self, **kwargs):
        '''Check for file name collisions'''
        src_sfloc = self.getfiles_query(**kwargs)
        if dstsharename := kwargs.get('dstsharename'):
            sshare = rm.ServerShare.objects.filter(name=dstsharename).get()
        else:
            sshare = src_sfloc.servershare
        newfn = kwargs.get('newname', src_sfloc.sfile.filename)
        fullnewpath = os.path.join(kwargs['dst_path'], newfn)
        if rm.StoredFileLoc.objects.filter(sfile__filename=newfn, path=kwargs['dst_path'],
                servershare=sshare, sfile__deleted=False, purged=False).exists():
            return (f'A file in path {kwargs["dst_path"]} with name {src_sfloc.sfile.filename} '
                    'already exists. Please choose another name.')
        elif dm.Dataset.objects.filter(storage_loc=fullnewpath, storageshare=sshare,
                deleted=False, purged=False).exists():
            return f'A dataset with the same directory name as your new file location {fullnewpath} already exists'
        else:
            return False

    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs)
        taskkwargs = {x: kwargs[x] for x in ['newname'] if x in kwargs}
        dstsharename = kwargs.get('dstsharename') or sfloc.servershare.name
        self.run_tasks.append(((
            sfloc.sfile.filename, sfloc.servershare.name,
            sfloc.path, kwargs['dst_path'], sfloc.pk, dstsharename), taskkwargs))


class DeleteEmptyDirectory(MultiFileJob):
    """Check first if all the sfids are set to purged, indicating the dir is actually empty.
    Then queue a task. The sfids also make this job dependent on other jobs on those, as in
    the file-purging tasks before this directory deletion"""
    refname = 'delete_empty_directory'
    task = tasks.delete_empty_dir
    queue = False

    def check_error_on_creation(self, **kwargs):
        if rm.StoredFileLoc.objects.filter(servershare_id=kwargs['share_id'], path=kwargs['path'],
                active=True):
            ssname = rm.ServerShare.objects.filter(pk=kwargs['share_id']).values('name').get()['name']
            return f'Cannot create job to delete dir {kwargs["path"]} on {ssname}, there are files in it'

    def check_error_on_running(self, **kwargs):
        if rm.StoredFileLoc.objects.filter(servershare_id=kwargs['share_id'], path=kwargs['path'],
                purged=False):
            ssname = rm.ServerShare.objects.filter(pk=kwargs['share_id']).values('name').get()['name']
            return f'Cannot queue job to delete dir {kwargs["path"]} on {ssname}, there are files in it'

    def process(self, **kwargs):
        fss = rm.FileserverShare.objects.filter(share_id=kwargs['share_id'],
                server__active=True).values('server__name', 'path').first()
        self.queue = self.get_server_based_queue(fss['server__name'], settings.QUEUE_FASTSTORAGE)
        self.run_tasks.append(((fss['path'], kwargs['path'])))


class RegisterExternalFile(MultiFileJob):
    refname = 'register_external_raw'
    task = tasks.register_downloaded_external_raw
    queue = settings.QUEUE_FILE_DOWNLOAD
    """gets sf_ids, of non-checked downloaded external RAW files in tmp., checks MD5 and 
    registers to dataset
    """

   # def getfiles_query(self, **kwargs):
   #     return super().getfiles_query(**kwargs).filter(checked=False)
    
    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs).values('path', 'sfile__filename', 'sfile_id',
                'sfile__rawfile_id'):
            self.run_tasks.append((os.path.join(fn['path'], fn['sfile__filename']), fn['sfile_id'],
                fn['sfile__rawfile_id'], kwargs['sharename'], kwargs['dset_id']))


class DownloadPXProject(DatasetJob):
    # FIXME dupe check?
    refname = 'download_px_data'
    task = tasks.download_px_file_raw
    queue = settings.QUEUE_FILE_DOWNLOAD
    """gets sf_ids, of non-checked non-downloaded PX files.
    checks pride, fires tasks for files not yet downloaded. 
    """

    def oncreate_getfiles_query(self, **kwargs):
        # FIXME not used
        return rm.StoredFileLoc.objects.filter(sfile__rawfile_id__in=kwargs['shasums'], 
            checked=False).select_related('rawfile')
    
    def process(self, **kwargs):
        px_stored = {x.sfile.filename: x.sfile for x in self.getfiles_query(**kwargs)}
        for fn in call_proteomexchange(kwargs['pxacc']):
            ftpurl = urlsplit(fn['downloadLink'])
            filename = os.path.split(ftpurl.path)[1]
            if filename in px_stored and fn['fileSize'] == px_stored[filename].rawfile.size:
                # Only download non-checked (i.e. non-confirmed already downloaded) files
                pxsf = px_stored[filename]
                self.run_tasks.append(((
                    ftpurl.path, ftpurl.netloc, 
                    pxsf.id, pxsf.rawfile_id, fn['sha1sum'],
                    fn['fileSize'], kwargs['sharename'], kwargs['dset_id']), {}))


def upload_file_pdc_runtask(sfloc, isdir):
    """Generates the arguments for task to upload file to PDC. Reused in dataset jobs"""
    yearmonth = datetime.strftime(sfloc.sfile.regdate, '%Y%m')
    pdcfile, _cr = rm.PDCBackedupFile.objects.get_or_create(storedfile_id=sfloc.sfile_id,
            is_dir=isdir, defaults={'pdcpath': '', 'success': False})
    if not _cr and pdcfile.success and not pdcfile.deleted:
        return
    fss = rm.FileserverShare.objects.filter(server__can_backup=True, share=sfloc.servershare).values('path').first()
    if not fss:
        raise RuntimeError('Cannot find server to backup file from, please'
                'configure system for backups')
    fnpath = os.path.join(sfloc.path, sfloc.sfile.filename)
    return (sfloc.sfile.md5, yearmonth, fss['path'], fnpath, sfloc.sfile.id, isdir)


def restore_file_pdc_runtask(sfloc):
    backupfile = rm.PDCBackedupFile.objects.get(storedfile_id=sfloc.sfile_id)
    fnpath = os.path.join(sfloc.path, sfloc.sfile.filename)
    yearmonth = datetime.strftime(sfloc.sfile.regdate, '%Y%m')
    fss = rm.FileserverShare.objects.filter(server__can_backup=True,
            share__function=rm.ShareFunction.INBOX).values('path').first()
    return (fss['path'], fnpath, backupfile.pdcpath, sfloc.id, backupfile.is_dir)


def call_proteomexchange(pxacc):
    # FIXME check SHA1SUM when getting this
    accessions = {'expfn': 'PRIDE:0000584',
            'ftp': 'PRIDE:0000469',
            'raw': 'PRIDE:0000404',
    }
    inst_type_map = {
        'MS:1001742': 'velos', # LTQ Orbitrap Velos 
        #'MS:1001909': 'velos', # Velos plus
        'MS:1001910': 'velos', # Orbitrap Elite
        'MS:1002835': 'velos', # Orbitrap Classic
        'MS:1001911': 'qe',  # Q Exactive
        'MS:1002523': 'qe',  # Q Exactive HF
        'MS:1002634': 'qe',  # Q Exactive plus
        'MS:1002877': 'qe',  # Q Exactive HF-X
        'MS:1002732': 'lumos',  # Orbitrap Fusion Lumos
# FIXME more instruments
# FIXME if we are trying to download OUR OWN data, we get problem with MD5 already existing
# ALso when re-queueing this job, there is a problem when MD5 is identical
# Possibly solve with "this is our data, please reactivate from PDC"
    }
    prideurl = 'https://www.ebi.ac.uk/pride/ws/archive/v2/'
    # Get project instruments before files so we can assign instrument types to the files
    project = requests.get(urljoin(prideurl, 'projects/{}'.format(pxacc)))
    if project.status_code != 200:
        raise RuntimeError(f'Connected to ProteomeXchange but could not get project information, '
                'status code {project.status_code}')
    try:
        all_inst = project.json()['instruments']
    except KeyError:
        raise RuntimeError('Could not determine instruments from ProteomeXchange project "{pxacc}"')
    try:
        inst_types = {x['accession']: inst_type_map[x['accession']] for x in all_inst}
    except KeyError:
        fail_inst = ', '.join([x['accession'] for x in all_inst if x['accession'] not in inst_type_map])
        raise RuntimeError(f'Not familiar with instrument type(s) {fail_inst}, please ask admin to upgrade Kantele')
    amount_instruments = len(set(inst_types.values()))

    # Now try to fetch the experiment design file if needed
    allfiles = requests.get(urljoin(prideurl, 'files/byProject'), params={'accession': pxacc}).json()
    fn_instr_map = {}
    for fn in allfiles:
        # first get experiment design file, if it exists
        if fn['fileCategory']['accession'] == accessions['expfn']:
            try:
                ftpfn = [x for x in fn['publicFileLocations'] if x['accession'] == accessions['ftp']][0]['value']
            except (KeyError, IndexError):
                if amount_instruments > 1:
                    raise RuntimeError('Cannot get FTP location for experiment design file')
                else:
                    print('Skipping experiment design file, cannot find it in file listing')
            expfn = urlparse(ftpfn)
            explines = []
            try:
                ftp = ftplib.FTP(expfn.netloc)
                ftp.login()
                ftp.retrlines(f'RETR {expfn.path}', lambda x: explines.append(x.strip().split('\t')))
            except ftplib.all_errors as ftperr:
                if amount_instruments > 1:
                    raise RuntimeError(f'Could not download experiment design file {ftpfn}')
                else:
                    print(f'Skipping experiment design file {ftpfn}, errored upon downloading')
            else:
                instr_ix = explines[0].index('comment[instrument]')
                fn_ix = explines[0].index('comment[data file]')
                for line in explines[1:]:
                    # parse e.g. "AC=MS:1002526;NT=Q Exactive Plus"
                    instr_acc = [x.split('=')[1] for x in line[instr_ix].split(';') if x[:2] == 'AC'][0]
                    fn_instr_map[line[fn_ix]] = inst_type_map[instr_acc]
            break
    if not fn_instr_map:
        print('Could not find experiment design file')
 
    fetchable_files = []
    first_instrument = set(inst_types.values()).pop()
    for fn in allfiles:
        try:
            ftype = fn['fileCategory']['accession']
            shasum = fn['checksum']
            dlink = [x for x in fn['publicFileLocations'] if x['accession'] == accessions['ftp']][0]['value']
            filesize = fn['fileSizeBytes']
            filename = fn['fileName']
        except (KeyError, IndexError):
            raise RuntimeError('Could not get download information for a file from ProteomeXchange')
        if ftype == accessions['raw']:
            if filename in fn_instr_map:
                instr_type = fn_instr_map[filename]
            elif amount_instruments == 1:
                instr_type = first_instrument
            else:
                raise RuntimeError(f'Could not find instrument for file {filename} and '
                        'more than 1 instrument type used in project')
            fetchable_files.append({'fileSize': filesize, 'downloadLink': dlink,
                'sha1sum': shasum, 'instr_type': instr_type})
    return fetchable_files
