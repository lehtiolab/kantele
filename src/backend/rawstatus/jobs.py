import os
import requests
from urllib.parse import urlsplit, urlparse, urljoin
import ftplib
from datetime import datetime

from rawstatus import tasks, models
from datasets import tasks as dstasks
from datasets import models as dm
from kantele import settings
from jobs.jobs import SingleFileJob, MultiFileJob, DatasetJob


def create_upload_dst_web(rfid, ftype):
    '''To create path for uploaded files'''
    return os.path.join(settings.TMP_UPLOADPATH, f'{rfid}.{ftype}')

def get_host_upload_dst(web_dst):
    '''To get path for uploaded files on the host (not the container)'''
    return web_dst.replace(settings.TMP_UPLOADPATH, settings.HOST_UPLOADDIR, 1)


class RsyncFileTransfer(SingleFileJob):
    # FIXME change name? All these are for web-to-storage, can maybe generalize that
    # with the also-rsyncing move_dset_servershare, or with (later) any-mover like for analysis results
    # Possibly dont generalize as this could check if 
    refname = 'rsync_transfer'
    task = tasks.rsync_transfer_file

    def check_error(self, **kwargs):
        src_sfloc = self.getfiles_query(**kwargs)
        if models.StoredFileLoc.objects.exclude(pk=src_sfloc.pk).filter(
                sfile__filename=src_sfloc.sfile.filename, path=src_sfloc.path,
                servershare=src_sfloc.servershare).exists():
            return (f'Cannot rsync file {src_sfloc.pk} to location {src_sfloc.servershare.name} / '
                    f'{src_sfloc.path} as another file with the same name is already there')
        else:
            return False

    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs)
        dstpath = os.path.join(sfloc.path, sfloc.sfile.filename)
        self.run_tasks.append(((sfloc.sfile_id, get_host_upload_dst(kwargs['src_path']),
            dstpath, sfloc.servershare.name, sfloc.sfile.filetype.is_folder,
            sfloc.sfile.filetype.stablefiles), {}))


class CreatePDCArchive(SingleFileJob):
    '''Archiving of newly arrived files - full datasets can also be archived if they
    are not - then we use the BackupDataset job instead'''
    refname = 'create_pdc_archive'
    task = tasks.pdc_archive

    def process(self, **kwargs):
        # FIXME should we not check if we already have a DB row w success=1 here?
        taskargs = upload_file_pdc_runtask(self.getfiles_query(**kwargs), isdir=kwargs['isdir'])
        if taskargs:
            self.run_tasks.append((taskargs, {}))
            print('PDC archival task queued')


class RestoreFromPDC(SingleFileJob):
    '''For restoring files which are not in a dataset'''
    refname = 'restore_from_pdc_archive'
    task = tasks.pdc_restore

    def process(self, **kwargs):
        '''Path must come from storedfile itself, not its dataset, since it
        can be a file without a dataset'''
        sfloc = self.getfiles_query(**kwargs)
        self.run_tasks.append((restore_file_pdc_runtask(sfloc), {}))
        print('PDC restore task queued')


class RenameFile(SingleFileJob):
    refname = 'rename_file'
    task = dstasks.move_file_storage
    retryable = False
    """Only renames file inside same path/server. Does not move cross directories. Does not change extensions.
    Updates RawFile in job instead of view since jobs are processed in a single queue. StoredFile names are
    updated in the post job view.
    It will also rename all mzML attached converted files.
    """

    def check_error(self, **kwargs):
        '''Check for file name collisions, also with directories'''
        src_sf = self.getfiles_query(**kwargs)
        fn_ext = os.path.splitext(src_sf.sfile.filename)[1]
        fullnewname = f'{kwargs["newname"]}{fn_ext}'
        fullnewpath = os.path.join(src_sf.path, f'{kwargs["newname"]}{fn_ext}')
        if models.StoredFileLoc.objects.filter(sfile__filename=fullnewname, path=src_sf.path,
                servershare_id=src_sf.servershare_id).exists():
            return f'A file in path {src_sf.path} with name {fullnewname} already exists. Please choose another name.'
        elif dm.Dataset.objects.filter(storage_loc=fullnewpath,
                storageshare_id=src_sf.servershare_id).exists():
            return f'A dataset with the same directory name as your new file name {fullnewpath} already exists'
        else:
            return False
        
    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs)
        newname = kwargs['newname']
        fn_ext = os.path.splitext(sfloc.sfile.filename)[1]
        sfloc.sfile.rawfile.name = newname + fn_ext
        sfloc.sfile.rawfile.save()
        for changefn in models.StoredFileLoc.objects.filter(sfile__rawfile=sfloc.sfile.rawfile):
            # FIXME deleted etc? Run the sfloc selection in view instead
            oldname, ext = os.path.splitext(changefn.sfile.filename)
            special_type = '_refined' if hasattr(changefn.sfile, 'mzmlfile') and changefn.sfile.mzmlfile.refined else ''
            self.run_tasks.append(((
                changefn.sfile.filename, changefn.servershare.name,
                changefn.path, changefn.path, changefn.id, changefn.servershare.name),
                {'newname': '{}{}{}'.format(newname, special_type, ext)}))


class ClassifyMSRawFile(SingleFileJob):
    refname = 'classify_msrawfile'
    task = tasks.classify_msrawfile

    def process(self, **kwargs):
        sfloc = self.getfiles_query(**kwargs)
        self.run_tasks.append(((kwargs['token'], sfloc.id, sfloc.sfile.filetype.name,
            sfloc.servershare.name, sfloc.path, sfloc.sfile.filename), {}))


class MoveSingleFile(SingleFileJob):
    '''Move file from one share/path to another. Technically the same as rename, as you can
    also specify a new filename, but this job is not exposed to the user, and only used
    internally for moving files to a predestined path (i.e. incoming mzML, QC raw files, library etc'''
    refname = 'move_single_file'
    task = dstasks.move_file_storage

    def check_error(self, **kwargs):
        '''Check for file name collisions'''
        src_sfloc = self.getfiles_query(**kwargs)
        if dstsharename := kwargs.get('dstsharename'):
            sshare = models.ServerShare.objects.filter(name=dstsharename).get()
        else:
            sshare = src_sfloc.servershare
        newfn = kwargs.get('newname', src_sfloc.sfile.filename)
        fullnewpath = os.path.join(kwargs['dst_path'], newfn)
        if models.StoredFileLoc.objects.filter(sfile__filename=newfn, path=kwargs['dst_path'],
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


class PurgeFiles(MultiFileJob):
    """Removes a number of files from active storage"""
    refname = 'purge_files'
    task = tasks.delete_file
    # FIXME needs to happen for all storages?

    def getfiles_query(self, **kwargs):
        return super().getfiles_query(**kwargs).values('sfile__mzmlfile', 'path', 'sfile__filename',
                'sfile__filetype__is_folder', 'servershare__name', 'sfile_id', 'pk') 

    def process(self, **kwargs):
        # Safety check:
        # First check if files have been archived if that is a demand
        if kwargs.get('need_archive', False):
            if models.PDCBackedupFile.objects.filter(storedfile_id__in=kwargs['sf_ids'],
                    success=True, deleted=False).count() != len(kwargs['sf_ids']):
                raise RuntimeError('Cannot purge these files which are dependent on an archive job '
                        'to have occurred, cannot find records of archived files in DB')
        # Do the actual purge
        for fn in self.getfiles_query(**kwargs):
            fullpath = os.path.join(fn['path'], fn['sfile__filename'])
            if fn['sfile__mzmlfile'] is not None:
                is_folder = False
            else:
                is_folder = fn['sfile__filetype__is_folder']
            self.run_tasks.append(((fn['servershare__name'], fullpath, fn['pk'], is_folder), {}))


class DeleteEmptyDirectory(MultiFileJob):
    """Check first if all the sfids are set to purged, indicating the dir is actually empty.
    Then queue a task. The sfids also make this job dependent on other jobs on those, as in
    the file-purging tasks before this directory deletion"""
    refname = 'delete_empty_directory'
    task = tasks.delete_empty_dir

    def getfiles_query(self, **kwargs):
        return super().getfiles_query(**kwargs).values('servershare__name', 'path')
    
    def process(self, **kwargs):
        sfiles = self.getfiles_query(**kwargs)
        if sfiles.count() and sfiles.count() == sfiles.filter(purged=True).count():
            fn = sfiles.last()
            self.run_tasks.append(((fn['servershare__name'], fn['path']), {}))
        elif not sfiles.count():
            pass
        else:
            raise RuntimeError('Cannot delete dir: according to the DB, there are still storedfiles which '
                'have not been purged yet in the directory')


class RegisterExternalFile(MultiFileJob):
    refname = 'register_external_raw'
    task = tasks.register_downloaded_external_raw
    """gets sf_ids, of non-checked downloaded external RAW files in tmp., checks MD5 and 
    registers to dataset
    """

    def getfiles_query(self, **kwargs):
        return super().getfiles_query(**kwargs).filter(checked=False).values('path', 'sfile__filename', 'sfile_id', 'sfile__rawfile_id')
    
    def process(self, **kwargs):
        for fn in self.getfiles_query(**kwargs):
            self.run_tasks.append(((os.path.join(fn['path'], fn['sfile__filename']), fn['sfile_id'],
                fn['sfile__rawfile_id'], kwargs['sharename'], kwargs['dset_id']), {}))


class DownloadPXProject(DatasetJob):
    # FIXME dupe check?
    refname = 'download_px_data'
    task = tasks.download_px_file_raw
    """gets sf_ids, of non-checked non-downloaded PX files.
    checks pride, fires tasks for files not yet downloaded. 
    """

    def get_sf_ids_jobrunner(self, **kwargs):
        """This is run before running job, to define files used by
        the job (so it cant run if if files are in use by other job)"""
        return [x.sfile_id for x in self.getfiles_query(**kwargs)]

    def getfiles_query(self, **kwargs):
        return models.StoredFileLoc.objects.filter(sfile__rawfile_id__in=kwargs['shasums'], 
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
    try:
        pdcfile = models.PDCBackedupFile.objects.get(storedfile=sfloc.sfile, is_dir=isdir)
    except models.PDCBackedupFile.DoesNotExist:
        # only create entry when not already exists
        models.PDCBackedupFile.objects.create(storedfile=sfloc.sfile, is_dir=isdir,
                pdcpath='', success=False)
    else:
        # Dont do more work than necessary, although this is probably too defensive
        if pdcfile.success and not pdcfile.deleted:
            return
    fnpath = os.path.join(sfloc.path, sfloc.sfile.filename)
    return (sfloc.sfile.md5, yearmonth, sfloc.servershare.name, fnpath, sfloc.sfile.id, isdir)


def restore_file_pdc_runtask(sfloc):
    backupfile = models.PDCBackedupFile.objects.get(storedfile=sfloc.sfile)
    fnpath = os.path.join(sfloc.path, sfloc.sfile.filename)
    yearmonth = datetime.strftime(sfloc.sfile.regdate, '%Y%m')
    return (settings.PRIMARY_STORAGESHARENAME, fnpath, backupfile.pdcpath, sfloc.id, backupfile.is_dir)


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
