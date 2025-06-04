import os
import re
from datetime import datetime
from base64 import b64encode

from django.db import models
from django.utils import timezone
from django.contrib.auth.models import User
from django.db.models import Q

from kantele import settings
from jobs.models import Job


class StoredFileType(models.Model):
    name = models.TextField(unique=True) 
    filetype = models.TextField() # fasta, tabular, raw, analysisoutput, d, etc
    is_folder = models.BooleanField(default=False)
    user_uploadable = models.BooleanField(default=False)
    is_rawdata = models.BooleanField(default=False)
    stablefiles = models.JSONField(default=list, blank=True)

    def __str__(self):
        return self.name


class MSInstrumentType(models.Model):
    name = models.TextField(unique=True) # timstof, qe, velos, tof, lcq, etc

    def __str__(self):
        return self.name


class Producer(models.Model):
    name = models.TextField()
    # client_id is a hash, so users cant accidentally mess up the identifier when
    # they edit transfer script, e.g. increment the client PK if that would be used
    client_id = models.TextField()
    shortname = models.TextField()
    internal = models.BooleanField(default=False, help_text='Internal instrument with own raw file upload client')

    def __str__(self):
        return self.name


class MSInstrument(models.Model):
    producer = models.OneToOneField(Producer, on_delete=models.CASCADE)
    instrumenttype = models.ForeignKey(MSInstrumentType, on_delete=models.CASCADE)
    filetype = models.ForeignKey(StoredFileType, on_delete=models.CASCADE) # raw, .d
    active = models.BooleanField(default=True)

    def __str__(self):
        return 'MS - {}/{}'.format(self.producer.name, self.filetype.name)


class DataSecurityClass(models.IntegerChoices):
    # Go from lowest to highest classification
    NOSECURITY = 1, 'Not classified'
    # FIXME when ready, also have personal data dsets
    # PERSONAL = 2, 'Personal data'


class FileServer(models.Model):
    name = models.TextField(unique=True)
    uri = models.TextField() # for users
    fqdn = models.TextField() # controller URL for rsync SSH etc
    # Is this an analysis server? Scratchdir can be blank even for those!
    is_analysis = models.BooleanField(default=False)
    scratchdir = models.TextField()
    # Does the server have rsync private keys (aka is some kind of controller)
    can_rsync = models.BooleanField(default=False)
    rsyncusername = models.TextField()
    rsynckeyfile = models.TextField()

    def __str__(self):
        return self.name


class ServerShare(models.Model):
    name = models.TextField(unique=True)  # storage, tmp,
    max_security = models.IntegerField(choices=DataSecurityClass.choices)
    description = models.TextField()
    active = models.BooleanField(default=True)
    # Not all shares are for raw data:
    # - web for serving QC
    # - analysis for storing analysis results
    # - mzml_in etc etc
    # This may become obsolete in the future but is needed now
    has_rawdata = models.BooleanField(default=True)
    # 0 is no expiry date
    maxdays_data = models.IntegerField(default=0)

    def __str__(self):
        return self.name

    def get_non_open_dset_path(self, project_id, dset_id):
        return os.path.join(project_id, dset_id)

    def rename_storage_loc_toplvl(self, newprojname, oldpath, project_id, dset_id):
        if self.max_security == DataSecurityClass.NOSECURITY:
            return os.path.join(newprojname, *oldpath.split(os.path.sep)[1:])
        else:
            return self.get_non_open_dset_path(project_id, dset_id)

    def set_dset_storage_location(self, quantprot_id, project, exp, dset, dtype, prefrac, hiriefrange_id):
        # FIXME adapt to multiple server shares
        """
        Governs rules for storage path naming of datasets on "open" servershares.
        Project name is always top level, this is also used in 
        rename_storage_loc_toplvl
        """
        if self.max_security == DataSecurityClass.NOSECURITY:
            subdir = ''
            if dtype.pk != quantprot_id:
                subdir = dtype.name
            if prefrac and hiriefrange_id:
                subdir = os.path.join(subdir, models.HiriefRange.objects.get(
                    pk=hiriefrange_id).get_path())
            elif prefrac:
                subdir = os.path.join(prefrac.name)
            subdir = re.sub('[^a-zA-Z0-9_\-\/\.]', '_', subdir)
            if len(subdir):
                subdir = '/{}'.format(subdir)
            if dtype.id in settings.LC_DTYPE_IDS:
                return os.path.join(project.name, exp.name, str(dset.runname_id))
            else:
                #return os.path.join(project.name, exp.name, subdir, runname.name)
                return '{}/{}{}/{}'.format(project.name, exp.name, subdir, dset.runname.name)
        else:
            return self.get_non_open_dset_path(project.pk, dset.pk)


class FileserverShare(models.Model):
    server = models.ForeignKey(FileServer, on_delete=models.CASCADE)
    share = models.ForeignKey(ServerShare, on_delete=models.CASCADE)
    path = models.TextField()


class RawFile(models.Model):
    """Data (raw) files as reported by instrument"""
    name = models.TextField()
    producer = models.ForeignKey(Producer, on_delete=models.CASCADE)
    source_md5 = models.CharField(max_length=32, unique=True)
    size = models.BigIntegerField('size in bytes')
    date = models.DateTimeField('date/time created')
    claimed = models.BooleanField()
    #is_sensitive = models.BooleanField(default=False)

    def __str__(self):
        return self.name


class StoredFile(models.Model):
    """Files transferred from instrument to storage"""
    rawfile = models.ForeignKey(RawFile, on_delete=models.CASCADE)
    # FIXME filetype to raw file, but some 2-3000 rawfiles have no storedfile!
    filetype = models.ForeignKey(StoredFileType, on_delete=models.CASCADE)
    filename = models.TextField()
    regdate = models.DateTimeField(auto_now_add=True)
    md5 = models.CharField(max_length=32, unique=True)
    checked = models.BooleanField(default=False)
    # marked for deletion by user, only UI, but it will be indicative
    # of file being deleted on ALL storages, so if True - file needs restoreing
    # from archive for any operations
    deleted = models.BooleanField(default=False)

    def __str__(self):
        return self.rawfile.name


class StoredFileLoc(models.Model):
    '''This should be an up-to-date table for file locations, so while a record here is always
    CREATED in the request view, its path/purged etc will e.g. change AFTER a rename job has 
    finished. In case rsync job is canceled or held, those will not be updated
    Deleting a file from a share will not delete this record, but it will set active=False
    in the view/request, likewise updating an old file will set active=True in the view.
    '''
    sfile = models.ForeignKey(StoredFile, on_delete=models.CASCADE)
    servershare = models.ForeignKey(ServerShare, on_delete=models.CASCADE)
    active = models.BooleanField(default=True)

    # Fields below are current status, i.e. they are updated after a job
    path = models.TextField()
    # deleted from active storage filesystems
    # This indicates ACTUAL deletion, not the UI or pre-job status of the file,
    # and is set as a delete job finishes
    purged = models.BooleanField(default=True)

    class Meta:
        # Include the deleted field to allow for multi-version of a file 
        # as can be the case in mzML (though only one existing)
        # Only one copy of each file per server
        constraints = [models.UniqueConstraint(fields=['servershare', 'sfile'],
            name='uni_storedfile')]


class MSFileData(models.Model):
    rawfile = models.OneToOneField(RawFile, on_delete=models.CASCADE)
    mstime = models.FloatField()


class UploadFileType(models.IntegerChoices):
    RAWFILE = 1, 'Raw file'
    ANALYSIS = 2, 'Analysis result'
    LIBRARY = 3, 'Shared file for all users'
    USERFILE = 4, 'User upload'
    # QC file for auto processing!? FIXME



class UploadToken(models.Model):
    """A token to upload a specific file type for a specified time"""

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    token = models.CharField(max_length=36, unique=True) # UUID keys
    timestamp = models.DateTimeField(auto_now=True) # this can be updated
    expires = models.DateTimeField()
    expired = models.BooleanField()
    producer = models.ForeignKey(Producer, on_delete=models.CASCADE)
    # ftype is encoded in token for upload, so need to bind Token to Filetype:
    filetype = models.ForeignKey(StoredFileType, on_delete=models.CASCADE)
    archive_only = models.BooleanField(default=False)
    uploadtype = models.IntegerField(choices=UploadFileType.choices)

    @staticmethod
    def validate_token(token, joinmodels):
        try:
            upload = UploadToken.objects.select_related(*joinmodels).get(
                    token=token, expired=False)
        except UploadToken.DoesNotExist as e:
            return False
        else:
            if upload.expires < timezone.now():
                upload.expired = True
                upload.save()
                return False
            elif upload.expired:
                return False
            return upload

    def parse_token_for_frontend(self, host):
        need_desc = int(self.uploadtype in [UploadFileType.LIBRARY, UploadFileType.USERFILE])
        user_token = b64encode(f'{self.token}|{host}|{need_desc}'.encode('utf-8'))
        return {'user_token': user_token.decode('utf-8'), 'expired': self.expired,
                'expires': datetime.strftime(self.expires, '%Y-%m-%d, %H:%M')}

    def invalidate(self):
        self.expired = True
        self.save()


class UserFile(models.Model):
    sfile = models.OneToOneField(StoredFile, on_delete=models.CASCADE)
    description = models.TextField()
    upload = models.ForeignKey(UploadToken, on_delete=models.CASCADE)
    # FIXME do we care about the upload token? In that case, should we care
    # also for libfiles, or maybe even raw files? (mzml etc of course not)


class PDCBackedupFile(models.Model):
    storedfile = models.OneToOneField(StoredFile, on_delete=models.CASCADE)
    pdcpath = models.TextField()
    success = models.BooleanField()
    deleted = models.BooleanField(default=False)
    is_dir = models.BooleanField(default=False)


class SwestoreBackedupFile(models.Model):
    storedfile = models.OneToOneField(StoredFile, on_delete=models.CASCADE)
    swestore_path = models.TextField()
    success = models.BooleanField()
    deleted = models.BooleanField(default=False)


class FileJob(models.Model):
    '''This registers which files are in a job for the jobrunner, so it knows
    if it should wait before executing a job. Creation of a filejob is done when
    the job is created. It will overreport the number of actually-used files in
    case of datasets, since some jobs operate only on half of a dataset, e.g.
    deletion, refine mzML, etc
    '''
    # FIXME move to job module
    storedfile = models.ForeignKey(StoredFile, on_delete=models.CASCADE)
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
