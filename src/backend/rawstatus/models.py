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
    filetype = models.TextField() # fasta, tabular, raw, analysisoutput
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


class FileServer(models.Model):
    name = models.TextField(unique=True)
    uri = models.TextField() # for users
    fqdn = models.TextField() # controller URL for rsync SSH etc

    def __str__(self):
        return self.name


class ServerShare(models.Model):
    name = models.TextField(unique=True)  # storage, tmp,
    server = models.ForeignKey(FileServer, on_delete=models.CASCADE)
    share = models.TextField()  # /home/disk1

    def __str__(self):
        return self.name

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
    filename = models.TextField()
    servershare = models.ForeignKey(ServerShare, on_delete=models.CASCADE)
    path = models.TextField()
    regdate = models.DateTimeField(auto_now_add=True)
    md5 = models.CharField(max_length=32, unique=True)
    checked = models.BooleanField(default=False)
    # TODO put filetype on the RawFile instead of storedfile? Possible?
    filetype = models.ForeignKey(StoredFileType, on_delete=models.CASCADE)
    deleted = models.BooleanField(default=False) # marked for deletion by user, only UI
    purged = models.BooleanField(default=False) # deleted from active storage filesystem

    class Meta:
        # Include the deleted field to allow for multi-version of a file 
        # as can be the case in mzML (though only one existing)
        constraints = [models.UniqueConstraint(fields=['servershare', 'path', 'filename', 'deleted'],
            name='uni_storedfile', condition=Q(deleted=False))]

    def __str__(self):
        return self.rawfile.name


class UploadToken(models.Model):
    """A token to upload a specific file type for a specified time"""

    class UploadFileType(models.IntegerChoices):
        RAWFILE = 1, 'Raw file'
        ANALYSIS = 2, 'Analysis result'
        LIBRARY = 3, 'Shared file for all users'
        USERFILE = 4, 'User upload'
        # QC file for auto processing!? FIXME

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
        ufts = self.UploadFileType
        need_desc = int(self.uploadtype in [ufts.LIBRARY, ufts.USERFILE])
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
    # FIXME move to job module
    storedfile = models.ForeignKey(StoredFile, on_delete=models.CASCADE)
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
