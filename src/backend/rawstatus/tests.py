import os
import re
import json
import shutil
import sqlite3
import zipfile
import signal
from string import Template
from time import sleep, time
from io import BytesIO
from base64 import b64encode, b64decode
import subprocess
from datetime import timedelta, datetime
from tempfile import mkdtemp

from celery import states
from django.utils import timezone
from django.contrib.auth.models import User
from django.core.management import call_command

from kantele import settings
from kantele.tests import BaseTest, ProcessJobTest, BaseIntegrationTest
from rawstatus import models as rm
from rawstatus import jobs as rjobs
from datasets import models as dm
from dashboard import models as dashm
from analysis import models as am
from analysis import models as am
from jobs import models as jm
from jobs import jobs as jj


class BaseFilesTest(BaseTest):
    def setUp(self):
        super().setUp()
        self.nottoken = 'blablabla'
        self.token= 'fghihj'
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.RAWFILE)

        # expired token
        rm.UploadToken.objects.create(user=self.user, token=self.nottoken, 
                expires=timezone.now() - timedelta(1), expired=False, 
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.RAWFILE)

        self.registered_raw = rm.RawFile.objects.create(name='file1', producer=self.prod,
                source_md5='b7d55c322fa09ecd8bea141082c5419d', size=100, date=timezone.now(),
                claimed=False, usetype=rm.UploadFileType.RAWFILE)

class TestBrowserUpload(BaseIntegrationTest):

    def test_libfile(self):
        url = f'{self.live_server_url}/files/upload/userfile/'
        libfn = os.path.join(self.newstorctrl.path, 'libfiles', 'db.fa')
        data = {'ftype_id': self.lft.pk, 'uploadtype': rm.UploadFileType.LIBRARY, 'desc': 'abc',
                'file': open(libfn, 'r')}
        resp = self.cl.post(url, data)
        self.assertEqual(resp.status_code, 200)
        fn = rm.StoredFile.objects.last()
        newname = f'libfile_{fn.libraryfile.pk}_{fn.rawfile.name}'
        self.assertEqual(fn.filename, f'{fn.rawfile_id}_db.fa')
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.inboxctrl.path, fn.filename)))
        self.run_job()
        fn.refresh_from_db()
        self.assertEqual(fn.filename, newname)
        self.assertTrue(os.path.exists(os.path.join(self.inboxctrl.path, fn.filename)))
        self.assertFalse(os.path.exists(os.path.join(self.libctrl.path, fn.filename)))
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.libctrl.path, fn.filename)))

    def test_userfile(self):
        url = f'{self.live_server_url}/files/upload/userfile/'
        upfn = os.path.join(self.newstorctrl.path, 'libfiles', 'db.fa')
        data = {'ftype_id': self.lft.pk, 'uploadtype': rm.UploadFileType.USERFILE, 'desc': 'abc',
                'file': open(upfn, 'r')}
        resp = self.cl.post(url, data)
        self.assertEqual(resp.status_code, 200)
        fn = rm.StoredFile.objects.last()
        newname = f'userfile_{fn.rawfile_id}_{fn.rawfile.name}'
        self.assertEqual(fn.filename, f'{fn.rawfile_id}_db.fa')
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.inboxctrl.path, fn.filename)))
        self.run_job()
        fn.refresh_from_db()
        self.assertEqual(fn.filename, newname)
        self.assertTrue(os.path.exists(os.path.join(self.inboxctrl.path, fn.filename)))
        self.assertFalse(os.path.exists(os.path.join(self.libctrl.path, fn.filename)))
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.libctrl.path, fn.filename)))


class TestUploadScript(BaseIntegrationTest):
    def setUp(self):
        super().setUp()
        self.token= 'fghihj'

        rm.MSInstrument.objects.create(producer=self.adminprod, instrumenttype=self.msit, filetype=self.ft)
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.adminprod, filetype=self.ft, uploadtype=rm.UploadFileType.RAWFILE)
        need_desc = 0
        #need_desc = int(self.uploadtype in [ufts.LIBRARY, ufts.USERFILE])
        self.user_token = b64encode(f'{self.token}|{self.live_server_url}|{need_desc}'.encode('utf-8')).decode('utf-8')
        self.actual_md5 = '1eab409e2965943f52df8e3c63874d5b'

    def tearDown(self):
        super().tearDown()
        if os.path.exists('ledger.json'):
            os.unlink('ledger.json')

    def run_script(self, fullpath, *, config=False, session=False):
        cmd = ['python3', 'rawstatus/file_inputs/upload.py']
        if config:
            cmd.extend(['--config', config])
        else:
            cmd.extend(['--files', fullpath, '--token', self.user_token])
        return subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE,
                start_new_session=session)

    def test_transferstate_done(self):
        # Have ledger with f3sf md5 so system thinks it's already done
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        self.f3sssinbox.active = True
        self.f3sssinbox.save()
        self.f3raw.producer = self.adminprod
        self.f3raw.save()
        timestamp = os.path.getctime(fullp)
        cts_id = f'{timestamp}__{self.f3raw.size}'
        with open('ledger.json', 'w') as fp:
            json.dump({cts_id: {'md5': self.f3sf.md5,
                'is_dir': False,
                'fname': self.f3sf.filename,
                'fpath': fullp,
                'prod_date': timestamp,
                'size': self.f3raw.size,
                'fn_id': self.f3raw.pk,
                }}, fp)

        cmsjobs = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sfloc_id': self.f3sssinbox.pk, 'token': self.token})
        self.assertEqual(cmsjobs.count(), 0)
        # Can use subproc run here since the process will finish - no need to sleep/run jobs
        cmd = ['python3', 'rawstatus/file_inputs/upload.py', '--files', fullp, '--token', self.user_token]
        sp = subprocess.run(cmd, stderr=subprocess.PIPE)
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: done']
        outlines = [x for x in sp.stderr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        self.assertEqual(cmsjobs.count(), 1)

    def get_token(self, *, uploadtype=False):
        resp = self.cl.post(f'{self.live_server_url}/files/token/', content_type='application/json',
                data={'ftype_id': self.ft.pk, 'archive_only': False,
                    'uploadtype': uploadtype or rm.UploadFileType.RAWFILE})
        self.assertEqual(resp.status_code, 200)
        self.token = resp.json()
        self.user_token = self.token['user_token']

    def test_new_file(self):
        # Testing all the way from register to upload
        # This file is of filetype self.ft which is is_folder
        self.f3raw.delete()
        self.get_token()
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path) 
        fullp = os.path.join(fpath, self.f3sf.filename)
        old_raw = rm.RawFile.objects.last()
        sp = self.run_script(fullp)
        # Give time for running script, so job is created before running it etc
        sleep(2)
        new_raw = rm.RawFile.objects.last()
        self.assertEqual(new_raw.pk, old_raw.pk + 1)
        sf = rm.StoredFile.objects.last()
        sss = sf.storedfileloc_set.get()
        self.assertEqual(sf.filename, f'{self.f3sf.filename}.zip')
        self.assertEqual(sss.path, settings.TMPPATH)
        self.assertEqual(sss.servershare, self.ssinbox)
        self.assertFalse(sf.checked)
        # Run rsync
        self.run_job()
        sf.refresh_from_db()
        self.assertTrue(os.path.exists(os.path.join(self.rootdir, self.inboxctrl.path, sss.path, sf.filename)))
        # Run classify
        self.run_job()
        self.assertEqual(sf.filename, self.f3sf.filename)
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        cjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs__sfloc_id=sss.pk).get()
        new_raw.refresh_from_db()
        self.assertEqual(cjob.task_set.get().state, states.SUCCESS)
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sfloc_id=sss.pk)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertFalse(new_raw.claimed) # not QC
        self.assertEqual(new_raw.msfiledata.mstime, 123.456)
        self.assertEqual(jm.Job.objects.filter(funcname='create_pdc_archive', kwargs__sfloc_id=sss.pk).count(), 1)
        self.assertTrue(sf.checked)
        zipboxpath = os.path.join(os.getcwd(), 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {self.token["expires"]}',
                'Registering 1 new file(s)', 
                f'File {new_raw.name} has ID {new_raw.pk}, instruction: transfer',
                f'Uploading {zipboxpath} to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {new_raw.name} has ID {new_raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)

    def test_transfer_again(self):
        '''Transfer already existing file, e.g. overwrites of previously
        found to be corrupt file. It needs to be checked=False for that'''
        # Actual raw3 fn md5:
        self.get_token()
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        self.f3raw.source_md5 = self.actual_md5
        self.f3raw.claimed = False
        self.f3raw.producer = self.adminprod
        self.f3raw.save()
        self.f3sf.checked = False
        self.f3sf.save()
        self.f3sfmz.delete()
        jm.Job.objects.create(funcname='rsync_transfer_fromweb', kwargs={'sfloc_id': self.f3sssinbox.pk,
            'src_path': os.path.join(settings.TMP_UPLOADPATH, f'{self.f3raw.pk}.{self.f3sf.filetype.filetype}')},
            timestamp=timezone.now(), state=jj.Jobstates.DONE)
        fullp = os.path.join(fpath, self.f3sf.filename)
        sp = self.run_script(fullp)
        sleep(1)
        self.f3sf.refresh_from_db()
        self.assertFalse(self.f3sf.checked)
        # rsync
        self.run_job()
        sleep(2)
        # classify
        self.run_job()
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        self.f3raw.refresh_from_db()
        self.f3sf.refresh_from_db()
        self.assertEqual(self.f3raw.msfiledata.mstime, 123.456)
        self.assertTrue(self.f3sf.checked)
        self.assertEqual(self.f3sf.md5, self.f3raw.source_md5)
        zipboxpath = os.path.join(os.getcwd(), 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {self.token["expires"]}',
                'Registering 1 new file(s)', 
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: transfer',
                f'Uploading {zipboxpath} to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)

    def test_transfer_same_name(self):
        # Test trying to upload file with same name/path but diff MD5
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        lastraw = rm.RawFile.objects.last()
        lastsf = rm.StoredFile.objects.last()
        tmpdir = mkdtemp()
        outbox = os.path.join(tmpdir, 'testoutbox')
        os.makedirs(outbox, exist_ok=True)
        shutil.copytree(fullp, os.path.join(outbox, self.f3sf.filename))
        timestamp = os.path.getctime(os.path.join(outbox, self.f3sf.filename))
        with open(os.path.join(tmpdir, 'config.json'), 'w') as fp:
            json.dump({'client_id': self.prod.client_id, 'host': self.live_server_url,

                'token': self.token, 'outbox': outbox, 'raw_is_folder': True,
                'filetype_id': self.ft.pk, 'acq_process_names': ['TEST'],
                'filetype_ext': os.path.splitext(fullp)[1][1:], 'injection_waittime': 5}, fp)
        self.assertFalse(os.path.exists(os.path.join(tmpdir, 'skipbox', self.f3sf.filename)))

        sp = self.run_script(False, config=os.path.join(tmpdir, 'config.json'), session=True)
        sleep(4)
        sp.terminate()
        # Properly kill children since upload.py uses multiprocessing
        os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
        spout, sperr = sp.stdout.read(), sp.stderr.read()
        newraw = rm.RawFile.objects.last()
        newsf = rm.StoredFile.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(newsf.pk, lastsf.pk)
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                f'Checking for new files in {outbox}',
                f'Found new file: {os.path.join(outbox, newraw.name)} produced {timestamp}',
                'Registering 1 new file(s)',
                f'File {newraw.name} has ID {newraw.pk}, instruction: transfer',
                f'Uploading {os.path.join(tmpdir, "zipbox", newraw.name)}.zip to {self.live_server_url}',
                f'Moving file {newraw.name} to skipbox',
                f'Another file in the system has the same name and is stored in the same path '
                f'({self.f3sssinbox.path}/{self.f3sf.filename}). Please '
                'investigate, possibly change the file name or location of this or the other file '
                'to enable transfer without overwriting.']
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines,  explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)
        self.assertTrue(os.path.exists(os.path.join(tmpdir, 'skipbox', self.f3sf.filename)))

    def test_transfer_file_namechanged(self):
        self.get_token()
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        rawfn = rm.RawFile.objects.create(source_md5=self.actual_md5, name='fake_oldname',
                producer=self.adminprod, size=123, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        lastsf = rm.StoredFile.objects.last()
        self.f3raw.delete()
        sp = self.run_script(fullp)
        sleep(1)
        # Run the rsync
        self.run_job()
        sleep(2)
        # Run the raw classification
        self.run_job()
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        newsf = rm.StoredFile.objects.last()
        self.assertEqual(newsf.rawfile.msfiledata.mstime, 123.456)
        self.assertEqual(newsf.pk, lastsf.pk + 1)
        self.assertEqual(rawfn.pk, newsf.rawfile_id)
        self.assertEqual(newsf.filename, self.f3sf.filename)
        rawfn.refresh_from_db()
        self.assertEqual(rawfn.name, self.f3sf.filename)

    def test_rsync_not_finished_yet(self):
        # Have ledger with f3sf md5 so system uses that file
        self.get_token()
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        self.f3raw.claimed = False
        self.f3raw.producer = self.adminprod
        self.f3raw.save()
        self.f3sf.checked = False
        self.f3sfmz.delete()
        self.f3sf.save()
        timestamp = os.path.getctime(fullp)
        cts_id = f'{timestamp}__{self.f3raw.size}'
        with open('ledger.json', 'w') as fp:
            json.dump({cts_id: {'md5': self.f3sf.md5,
                'is_dir': False,
                'fname': self.f3sf.filename,
                'fpath': fullp,
                'prod_date': timestamp,
                'size': self.f3raw.size,
                'fn_id': self.f3raw.pk,
                }}, fp)
        # Rsync job to wait for
        upl_path = os.path.join(settings.TMP_UPLOADPATH, f'{self.f3raw.pk}.{self.f3sf.filetype.filetype}')
        job = jm.Job.objects.create(funcname='rsync_transfer_fromweb',
                kwargs={'sfloc_id': self.f3sss.pk, 'src_path': upl_path},
                timestamp=timezone.now(), state=jj.Jobstates.PROCESSING)
        sp = self.run_script(fullp)
        sleep(1)
        job.state = jj.Jobstates.DONE
        job.save()
        self.f3sf.checked = True
        self.f3sf.save()
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        explines = [f'Token OK, expires on {self.token["expires"]}',
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: wait',
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: done']
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.*producer.worker - ', '', out)
            self.assertEqual(out, exp)

    def test_transfer_qc(self):
        # Test trying to upload file with same name/path but diff MD5
        self.token = 'prodtoken_noadminprod'
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.RAWFILE)
        self.f3raw.delete()
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        with open(os.path.join(fullp, 'HyStarMetadata_template.xml')) as fp:
            template = Template(fp.read())
            meta_xml = template.substitute({'BRUKEY': settings.BRUKERKEY, 'DESC': 'DIAQC'})
        with open(os.path.join(fullp, 'HyStarMetadata.xml'), 'w') as fp: 
            fp.write(meta_xml)
        tmpdir = mkdtemp()
        outbox = os.path.join(tmpdir, 'testoutbox')
        os.makedirs(outbox, exist_ok=True)
        shutil.copytree(fullp, os.path.join(outbox, self.f3sf.filename))
        timestamp = os.path.getctime(os.path.join(outbox, self.f3sf.filename))
        lastraw = rm.RawFile.objects.last()
        lastsf = rm.StoredFile.objects.last()
        with open(os.path.join(tmpdir, 'config.json'), 'w') as fp:
            json.dump({'client_id': self.prod.client_id, 'host': self.live_server_url,

                'token': self.token, 'outbox': outbox, 'raw_is_folder': True,
                'filetype_id': self.ft.pk, 'acq_process_names': ['TEST'],
                'filetype_ext': os.path.splitext(fullp)[1][1:], 'injection_waittime': 5}, fp)
        sp = self.run_script(False, config=os.path.join(tmpdir, 'config.json'), session=True)
        sleep(5)

        newraw = rm.RawFile.objects.last()
        newsfloc = rm.StoredFileLoc.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(newsfloc.sfile_id, lastsf.pk + 1)
        self.assertFalse(newraw.claimed)
        self.assertEqual(newraw.usetype, rm.UploadFileType.RAWFILE)
        # Run rsync fromweb
        self.run_job()
        classifyjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sfloc_id': newsfloc.pk, 'token': self.token})
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sfloc_id=newsfloc.pk)
        rsjobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare', kwargs__sfloc_id=newsfloc.pk, kwargs__dstshare_id=self.ssnewstore.pk,
            kwargs__dstpath=os.path.join(settings.QC_STORAGE_DIR, self.prod.name))
        nfrsjobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare',
                kwargs__sfloc_id=self.nfc_loc.pk, kwargs__dstshare_id=self.ssnewstore.pk)
        self.assertEqual(classifyjob.count(), 1)
        self.assertEqual(classifytask.count(), 0)
        self.assertEqual(rsjobs.count(), 0)
        self.assertEqual(nfrsjobs.count(), 0)
        # Run classify
        self.run_job()
        newraw.refresh_from_db()
        self.assertEqual(newraw.usetype, rm.UploadFileType.QC)
        dstsfloc = rm.StoredFileLoc.objects.filter(sfile_id=newsfloc.sfile_id).last()
        qcjobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow',
                kwargs__sfloc_id=dstsfloc.pk, kwargs__params=['--instrument', self.msit.name, '--dia'])
        self.assertTrue(newraw.claimed)
        self.assertEqual(newraw.msfiledata.mstime, 123.456)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertEqual(dashm.QCRun.objects.filter(rawfile=newraw).count(), 1)
        self.assertEqual(rsjobs.count(), 1)
        self.assertEqual(nfrsjobs.count(), 1)
        self.assertEqual(qcjobs.count(), 1)
        self.run_job() # run rsync to analysis of both raw and nfconfig files
        self.run_job() # run qc
        qcrun = dashm.QCRun.objects.last()
        self.assertTrue(dashm.LineplotData.objects.filter(qcrun=qcrun,
            datatype=dashm.LineDataTypes.NRPSMS, value=1).exists())
        self.assertTrue(dashm.BoxplotData.objects.filter(qcrun=qcrun,
            datatype=dashm.QuartileDataTypes.MASSERROR, q1=1, q2=2, q3=3).exists())
        self.assertTrue(dashm.LineplotData.objects.filter(qcrun=qcrun,
            datatype=dashm.LineDataTypes.MISCLEAV1, value=1).exists())
        self.assertTrue(dashm.LineplotData.objects.filter(qcrun=qcrun,
            datatype=dashm.LineDataTypes.MISCLEAV2, value=3).exists())
        # Must kill this script, it will keep scanning outbox
        try:
            spout, sperr = sp.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
        zipboxpath = os.path.join(tmpdir, 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                f'Checking for new files in {outbox}',
                f'Found new file: {os.path.join(outbox, newraw.name)} produced {timestamp}',
                'Registering 1 new file(s)',
                f'File {newraw.name} has ID {newraw.pk}, instruction: transfer',
                f'Uploading {os.path.join(tmpdir, "zipbox", newraw.name)}.zip to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        # Remove warnings for upload.py ascii art
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines , explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)
        self.assertFalse(os.path.exists(os.path.join(outbox, self.f3sf.filename)))

    def test_upload_to_analysis(self):
        # Use same file as f3sf but actually take its md5 and therefore it is new
        # Testing all the way from register to upload
        # This file is of filetype self.ft which is NOT is_folder -> so it will be zipped up
        self.token = 'hfsjkfhhkjshfskj'
        anaft = rm.StoredFileType.objects.create(name='anaft', filetype=settings.ANALYSIS_FT_NAME,
                is_rawdata=False)
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.anaprod, filetype=anaft, uploadtype=rm.UploadFileType.ANALYSIS)
        ana = am.Analysis.objects.create(user=self.user, name='testana', base_rundir='testdir_iso',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        exta = am.ExternalAnalysis.objects.create(analysis=ana, description='bla', last_token=self.uploadtoken)
        need_desc = 0
        self.user_token = b64encode(f'{self.token}|{self.live_server_url}|{need_desc}'.encode('utf-8')).decode('utf-8')
        self.actual_md5 = 'dee94af7703a5beb01e8fdc84da018bb'
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path, self.f3sf.filename)
        self.f3sf.filename = 'analysis.tdf'
        fullp = os.path.join(fpath, self.f3sf.filename)
        old_raw = rm.RawFile.objects.last()
        sp = self.run_script(fullp)
        # Give time for running script, so job is created before running it etc
        sleep(2)
        new_raw = rm.RawFile.objects.last()
        self.assertEqual(new_raw.pk, old_raw.pk + 1)
        sf = rm.StoredFile.objects.last()
        sss = sf.storedfileloc_set.first()
        self.assertEqual(sf.filename, self.f3sf.filename)
        self.assertEqual(sss.path, ana.get_public_output_dir())
        self.assertEqual(sss.servershare, self.ssana)
        self.assertFalse(sf.checked)
        # Run rsync
        self.run_job()
        sf.refresh_from_db()
        self.assertEqual(sf.filename, self.f3sf.filename)
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        # Analysis files dont go through classify raw of course
        cjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs__sfloc_id=sss.pk)
        self.assertFalse(cjob.exists())
        new_raw.refresh_from_db()
        self.assertTrue(new_raw.claimed)
        self.assertEqual(jm.Job.objects.filter(funcname='create_pdc_archive', kwargs__sfloc_id=sss.pk).count(), 1)
        self.assertTrue(sf.checked)
        self.assertTrue(am.AnalysisResultFile.objects.filter(sfile=sf, analysis=ana).exists())
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                'Registering 1 new file(s)', 
                f'File {new_raw.name} has ID {new_raw.pk}, instruction: transfer',
                f'Uploading {fullp} to {self.live_server_url}',
                f'Succesful transfer of file {fullp}',
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {new_raw.name} has ID {new_raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)

    def test_file_being_acquired(self):
        # Test trying to upload file with same name/path but diff MD5
        self.token = 'prodtoken_noadminprod'
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.RAWFILE)
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        self.f3raw.delete()
        tmpdir = mkdtemp()
        outbox = os.path.join(tmpdir, 'testoutbox')
        os.makedirs(outbox, exist_ok=True)
        shutil.copytree(fullp, os.path.join(outbox, self.f3sf.filename))
        timestamp = os.path.getctime(os.path.join(outbox, self.f3sf.filename))
        lastraw = rm.RawFile.objects.last()
        lastsf = rm.StoredFile.objects.last()
        with open(os.path.join(tmpdir, 'config.json'), 'w') as fp:
            json.dump({'client_id': self.prod.client_id, 'host': self.live_server_url,

                'token': self.token, 'outbox': outbox, 'raw_is_folder': True,
                'filetype_id': self.ft.pk, 'acq_process_names': ['flock'],
                'filetype_ext': os.path.splitext(fullp)[1][1:], 'injection_waittime': 0}, fp)
        # Lock analysis.tdf for 3 seconds to pretend we are the acquisition software
        subprocess.Popen(f'flock {os.path.join(outbox, self.f3sf.filename, "analysis.tdf")} sleep 3', shell=True)
        # Lock another file to simulate that the acquisition software stays open after acquiring
        subprocess.Popen(f'flock manage.py sleep 20', shell=True)
        sp = self.run_script(False, config=os.path.join(tmpdir, 'config.json'), session=True)
        sleep(13)
        newraw = rm.RawFile.objects.last()
        newsf = rm.StoredFileLoc.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(newsf.sfile_id, lastsf.pk + 1)
        # Run rsync
        self.run_job()
        classifyjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sfloc_id': newsf.pk, 'token': self.token})
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sfloc_id=newsf.pk)
        rsjobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare')
        qcjobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow')
        # Run classify
        sleep(1)
        self.assertEqual(classifyjob.count(), 1)
        self.run_job()
        self.assertEqual(classifyjob.count(), 1)
        newraw.refresh_from_db()
        self.assertFalse(newraw.claimed)
        self.assertEqual(newraw.msfiledata.mstime, 123.456)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertEqual(rsjobs.count(), 0)
        self.assertEqual(qcjobs.count(), 0)
        # Must kill this script, it will keep scanning outbox
        try:
            spout, sperr = sp.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
        zipboxpath = os.path.join(tmpdir, 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                f'Checking for new files in {outbox}',
                f'Will wait until acquisition status ready, currently: File {os.path.join(outbox, newraw.name)} is opened by flock',
                f'Checking for new files in {outbox}',
                f'Found new file: {os.path.join(outbox, newraw.name)} produced {timestamp}',
                'Registering 1 new file(s)',
                f'File {newraw.name} has ID {newraw.pk}, instruction: transfer',
                f'Uploading {os.path.join(tmpdir, "zipbox", newraw.name)}.zip to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                # This is enough, afterwards the 'Checking new files in outbox' starts
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines , explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)

    def do_transfer_dset_assoc(self, *, ds_hasfiles):
        self.f3raw.delete()
        self.token = 'prodtoken_noadminprod'
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.RAWFILE)
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        # Using BRUKERKEY=Description with current metadata:
        with open(os.path.join(fullp, 'HyStarMetadata_template.xml')) as fp:
            template = Template(fp.read())
            meta_xml = template.substitute({'BRUKEY': settings.BRUKERKEY, 'DESC': self.oldds.pk})
        with open(os.path.join(fullp, 'HyStarMetadata.xml'), 'w') as fp: 
            fp.write(meta_xml)
        old_raw = rm.RawFile.objects.last()
        tmpdir = mkdtemp()
        outbox = os.path.join(tmpdir, 'testoutbox')
        os.makedirs(outbox, exist_ok=True)
        shutil.copytree(fullp, os.path.join(outbox, self.f3sf.filename))
        timestamp = os.path.getctime(os.path.join(outbox, self.f3sf.filename))
        lastraw = rm.RawFile.objects.last()
        lastsf = rm.StoredFile.objects.last()
        with open(os.path.join(tmpdir, 'config.json'), 'w') as fp:
            json.dump({'client_id': self.prod.client_id, 'host': self.live_server_url,

                'token': self.token, 'outbox': outbox, 'raw_is_folder': True,
                'filetype_id': self.ft.pk, 'acq_process_names': ['TEST'],
                'filetype_ext': os.path.splitext(fullp)[1][1:], 'injection_waittime': 5}, fp)
        sp = self.run_script(False, config=os.path.join(tmpdir, 'config.json'), session=True)
        sleep(5)
        newraw = rm.RawFile.objects.last()
        self.newsf = rm.StoredFileLoc.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(self.newsf.sfile_id, lastsf.pk + 1)
        self.assertFalse(newraw.claimed)
        # Run rsync from web
        self.run_job()
        mvjobs = jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare', kwargs__dss_id=self.olddss.pk,
                kwargs__sfloc_ids=[self.newsf.sfile.storedfileloc_set.first().pk])

        qcjobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow', kwargs__sfloc_id=self.newsf.pk,
                kwargs__params=['--instrument', self.msit.name])
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sfloc_id=self.newsf.pk)
        self.assertEqual(mvjobs.count(), 0)
        self.assertEqual(qcjobs.count(), 0)
        # Run classify
        self.run_job()
        newraw.refresh_from_db()
        self.assertEqual(newraw.msfiledata.mstime, 123.456)
        if ds_hasfiles:
            self.assertFalse(newraw.claimed)
            self.assertEqual(mvjobs.count(), 0)
        else:
            self.assertTrue(newraw.claimed)
            self.assertEqual(mvjobs.filter(state=jj.Jobstates.HOLD).count(), 1)
        self.assertEqual(qcjobs.count(), 0)

        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        # Must kill this script, it will keep scanning outbox
        try:
            spout, sperr = sp.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
        zipboxpath = os.path.join(tmpdir, 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                f'Checking for new files in {outbox}',
                f'Found new file: {os.path.join(outbox, newraw.name)} produced {timestamp}',
                'Registering 1 new file(s)',
                f'File {newraw.name} has ID {newraw.pk}, instruction: transfer',
                f'Uploading {os.path.join(tmpdir, "zipbox", newraw.name)}.zip to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines , explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)
        self.assertFalse(os.path.exists(os.path.join(outbox, self.f3sf.filename)))

    def test_transfer_dset_assoc_hasfiles(self):
        self.do_transfer_dset_assoc(ds_hasfiles=True)

    def test_transfer_dset_assoc(self):
        self.oldds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(
                state=dm.DCStates.NEW)
        self.do_transfer_dset_assoc(ds_hasfiles=False)
        # Accept files and start convert job to make sure that waits for the accepted file rsync
        # Especially done since that job is created in code in rawstatus/views instead of in
        # jobs/jobs.py:create_job
        self.url = '/datasets/save/files/pending/'
        resp = self.post_json({'dataset_id': self.oldds.pk, 'accepted_files': [self.newsf.sfile_id],
            'rejected_files': []})
        self.assertEqual(resp.status_code, 200)
        self.url = '/mzml/create/'
        am.NfConfigFile.objects.create(serverprofile=self.anaprofile2, nfpipe=self.nfwv, nfconfig=self.nfc_lf)
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.oldds.pk})
        self.assertEqual(resp.status_code, 200)
        backupjobs = jm.Job.objects.filter(funcname='create_pdc_archive',
                kwargs__sfloc_id=self.newsf.sfile.storedfileloc_set.first().pk)
        mvjobs = jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare',
                kwargs__dss_id=self.olddss.pk, kwargs__sfloc_ids=[self.newsf.sfile.storedfileloc_set.first().pk])
        mzmljobs = jm.Job.objects.filter(funcname='convert_dataset_mzml',
                kwargs__dss_id=self.olddss.pk, kwargs__pwiz_id=self.pwiz.pk)
        self.assertEqual(mvjobs.count(), 1)
        self.assertEqual(backupjobs.count(), 1)
        self.assertEqual(mzmljobs.count(), 1)
        self.assertEqual(mvjobs.filter(state=jj.Jobstates.PENDING).count(), 1)
        self.assertEqual(backupjobs.filter(state=jj.Jobstates.PENDING).count(), 1)
        self.assertEqual(mzmljobs.filter(state=jj.Jobstates.PENDING).count(), 1)
        self.run_job()
        self.assertEqual(mvjobs.filter(state=jj.Jobstates.PROCESSING).count(), 1)
        self.assertEqual(mzmljobs.filter(state=jj.Jobstates.PENDING).count(), 1)
        self.assertEqual(backupjobs.filter(state=jj.Jobstates.PENDING).count(), 1)
        self.run_job()
        self.assertEqual(backupjobs.filter(state=jj.Jobstates.PROCESSING).count(), 1)
        backupjobs.get().task_set.update(state=states.SUCCESS)
        self.run_job()
        self.assertEqual(mzmljobs.filter(state=jj.Jobstates.PROCESSING).count(), 1)

    def test_classify_fail_nometadata_and_renew_token(self):
        # This file is of filetype self.ft which is is_folder
        self.f3raw.delete()
        self.get_token()
        strtoken, _h, _d = b64decode(self.token['user_token']).decode('utf-8').split('|')
        token = rm.UploadToken.objects.get(token=strtoken)
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path) 
        fullp = os.path.join(fpath, self.f3sf.filename)
        old_raw = rm.RawFile.objects.last()
        sp = self.run_script(fullp)
        # Give time for running script, so job is created before running it etc
        sleep(2)
        new_raw = rm.RawFile.objects.last() # FIXME
        sf = rm.StoredFile.objects.last()
        sss = sf.storedfileloc_set.get()
        # Run rsync
        self.run_job()
        sf.refresh_from_db()
        # Expire token so classify gets 403, make it run again
        token.expires = timezone.now()
        token.save()
        self.assertFalse(token.expired)
        # Delete classify input xml and run classify
        os.unlink(os.path.join(self.rootdir, self.inboxctrl.path, sss.path, sf.filename, 'HyStarMetadata.xml'))
        self.run_job()
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        # Classify task call w "old" token, will set token to expired
        cjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs__sfloc_id=sss.pk)
        cj_g = cjob.get()
        token.refresh_from_db()
        self.assertTrue(token.expired)
        self.assertEqual(cj_g.task_set.get().state, states.FAILURE)
        # Call retry job which will update the expired
        self.user.is_staff = True
        self.user.save()
        resp = self.cl.post('/jobs/retry/', content_type='application/json',
                data={'item_id': cj_g.pk})
        self.assertEqual(resp.status_code, 200)
        self.run_job()
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        new_raw.refresh_from_db()
        self.assertEqual(cjob.get().task_set.get().state, states.SUCCESS)
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sfloc_id=sss.pk)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertFalse(new_raw.claimed) # not QC
        self.assertFalse(jm.Job.objects.filter(funcname='create_pdc_archive').exists())
        self.assertTrue(sf.checked)
        self.assertTrue(rm.MSFileData.objects.filter(rawfile=new_raw, mstime=123.456, success=False,
            errmsg='File reader did not generate report').exists())

        zipboxpath = os.path.join(os.getcwd(), 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {self.token["expires"]}',
                'Registering 1 new file(s)', 
                f'File {new_raw.name} has ID {new_raw.pk}, instruction: transfer',
                f'Uploading {zipboxpath} to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {new_raw.name} has ID {new_raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)

    def test_classify_fail_corrupt(self):
        # This file is of filetype self.ft which is is_folder
        # Delete metadata and also analysis.tdf to simulate a "bad" raw file
        self.f3raw.delete()
        self.get_token()
        fpath = os.path.join(self.rootdir, self.newstorctrl.path, self.f3sss.path) 
        fullp = os.path.join(fpath, self.f3sf.filename)
        old_raw = rm.RawFile.objects.last()
        sp = self.run_script(fullp)
        # Give time for running script, so job is created before running it etc
        sleep(2)
        new_raw = rm.RawFile.objects.last() # FIXME
        sf = rm.StoredFile.objects.last()
        sss = sf.storedfileloc_set.get()
        # Run rsync
        self.run_job()
        sf.refresh_from_db()
        # Delete classify input xml and run classify
        os.unlink(os.path.join(self.rootdir, self.inboxctrl.path, sss.path, sf.filename, 'HyStarMetadata.xml'))
        os.unlink(os.path.join(self.rootdir, self.inboxctrl.path, sss.path, sf.filename, 'analysis.tdf'))
        self.run_job()
        try:
            spout, sperr = sp.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            sp.terminate()
            # Properly kill children since upload.py uses multiprocessing
            os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
            spout, sperr = sp.communicate()
            self.fail()
        cjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs__sfloc_id=sss.pk).get()
        new_raw.refresh_from_db()
        self.assertEqual(cjob.task_set.get().state, states.SUCCESS)
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sfloc_id=sss.pk)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertFalse(new_raw.claimed) # not QC/dset
        self.assertFalse(jm.Job.objects.filter(funcname='create_pdc_archive').exists())
        self.assertTrue(sf.checked)
        self.assertTrue(rm.MSFileData.objects.filter(rawfile=new_raw, mstime=0, success=False,
            errmsg=f'File reader did not generate report; Cannot find {settings.BRUKERRAW} file '
            'analysis.tdf').exists())

        zipboxpath = os.path.join(os.getcwd(), 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {self.token["expires"]}',
                'Registering 1 new file(s)', 
                f'File {new_raw.name} has ID {new_raw.pk}, instruction: transfer',
                f'Uploading {zipboxpath} to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        outlines = [x for x in sperr.decode('utf-8').strip().split('\n')
                if 'invalid escape sequence' not in x and 'r' in x]
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {new_raw.name} has ID {new_raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)

#    def test_libfile(self):
#    
#    def test_userfile(self):
 

class TransferStateTest(BaseFilesTest):
    url = '/files/transferstate/'

    def setUp(self):
        super().setUp()
        self.trfraw = rm.RawFile.objects.create(name='filetrf', producer=self.prod, source_md5='defghi123',
                size=100, date=timezone.now(), claimed=False, usetype=rm.UploadFileType.RAWFILE)
        self.doneraw = rm.RawFile.objects.create(name='filedone', producer=self.prod, source_md5='jklmnop123',
                size=100, date=timezone.now(), claimed=False, usetype=rm.UploadFileType.RAWFILE)
        self.multifileraw = rm.RawFile.objects.create(name='filemulti', producer=self.prod, source_md5='jsldjak8',
                size=100, date=timezone.now(), claimed=False, usetype=rm.UploadFileType.RAWFILE)
        self.trfsf = rm.StoredFile.objects.create(rawfile=self.trfraw, filename=self.trfraw.name,
                md5=self.trfraw.source_md5, filetype=self.ft)
        self.trfsss = rm.StoredFileLoc.objects.create(sfile=self.trfsf, servershare=self.ssinbox,
                path='', purged=False, active=True)
        self.donesf = rm.StoredFile.objects.create(rawfile=self.doneraw, filename=self.doneraw.name,
                md5=self.doneraw.source_md5, checked=True, filetype=self.ft)
        self.donesss = rm.StoredFileLoc.objects.create(sfile=self.donesf, servershare=self.ssinbox,
                path='', purged=False, active=True)
        self.multisf1 = rm.StoredFile.objects.create(rawfile=self.multifileraw, filename=self.multifileraw.name,
                md5=self.multifileraw.source_md5, filetype=self.ft)
        self.multisss1 = rm.StoredFileLoc.objects.create(sfile=self.multisf1,
                servershare=self.ssinbox, path='', purged=False, active=True)
        # ft2 = rm.StoredFileType.objects.create(name='testft2', filetype='tst')
        # FIXME multisf with two diff filenames shouldnt be a problem right?
        self.multisf2 = rm.StoredFile.objects.create(rawfile=self.multifileraw,
                filename=f'{self.multifileraw.name}.mzML', md5='', filetype=self.ft)
        self.multisss2 = rm.StoredFileLoc.objects.create(sfile=self.multisf2,
                servershare=self.ssinbox, path='', purged=False, active=True)

    def test_transferstate_done(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': self.doneraw.id, 'desc': False})
        rj = resp.json()
        self.assertEqual(rj['transferstate'], 'done')
        cmsjobs = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sfloc_id': self.donesss.pk, 'token': self.token})
        self.assertEqual(cmsjobs.count(), 1)

    def test_trfstate_done_libfile(self):
        '''Test if state done is correctly reported for uploaded library file,
        and that archiving and move jobs exist for it'''
        # Create lib file which is not claimed yet
        remotesslib = rm.ServerShare.objects.create(name='libshare_remote', max_security=1,
                function=rm.ShareFunction.LIBRARY)
        remotelibctrl = rm.FileserverShare.objects.create(server=self.remoteanaserver,
                share=remotesslib, path='blabla')
        libraw = rm.RawFile.objects.create(name='another_libfiledone',
                producer=self.prod, source_md5='test_trfstate_libfile',
                size=100, claimed=False, date=timezone.now(), usetype=rm.UploadFileType.LIBRARY)
        sflib = rm.StoredFile.objects.create(rawfile=libraw, md5=libraw.source_md5,
                filetype=self.ft, checked=True, filename=libraw.name)
        sfloc = rm.StoredFileLoc.objects.create(sfile=sflib, servershare=self.sslib,
                path=settings.LIBRARY_FILE_PATH, purged=False, active=True)
        libq = am.LibraryFile.objects.filter(sfile=sflib, description='This is a libfile')
        self.assertFalse(libq.exists())
        libtoken = rm.UploadToken.objects.create(user=self.user, token='libtoken123',
                expires=timezone.now() + timedelta(1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.LIBRARY)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': libtoken.token, 'fnid': libraw.id, 'desc': 'This is a libfile'})
        self.assertEqual(resp.status_code, 200)
        rj = resp.json()
        self.assertEqual(rj['transferstate'], 'done')
        self.assertTrue(libq.exists())
        lf = libq.get()
        jobs = jm.Job.objects.filter(funcname='rename_file', kwargs={'sfloc_id': sfloc.pk,
            'newname': f'libfile_{lf.pk}_{sflib.filename}'})
        self.assertEqual(jobs.count(), 1)
        jobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare',
                kwargs__sfloc_id=sfloc.pk, kwargs__dstshare_id=remotesslib.pk,
                kwargs__dstpath=settings.LIBRARY_FILE_PATH)
        self.assertEqual(jobs.count(), 1)
        pdcjobs = jm.Job.objects.filter(funcname='create_pdc_archive', kwargs={
            'sfloc_id': sfloc.pk, 'isdir': sflib.filetype.is_folder})
        self.assertEqual(pdcjobs.count(), 1)

    def test_trfstate_done_usrfile(self):
        '''Test if state done is correctly reported for uploaded userfile,
        and that archiving and move jobs exist for it'''
        # Create userfile during upload
        usrfraw = rm.RawFile.objects.create(name='usrfiledone', producer=self.prod,
                source_md5='newusrfmd5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.USERFILE)
        sfusr = rm.StoredFile.objects.create(rawfile=usrfraw, md5=usrfraw.source_md5,
                filetype=self.uft, filename=usrfraw.name, checked=True)
        sfloc = rm.StoredFileLoc.objects.create(sfile=sfusr, servershare=self.sslib,
                path=settings.LIBRARY_FILE_PATH, purged=False, active=True)
        usertoken = rm.UploadToken.objects.create(user=self.user, token='usrffailtoken',
                expired=False, producer=self.prod, filetype=self.uft,
                uploadtype=rm.UploadFileType.USERFILE, 
                expires=timezone.now() + timedelta(1))
        desc = 'This is a userfile'
        ufq = rm.UserFile.objects.filter(sfile=sfusr, description=desc, upload=usertoken)
        self.assertFalse(ufq.exists())
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': usertoken.token, 'fnid': usrfraw.pk, 'desc': desc})
        rj = resp.json()
        self.assertTrue(ufq.exists())
        self.assertEqual(rj['transferstate'], 'done')
        pdcjobs = jm.Job.objects.filter(funcname='create_pdc_archive', kwargs={
            'sfloc_id': sfloc.pk, 'isdir': sfusr.filetype.is_folder})
        self.assertEqual(pdcjobs.count(), 1)
        userfile = ufq.get()
        jobs = jm.Job.objects.filter(funcname='rename_file', kwargs={'sfloc_id': sfloc.pk,
            'newname': f'userfile_{usrfraw.pk}_{sfusr.filename}'})
        self.assertEqual(jobs.count(), 1)

    def test_transferstate_transfer(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': self.registered_raw.id, 'desc': False})
        rj = resp.json()
        self.assertEqual(rj['transferstate'], 'transfer')

    def test_transferstate_wait(self):
        upload_file = os.path.join(settings.TMP_UPLOADPATH,
                f'{self.trfraw.pk}.{self.trfsf.filetype.filetype}')
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': self.trfraw.id, 'desc': False})
        rj = resp.json()
        self.assertEqual(rj['transferstate'], 'wait')
        # TODO test for no-rsync-job exists (wait but talk to admin)

    def test_failing_transferstate(self):
        # test all the fail HTTPs
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('No token', resp.json()['error'])
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': 'wrongid', 'fnid': 1, 'desc': False})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('invalid or expired', resp.json()['error'])
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': 'self.token'})
        self.assertEqual(resp.status_code, 400)
        self.assertIn('Bad request', resp.json()['error'])
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': 99, 'desc': False})
        self.assertEqual(resp.status_code, 404)
        # token expired
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.nottoken, 'fnid': 1, 'desc': False})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('invalid or expired', resp.json()['error'])
        # wrong producer
        prod2 = rm.Producer.objects.create(name='prod2', client_id='secondproducer', shortname='p2')
        p2raw = rm.RawFile.objects.create(name='p2file1', producer=prod2, source_md5='p2rawmd5',
                size=100, date=timezone.now(), claimed=False, usetype=rm.UploadFileType.RAWFILE)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': p2raw.id, 'desc': False})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('is not from producer', resp.json()['error'])
        # raw with multiple storedfiles -> conflict
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': self.multisf1.rawfile_id, 'desc': False})
        self.assertEqual(resp.status_code, 409)
        self.assertIn('there are multiple', resp.json()['error'])
        # userfile without description
        usertoken = rm.UploadToken.objects.create(user=self.user, token='usrffailtoken',
                expired=False, producer=self.prod, filetype=self.uft,
                uploadtype=rm.UploadFileType.USERFILE, 
                expires=timezone.now() + timedelta(1))
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': usertoken.token, 'fnid': self.doneraw.id, 'desc': False})
        rj = resp.json()
        self.assertEqual(resp.status_code, 403)
        self.assertEqual('Library or user files need a description', resp.json()['error'])


class TestFileRegistered(BaseFilesTest):
    url = '/files/transferstate/'

    def test_auth_etc_fails(self):
        # GET
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # No token
        resp = self.cl.post(self.url, content_type='application/json',
                data={'hello': 'test'})
        self.assertEqual(resp.status_code, 403)
        # No other param
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': 'test'})
        self.assertEqual(resp.status_code, 400)
        # Missing client ID /token (False)
        stddata = {'fn': self.registered_raw.name, 'token': False, 'size': 200,
                'date': time(), 'md5': 'fake'}
        resp = self.cl.post(self.url, content_type='application/json',
                data=stddata)
        self.assertEqual(resp.status_code, 403)
        # Wrong token
        resp = self.cl.post(self.url, content_type='application/json',
                data= {**stddata, 'token': self.nottoken})
        self.assertEqual(resp.status_code, 403)
        # expired token
        resp = self.cl.post(self.url, content_type='application/json',
                data={**stddata, 'token': self.nottoken})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('invalid or expired', resp.json()['error'])
        # Wrong date
        resp = self.cl.post(self.url, content_type='application/json',
                data={**stddata, 'token': self.token, 'date': 'fake'})
        self.assertEqual(resp.status_code, 400)

    def test_normal(self):
        nowdate = timezone.now()
        stddata = {'fn': self.registered_raw.name, 'token': self.token, 'size': 100,
                'date': datetime.timestamp(nowdate), 'md5': 'fake'}
        resp = self.cl.post(self.url, content_type='application/json',
                data=stddata)
        self.assertEqual(resp.status_code, 200)
        newraws = rm.RawFile.objects.filter(source_md5='fake', #date=nowdate,
                name=self.registered_raw.name, producer=self.prod, size=100) 
        self.assertEqual(newraws.count(), 1)
        newraw = newraws.get()
        self.assertTrue(nowdate - newraw.date < timedelta(seconds=60))
        self.assertEqual(resp.json(), {'transferstate': 'transfer', 'fn_id': newraw.pk})

    def test_register_again(self):
        # create a rawfile
        self.test_normal() 
        # try one with same MD5, check if only one file is there
        nowdate = timezone.now()
        stddata = {'fn': self.registered_raw.name, 'token': self.token, 'size': 100,
                'date': datetime.timestamp(nowdate), 'md5': 'fake'}
        resp = self.cl.post(self.url, content_type='application/json',
                data=stddata)
        self.assertEqual(resp.status_code, 200)
        newraws = rm.RawFile.objects.filter(source_md5='fake',
                name=self.registered_raw.name, producer=self.prod, size=100) 
        self.assertEqual(newraws.count(), 1)
        newraw = newraws.get()
        self.assertEqual(resp.json(), {'transferstate': 'transfer', 'fn_id': newraw.pk})


class TestFileTransfer(BaseFilesTest):
    url = '/files/transfer/'

    def do_check_okfile(self, resp, infile_contents):
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {'state': 'ok', 'fn_id': self.registered_raw.pk})
        sfns = rm.StoredFileLoc.objects.filter(sfile__rawfile=self.registered_raw)
        self.assertEqual(sfns.count(), 1)
        sf = sfns.get().sfile
        self.assertEqual(sf.md5, self.registered_raw.source_md5)
        self.assertFalse(sf.checked)
        upload_file = os.path.join(settings.TMP_UPLOADPATH,
                f'{self.registered_raw.pk}.{self.uploadtoken.filetype.filetype}')
        jobs = jm.Job.objects.filter(funcname='rsync_transfer_fromweb', kwargs={
            'src_path': upload_file, 'sfloc_id': sfns.get().pk})
        self.assertEqual(jobs.count(), 1)
        job = jobs.get()
        # this may fail occasionally
        self.assertTrue(sf.regdate + timedelta(milliseconds=10) > job.timestamp)
        upfile = f'{self.registered_raw.pk}.{sf.filetype.filetype}'
        with open(os.path.join(settings.TMP_UPLOADPATH, upfile)) as fp:
            self.assertEqual(fp.read(), infile_contents)

    def do_transfer_file(self, desc=False, token=False, fname=False):
        # FIXME maybe break up, function getting overloaded
        '''Tries to upload file and checks if everything is OK'''
        fn = 'test_upload.txt'
        if not token:
            token = self.token
        if not fname:
            fname = self.registered_raw.name
        # FIXME rawstatus/ wrong place for uploads test files!
        with open(f'rawstatus/{fn}') as fp:
            stddata = {'fn_id': self.registered_raw.pk, 'token': token,
                    'filename': fname, 'file': fp}
            if desc:
                stddata['desc'] = desc
            resp = self.cl.post(self.url, data=stddata)
            fp.seek(0)
            uploaded_contents = fp.read()
        return resp, uploaded_contents

    def test_fails_badreq_badauth(self):
        # GET
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # No params
        resp = self.cl.post(self.url, data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)
        # Missing client ID /token (False)
        stddata = {'fn_id': self.registered_raw.pk, 'token': False,
                'desc': False, 'filename': self.registered_raw.name}
        resp = self.cl.post(self.url, data=stddata)
        self.assertEqual(resp.status_code, 403)
        # Wrong token
        resp = self.cl.post(self.url, data= {**stddata, 'token': 'thisisnotatoken'})
        self.assertEqual(resp.status_code, 403)
        # Wrong fn_id
        resp = self.cl.post(self.url, 
                data={**stddata, 'fn_id': self.registered_raw.pk + 1000, 'token': self.token})
        self.assertEqual(resp.status_code, 403)
        # expired token
        resp = self.cl.post(self.url, data={**stddata, 'token': self.nottoken})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('invalid or expired', resp.json()['error'])
        
    def test_transfer_file(self):
        resp, upload_content = self.do_transfer_file()
        self.do_check_okfile(resp, upload_content)

    def test_transfer_again(self):
        '''Transfer already existing file, e.g. overwrites of previously
        found to be corrupt file'''
        # Create storedfile which is the existing file w same md5, to get 403:
        sf = rm.StoredFile.objects.create(rawfile=self.registered_raw, filetype=self.ft,
                md5=self.registered_raw.source_md5, filename=self.registered_raw.name)
        rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssinbox, path='',
                purged=False, active=True)
        resp, upload_content = self.do_transfer_file()
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.json(), {'error': f'This file is already in the system: {sf.filename}, '
            'but there is no job to put it in the storage. Please contact admin', 'problem': 'NO_RSYNC'})
        # FIXME also test rsync pending,and already_exist

    def test_transfer_same_name(self):
        # Test trying to upload file with same name/path but diff MD5
        other_raw = rm.RawFile.objects.create(name=self.registered_raw.name, producer=self.prod,
                source_md5='fake_existing_md5', size=100, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        sf = rm.StoredFile.objects.create(rawfile=other_raw, filetype=self.ft,
                md5=other_raw.source_md5, filename=other_raw.name)
        rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssinbox, path=settings.TMPPATH,
                purged=False, active=True)
        resp, upload_content = self.do_transfer_file()
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {'error': 'Another file in the system has the same name '
            f'and is stored in the same path ({settings.TMPPATH}/{self.registered_raw.name}). '
            'Please investigate, possibly change the file name or location of this or the other '
            'file to enable transfer without overwriting.', 'problem': 'DUPLICATE_EXISTS'})

    def test_transfer_file_namechanged(self):
        fname = 'newname'
        resp, content = self.do_transfer_file(fname=fname)
        self.do_check_okfile(resp, content)
        sf = rm.StoredFile.objects.get(rawfile=self.registered_raw)
        self.assertEqual(sf.filename, fname)
        self.registered_raw.refresh_from_db()
        self.assertEqual(self.registered_raw.name, fname)
    

class TestUpdateStorageFile(BaseFilesTest):
    url = '/files/storage/'

    def setUp(self):
        super().setUp()
        self.sfile = rm.StoredFile.objects.create(rawfile=self.registered_raw, filename=self.registered_raw.name,
                md5=self.registered_raw.source_md5, filetype_id=self.ft.id)
        self.sfloc = rm.StoredFileLoc.objects.create(sfile=self.sfile, servershare_id=self.sstmp.id,
                path='', purged=False, active=True)


    def test_fails(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': -1})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'File does not exist')

    def test_cannot_move(self):
        badsharemsg = 'Invalid share requested to move file to'
        # Analysis to raw share
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.anasfile.pk, 'share_ids': [self.analocalstor.pk]})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], badsharemsg)
         
        # Dset file to analysis
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.f3sf.pk, 'share_ids': [self.ssana.pk]})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], badsharemsg)

        # No backup, cant restore
        self.f3sf.deleted = True
        self.f3sf.save()
        self.f3sf.storedfileloc_set.update(active=False)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.f3sf.pk, 'share_ids': [self.analocalstor.pk]})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'File has no archived copy in PDC backup '
                'registered in Kantele, can not restore')
        # Dset file to place without dataset
        rm.PDCBackedupFile.objects.create(success=True, storedfile=self.f3sf,
                pdcpath='testbup', deleted=False)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.f3sf.pk, 'share_ids': [self.analocalstor.pk]})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'You cannot move dataset-associated files to '
                'where their dataset is not, move the dataset instead')

        # No active DSS
        self.dss.active = False
        self.dss.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.f3sf.pk, 'share_ids': [self.analocalstor.pk]})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'File is in a dataset, please update entire set')

    def test_analysis(self):
        # Analysis file on delivery to rundir, push directly to rsync capable dest
        self.ana = am.Analysis.objects.create(user=self.user, name='test', base_rundir='testdir',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        self.resultfn = am.AnalysisResultFile.objects.create(analysis=self.ana,
                sfile=self.anasfile)
        # Create inactive copy to see there is an historical copy in an nfrundir
        oldsfl = rm.StoredFileLoc.objects.create(sfile=self.anasfile, servershare=self.ssanaruns,
                path='hello', active=False, purged=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.anasfile.pk, 'share_ids': [self.ssanaruns.pk]})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(jm.Job.objects.filter(funcname='restore_from_pdc_archive').exists())
        self.assertEqual(jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare').count(), 1)

        # Analysis file on rundir back to delivery, is on remote and will go to an rsync-ok server
        self.anasfile_sfl.active, self.anasfile_sfl.purged = False, True
        self.anasfile_sfl.save()
        # Delete jobs to have fresh count
        jm.Job.objects.all().delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.anasfile.pk, 'share_ids': [self.ssana.pk, self.ssanaruns.pk]})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(jm.Job.objects.filter(funcname='restore_from_pdc_archive').exists())
        self.assertEqual(jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare').count(), 1)

        # Analysis file on rundir to other rundir, is on remote and will go through inbox
        # set anasfile_sfl on self.ssana to active=False, so it is only on ssanaruns
        self.anasfile_sfl.active, self.anasfile_sfl.purged = False, True
        self.anasfile_sfl.save()
        # Delete jobs to have fresh count
        jm.Job.objects.all().delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.anasfile.pk, 'share_ids': [self.ssanaruns2.pk]})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(jm.Job.objects.filter(funcname='restore_from_pdc_archive').exists())
        self.assertEqual(jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare').count(), 2)
        inboxjob = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare',
            kwargs__dstshare_id=self.ssinbox.pk)
        self.assertTrue(inboxjob.exists())
        secondjob = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare',
            kwargs__dstshare_id=self.ssanaruns2.pk)
        self.assertTrue(secondjob.exists())
        self.assertEqual(secondjob.get().pk, inboxjob.get().pk + 1)

    def test_dset(self):
        # Between shares on storagecontroller (first retrieve, then rsync)
        rsjobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare')
        bupjobs = jm.Job.objects.filter(funcname='restore_from_pdc_archive')
        self.assertFalse(rsjobs.exists())
        self.assertFalse(bupjobs.exists())
        rm.PDCBackedupFile.objects.create(success=True, storedfile=self.f3sf,
                pdcpath='testbup', deleted=False)
        self.f3sf.storedfileloc_set.update(active=False, purged=True)
        self.f3sf.deleted = True
        self.f3sf.save()
        sfls = self.f3sf.storedfileloc_set.filter(active=True)
        self.assertEqual(sfls.count(), 0)
        dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.sstmp,
                storage_loc_ui=self.storloc, storage_loc=self.storloc, startdate=timezone.now())
        # Create inactive f3sss to make sure it doesnt go to inbox first
        tmpf3sfl = rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.sstmp,
                path=self.storloc, active=False, purged=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.f3sf.pk, 'share_ids': [self.sstmp.pk]})
        self.f3sf.refresh_from_db()
        self.assertFalse(self.f3sf.deleted)
        self.assertEqual(sfls.count(), 1)
        self.assertTrue(sfls.filter(servershare=self.sstmp).exists())
        self.assertTrue(bupjobs.filter(kwargs__sfloc_id=tmpf3sfl.pk).exists())
        self.assertFalse(rsjobs.exists())
        self.assertEqual(resp.status_code, 200)

        # From inbox to remote analocalstor (first retrieve, then rsync)
        jm.Job.objects.all().delete()
        self.f3sf.storedfileloc_set.update(active=False, purged=True)
        self.f3sf.deleted = True
        self.f3sf.save()
        sfls = self.f3sf.storedfileloc_set.filter(active=True)
        self.assertEqual(sfls.count(), 0)
        dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.analocalstor,
                storage_loc_ui=self.storloc, storage_loc=self.storloc, startdate=timezone.now())
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.f3sf.pk, 'share_ids': [self.analocalstor.pk]})
        self.assertEqual(resp.status_code, 200)
        # Only one since the inbox one will have been deleted in the update
        self.assertEqual(sfls.count(), 1)
        self.assertTrue(sfls.filter(servershare=self.analocalstor).exists())
        srcsflid = self.f3sf.storedfileloc_set.filter(active=False, servershare=self.ssinbox).get().pk
        self.assertTrue(bupjobs.filter(kwargs__sfloc_id=srcsflid).exists())
        self.assertTrue(rsjobs.filter(kwargs__sfloc_id=srcsflid).exists())
        self.assertTrue(jm.Job.objects.filter(funcname='purge_files', kwargs__sfloc_ids=[srcsflid]).exists())
        self.assertEqual(jm.Job.objects.count(), 3)


class TestDeleteFile(BaseFilesTest):
    url = '/files/delete/'

    def setUp(self):
        super().setUp()
        self.sfile = rm.StoredFile.objects.create(rawfile=self.registered_raw, filename=self.registered_raw.name,
                md5=self.registered_raw.source_md5, filetype_id=self.ft.id)
        self.sfloc = rm.StoredFileLoc.objects.create(sfile=self.sfile, servershare_id=self.sstmp.id,
                path='', purged=False, active=True)
        self.pdcf = rm.PDCBackedupFile.objects.create(success=True, storedfile=self.sfile, pdcpath='')

    def test_fails(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': -1})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'File does not exist, maybe it is deleted?')

    def test_noarchive_badfile(self):
        self.pdcf.delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.sfile.pk, 'noarchive': True})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'You are not authorized to remove files of type '
                f'{rm.UploadFileType.RAWFILE.label}')
        self.user.is_staff = True
        self.user.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.sfile.pk, 'noarchive': True})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(jm.Job.objects.filter(funcname='create_pdc_archive').exists())
        self.assertTrue(jm.Job.objects.filter(funcname='purge_files',
            kwargs__sfloc_ids=[self.sfloc.pk]).exists())

    def test_backup_delete_dset_file(self):
        '''File is in a dataset and on remote, so is not on backup capable server'''
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.oldsf.pk})
        self.assertEqual(resp.status_code, 402)
        self.assertIn('File is in a dataset, force delete, archive entire set, or remove it '
                'from dataset first', resp.json()['error'])
        self.olddsuser.delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.oldsf.pk, 'force': True})
        self.assertEqual(resp.status_code, 403)
        self.user.is_staff = True
        self.user.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.oldsf.pk, 'force': True})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare',
            kwargs__sfloc_id=self.oldsss.pk, kwargs__dstshare_id=self.ssinbox.pk).exists())
        self.assertTrue(jm.Job.objects.filter(funcname='create_pdc_archive',
            kwargs__sfloc_id=self.oldsf.storedfileloc_set.get(servershare=self.ssinbox).pk).exists())
        self.assertTrue(jm.Job.objects.filter(funcname='purge_files',
            kwargs__sfloc_ids=[x.pk for x in self.oldsf.storedfileloc_set.all()]).exists())

    def test_no_access(self):
        # First a loose rawfile
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'You are not authorized to remove files of type '
                f'{rm.UploadFileType.RAWFILE.label}')


    def test_deleted_file(self):
        '''- File has been scheduled for deletion, cannot make backup
           - File is actually purged but wrongly labeled
        '''
        sfile1 = rm.StoredFile.objects.create(rawfile=self.registered_raw,
                filename=self.registered_raw.name, md5='deletedmd5', filetype_id=self.ft.id, deleted=True)
        rm.StoredFileLoc.objects.create(sfile=sfile1, servershare_id=self.sstmp.id, path='',
                purged=False, active=False)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': sfile1.pk})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'File does not exist, maybe it is deleted?')
        # purged file to also test the check for it. Unrealistic to have it purged but
        # not deleted obviously
        sfile2 = rm.StoredFile.objects.create(rawfile=self.registered_raw, deleted=False, 
                filename=f'{self.registered_raw.name}_purged', 
                md5='deletedmd5_2', filetype_id=self.ft.id)
        rm.StoredFileLoc.objects.create(sfile=sfile2, purged=True, servershare_id=self.sstmp.id, path='',
                active=False)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': sfile2.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'File is possibly deleted, but not marked as such, '
                'please inform admin -- can not archive')

    def test_mzmlfile(self):
        self.user.is_staff = True
        self.user.save()

        am.MzmlFile.objects.create(sfile=self.sfile, pwiz=self.pwiz)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare').exists())
        self.assertFalse(jm.Job.objects.filter(funcname='create_pdc_archive').exists())

    def test_analysisfile(self):
        otheruser = User.objects.create(username='other', email='email')
        ana = am.Analysis.objects.create(user=otheruser, name='ana1', base_rundir='ana1',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        am.AnalysisResultFile.objects.create(analysis=ana, sfile=self.sfile)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {'error': 'You are not authorized to delete this analysis file'})
        ana.user = self.user
        ana.save()
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {'state': 'ok'})


class TestDownloadUploadScripts(BaseFilesTest):
    url = '/files/datainflow/download/'
    zipsizes = {'kantele_upload.sh': 344,
            'kantele_upload.bat': 192,
            'upload.py': 30586,
            'transfer.bat': 177,
            'transfer_config.json': 202,
            'setup.bat': 738,
            }

    def setUp(self):
        super().setUp()
        self.tmpdir = mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_fails_badreq_badauth(self):
        postresp = self.cl.post(self.url)
        self.assertEqual(postresp.status_code, 405)
        clientresp = self.cl.get(self.url, data={'client': 'none'})
        self.assertEqual(clientresp.status_code, 403)
        nowinresp = self.cl.get(self.url, data={'client': 'user'})
        self.assertEqual(nowinresp.status_code, 400)
        badwinresp = self.cl.get(self.url, data={'client': 'user', 'windows': 'yes'})
        self.assertEqual(badwinresp.status_code, 400)
        badinsnoprod = self.cl.get(self.url, data={'client': 'instrument'})
        self.assertEqual(badinsnoprod.status_code, 403)
        badinsbadprod = self.cl.get(self.url, data={'client': 'instrument', 'prod_id': 1000})
        self.assertEqual(badinsbadprod.status_code, 403)
        badinsbaddisk = self.cl.get(self.url, data={'client': 'instrument', 'prod_id': 1})
        self.assertEqual(badinsbaddisk.status_code, 403)

    def test_user_linuxmacos(self):
        resp = self.cl.get(self.url, data={'client': 'user', 'windows': '0'})
        self.assertEqual(resp.status_code, 200)
        with zipfile.ZipFile(BytesIO(b''.join(resp.streaming_content))) as zipfn:
            contents = zipfn.infolist()
            names = zipfn.namelist()
        # check if both files of correct name/size are there
        self.assertEqual(len(contents), 2)
        self.assertIn('kantele_upload.sh', names)
        self.assertIn('upload.py', names)
        for fn in contents:
            self.assertEqual(fn.file_size, self.zipsizes[fn.filename])

    def test_user_windows(self):
        resp = self.cl.get(self.url, data={'client': 'user', 'windows': '1'})
        self.assertEqual(resp.status_code, 200)
        with zipfile.ZipFile(BytesIO(b''.join(resp.streaming_content))) as zipfn:
            contents = zipfn.infolist()
            names = zipfn.namelist()
        # check if both files of correct name/size are there
        self.assertEqual(len(contents), 2)
        self.assertIn('kantele_upload.bat', names)
        self.assertIn('upload.py', names)
        for fn in contents:
            self.assertEqual(fn.file_size, self.zipsizes[fn.filename])

    def test_instrument(self):
        datadisk = 'D:'
        resp = self.cl.get(self.url, data={'client': 'instrument', 'prod_id': self.prod.pk,
            'datadisk': datadisk})
        self.assertEqual(resp.status_code, 200)
        with zipfile.ZipFile(BytesIO(b''.join(resp.streaming_content))) as zipfn:
            contents = {x.filename: x.file_size for x in zipfn.infolist()}
            names = zipfn.namelist()
            with zipfn.open('transfer_config.json') as tcfp:
                tfconfig = json.load(tcfp)
        self.assertEqual(len(names), 11)
        for fn in ['requests-2.32.3-py3-none-any.whl', 'certifi-2025.1.31-py3-none-any.whl', 
                'requests_toolbelt-1.0.0-py2.py3-none-any.whl', 'idna-3.10-py3-none-any.whl',
                'charset_normalizer-3.4.1-py3-none-any.whl', 'urllib3-2.4.0-py3-none-any.whl',
                'psutil-7.0.0-cp37-abi3-win_amd64.whl',
                ]:
            self.assertIn(fn, names)
        for key,val in {'outbox': f'{datadisk}\\outbox',
                'zipbox': f'{datadisk}\\zipbox',
                'donebox': f'{datadisk}\\donebox',
                'client_id': self.prod.client_id,
                'filetype_id': self.prod.msinstrument.filetype_id,
                'filetype_ext': self.prod.msinstrument.filetype.filetype,
                'raw_is_folder': 1 if self.prod.msinstrument.filetype.is_folder else 0,
                'host': settings.KANTELEHOST,
                }.items():
            self.assertEqual(tfconfig[key], val)
        for fn in ['transfer.bat', 'upload.py', 'setup.bat']:
            self.assertEqual(contents[fn], self.zipsizes[fn])


# Job tests should test failure modes of jobs, the success happy path is tested inside the
# integration tests

class TestRenameFile(BaseIntegrationTest):
    url = '/files/rename/'

    def test_renamefile(self):
        # There is no mzML for this sfile:
        self.f3sfmz.delete()
        oldfn = self.f3sf.filename
        oldname, ext = os.path.splitext(oldfn)
        newname = f'renamed_{oldname}{ext}'
        newfile_path = os.path.join(self.f3path, newname)
        kwargs_postdata = {'sf_id': self.f3sf.pk, 'newname': newname}
        # First call HTTP
        resp = self.post_json(data=kwargs_postdata)
        self.assertEqual(resp.status_code, 200)
        file_path = os.path.join(self.f3path, self.f3sf.filename)
        self.assertTrue(os.path.exists(file_path))
        self.assertFalse(os.path.exists(newfile_path))
        self.f3sf.refresh_from_db()
        self.assertEqual(self.f3sf.filename, oldfn)
        job = jm.Job.objects.last()
        self.assertEqual(job.kwargs, {'sfloc_id': self.f3sss.pk, 'newname': newname})
        # Now run job
        self.run_job()
        job.refresh_from_db()
        self.assertEqual(job.state, jj.Jobstates.PROCESSING)
        self.assertFalse(os.path.exists(file_path))
        self.assertTrue(os.path.exists(newfile_path))

    def test_cannot_create_job(self):
        # Try with non-existing file
        resp = self.post_json(data={'sf_id': -1000, 'newname': self.f3sf.filename})
        self.assertEqual(resp.status_code, 403)
        rj = resp.json()
        self.assertEqual('File does not exist', rj['error'])

        # Create file record
        oldfn = 'rename_oldfn.raw'
        oldraw = rm.RawFile.objects.create(name=oldfn, producer=self.prod,
                source_md5='rename_oldraw_fakemd5', size=10, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        sf = rm.StoredFile.objects.create(rawfile=oldraw, filename=oldfn, md5=oldraw.source_md5,
                filetype=self.ft, checked=True)
        sfloc = rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.f3sss.servershare, path=self.f3sss.path,
                purged=False, active=True)
        # Try with no file ownership 
        resp = self.post_json(data={'sf_id': sf.pk, 'newname': self.f3sf.filename})
        self.assertEqual(resp.status_code, 403)
        rj = resp.json()
        self.assertEqual('Not authorized to rename this file', rj['error'])

        self.user.is_superuser = True
        self.user.save()

        # Try rename to existing file
        resp = self.post_json(data={'sf_id': sf.pk, 'newname': self.f3sf.filename})
        self.assertEqual(resp.status_code, 403)
        rj = resp.json()
        self.assertIn('A file in path', rj['error'])
        self.assertIn('already exists. Please choose', rj['error'])


class TestUpdateFileDescription(BaseIntegrationTest):
    url = '/files/description/'

    def test_renamefile(self):
        newdesc = 'New description'
        self.user.is_superuser = True
        self.user.save()
        # Lib file
        kwargs_postdata = {'sf_id': self.sflib.pk, 'desc': newdesc}
        resp = self.post_json(data=kwargs_postdata)
        self.assertEqual(resp.status_code, 200)
        self.lf.refresh_from_db()
        self.assertEqual(self.lf.description, newdesc)
        # User file
        uploadtoken = rm.UploadToken.objects.create(user=self.user, token='whocares1234',
                expires=timezone.now() + timedelta(1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadFileType.LIBRARY)
        uf = rm.UserFile.objects.create(sfile=self.tmpsf, description='new uf',
                upload=uploadtoken)
        kwargs_postdata = {'sf_id': self.tmpsf.pk, 'desc': newdesc}
        resp = self.post_json(data=kwargs_postdata)
        self.assertEqual(resp.status_code, 200)
        uf.refresh_from_db()
        self.assertEqual(uf.description, newdesc)

    def test_fails(self):
        # Try with non-existing file
        resp = self.post_json(data={'sf_id': -1000, 'desc': self.f3sf.filename})
        self.assertEqual(resp.status_code, 403)
        rj = resp.json()
        self.assertEqual('File does not exist', rj['error'])

        # Try with no file ownership 
        resp = self.post_json(data={'sf_id': self.sflib.pk, 'desc': 'balbalabl'}) 
        self.assertEqual(resp.status_code, 403)
        rj = resp.json()
        self.assertEqual('Not authorized to update description of this file', rj['error'])

        self.user.is_superuser = True
        self.user.save()

        resp = self.post_json(data={'sf_id': self.f3sf.pk, 'desc': 'balbalabl'}) 
        self.assertEqual(resp.status_code, 403)
        rj = resp.json()
        self.assertEqual('File is not a shared file', rj['error'])


class TestDeleteJobFile(BaseIntegrationTest):
    jobname = 'purge_files'

    def test_dir(self):
        kwargs = {'sfloc_ids': [self.f3sss.pk]}
        file_path = os.path.join(self.f3path, self.f3sf.filename)
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(os.path.isdir(file_path))

        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.SUCCESS)
        self.assertFalse(os.path.exists(file_path))
        self.f3sss.refresh_from_db()
        self.assertTrue(self.f3sss.purged)

    def test_file(self):
        self.f3sss.path = os.path.join(self.f3sss.path, self.f3sf.filename)
        self.f3sss.save()
        self.f3sf.filename = 'analysis.tdf'
        self.f3sf.save()
        self.ft.is_folder = False
        self.ft.save()
        file_path = os.path.join(self.f3sss.path, self.f3sf.filename)
        file_path = os.path.join(self.rootdir, self.newstorctrl.path, file_path)
        self.assertTrue(os.path.exists(file_path))
        self.assertFalse(os.path.isdir(file_path))
        kwargs = {'sfloc_ids': [self.f3sss.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.SUCCESS)
        self.assertFalse(os.path.exists(file_path))
        self.f3sss.refresh_from_db()
        self.assertTrue(self.f3sss.purged)


    def test_no_file(self):
        '''Test without an actual file in a dir will not error, as it will register
        that it has already deleted this file.
        TODO Maybe we SHOULD error, as the file will be not there, so who knows where
        it is now?
        '''
        badfn = 'badraw'
        badraw = rm.RawFile.objects.create(name=badfn, producer=self.prod,
                source_md5='badraw_fakemd5', size=10, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        badsf = rm.StoredFile.objects.create(rawfile=badraw, filename=badfn,
                    md5=badraw.source_md5, filetype=self.ft, checked=True)
        badloc = rm.StoredFileLoc.objects.create(sfile=badsf, servershare=self.ssnewstore, path=self.storloc,
                purged=False, active=True)
        file_path = os.path.join(badloc.path, badsf.filename)
        self.assertFalse(os.path.exists(file_path))

        kwargs = {'sfloc_ids': [badloc.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        self.assertEqual(job.task_set.get().state, states.SUCCESS)
        badloc.refresh_from_db()
        self.assertTrue(badloc.purged)

    def test_fail_expect_file(self):
        self.ft.is_folder = False
        self.ft.save()
        kwargs = {'sfloc_ids': [self.f3sss.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.FAILURE)
        path_noshare = os.path.join(self.f3sss.path, self.f3sf.filename)
        full_path = os.path.join(self.rootdir, self.newstorctrl.path, path_noshare)
        msg = (f'When trying to delete file {path_noshare}, expected a file, but encountered '
                'a directory')
        self.assertEqual(task.taskerror.message, msg)
        self.assertTrue(os.path.exists(full_path))
        self.f3sss.refresh_from_db()
        self.assertFalse(self.f3sss.purged)

    def test_fail_expect_dir(self):
        self.f3sss.path = os.path.join(self.f3sss.path, self.f3sf.filename)
        self.f3sss.save()
        self.f3sf.filename = 'analysis.tdf'
        self.f3sf.save()
        path_noshare = os.path.join(self.f3sss.path, self.f3sf.filename)
        file_path = os.path.join(self.rootdir, self.newstorctrl.path, path_noshare)
        self.assertTrue(os.path.exists(file_path))
        self.assertFalse(os.path.isdir(file_path))          

        kwargs = {'sfloc_ids': [self.f3sss.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.FAILURE)
        msg = (f'When trying to delete file {path_noshare}, expected a directory, but encountered '
                'a file')
        self.assertEqual(task.taskerror.message, msg)
        self.assertTrue(os.path.exists(file_path))
        self.f3sss.refresh_from_db()
        self.assertFalse(self.f3sss.purged)


class TestAutoDelete(BaseIntegrationTest):

    def test(self):
        exp_ldu = timezone.now() - timedelta(days=3)
        # Not expired file in analysis local raw
        anadss = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.analocalstor,
                storage_loc_ui=self.storloc, storage_loc=self.storloc, startdate=timezone.now())
        rm.PDCBackedupFile.objects.create(success=True, storedfile=self.f3sf, pdcpath='f3sf')
        f3sssana = rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.analocalstor,
                path=self.storloc, active=True, purged=False)
        # raw file in analysis local raw
        dm.DatasetServer.objects.filter(pk=self.olddss.pk).update(last_date_used=exp_ldu)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=self.oldsf, pdcpath='oldsf')
        rm.StoredFileLoc.objects.filter(pk=self.oldsss.pk).update(last_date_used=exp_ldu)

        # f3sf inbox
        self.f3sssinbox.active = True
        self.f3sssinbox.save()

        # inbox file expired
        # f3sf already has pdc file
        rm.PDCBackedupFile.objects.create(success=True, storedfile=self.tmpsf, pdcpath='tmpsf')
        rm.StoredFileLoc.objects.filter(pk=self.tmpsss.pk).update(last_date_used=exp_ldu)

        # Web report not expired
        reportraw = rm.RawFile.objects.create(name='report.html', producer=self.prod,
                source_md5='reportmd5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.QC)
        reportsf = rm.StoredFile.objects.create(rawfile=reportraw, md5=reportraw.source_md5,
                filetype=self.lft, checked=True, filename=reportraw.name)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=reportsf, pdcpath='report')
        reportsfl = rm.StoredFileLoc.objects.create(sfile=reportsf,
                servershare=self.ssweb, path='abbc123', active=True, purged=False)
        # Web report expired
        expreportraw = rm.RawFile.objects.create(name='report.html', producer=self.prod,
                source_md5='expreportmd5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.QC)
        expreportsf = rm.StoredFile.objects.create(rawfile=expreportraw, md5=expreportraw.source_md5,
                filetype=self.lft, checked=True, filename=expreportraw.name)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=expreportsf, pdcpath='expreport')
        expreportsfl = rm.StoredFileLoc.objects.create(sfile=expreportsf,
                servershare=self.ssweb, path='abbc123', active=True, purged=False)
        rm.StoredFileLoc.objects.filter(pk=expreportsfl.pk).update(last_date_used=exp_ldu)

        # Analysis files not expired, one shared
        ana1 = am.Analysis.objects.create(user=self.user, name='ana1', base_rundir='ana1',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        anaraw = rm.RawFile.objects.create(name='ana1.html', producer=self.prod,
                source_md5='ana1md5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.ANALYSIS)
        anasf = rm.StoredFile.objects.create(rawfile=anaraw, md5=anaraw.source_md5,
                filetype=self.lft, checked=True, filename=anaraw.name)
        am.AnalysisResultFile.objects.create(analysis=ana1, sfile=anasf)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=anasf, pdcpath='ana1')
        anasfl = rm.StoredFileLoc.objects.create(sfile=anasf,
                servershare=self.ssanaruns, path=ana1.base_rundir, active=True, purged=False)
        anaraw2 = rm.RawFile.objects.create(name='ana2.html', producer=self.prod,
                source_md5='ana2md5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.ANALYSIS)
        anasf2 = rm.StoredFile.objects.create(rawfile=anaraw2, md5=anaraw2.source_md5,
                filetype=self.lft, checked=True, filename=anaraw2.name)
        am.AnalysisResultFile.objects.create(analysis=ana1, sfile=anasf2)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=anasf2, pdcpath='ana2')
        anasfl2 = rm.StoredFileLoc.objects.create(sfile=anasf2,
                servershare=self.ssanaruns, path=ana1.base_rundir, active=True, purged=False)

        # Analysis files expired
        ana2 = am.Analysis.objects.create(user=self.user, name='ana2', base_rundir='ana2',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        anaraw3 = rm.RawFile.objects.create(name='ana3.html', producer=self.prod,
                source_md5='ana3md5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.ANALYSIS)
        anasf3 = rm.StoredFile.objects.create(rawfile=anaraw3, md5=anaraw3.source_md5,
                filetype=self.lft, checked=True, filename=anaraw3.name)
        am.AnalysisResultFile.objects.create(analysis=ana2, sfile=anasf3)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=anasf3, pdcpath='ana3')
        anasfl3 = rm.StoredFileLoc.objects.create(sfile=anasf3,
                servershare=self.ssanaruns, path=ana2.base_rundir, active=True, purged=False)
        rm.StoredFileLoc.objects.filter(pk=anasfl3.pk).update(last_date_used=exp_ldu)

        # Shared analysis file (not expired):
        am.AnalysisResultFile.objects.create(analysis=ana2, sfile=anasf2)

        # Analysis without shared files expired
        ana3 = am.Analysis.objects.create(user=self.user, name='ana3', base_rundir='ana3',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        anaraw4 = rm.RawFile.objects.create(name='ana4.html', producer=self.prod,
                source_md5='ana4md5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.ANALYSIS)
        anasf4 = rm.StoredFile.objects.create(rawfile=anaraw4, md5=anaraw4.source_md5,
                filetype=self.lft, checked=True, filename=anaraw4.name)
        am.AnalysisResultFile.objects.create(analysis=ana3, sfile=anasf4)
        rm.PDCBackedupFile.objects.create(success=True, storedfile=anasf4, pdcpath='ana4')
        anasfl4 = rm.StoredFileLoc.objects.create(sfile=anasf4,
                servershare=self.ssanaruns, path=ana3.base_rundir, active=True, purged=False)
        rm.StoredFileLoc.objects.filter(pk=anasfl4.pk).update(last_date_used=exp_ldu)

        call_command('delete_expired_files')

        for fn in [f3sssana, self.oldsss, self. tmpsss, reportsfl, expreportsfl, anasfl, anasfl2,
                anasfl3, anasfl4]:
            fn.refresh_from_db()

        self.assertTrue(f3sssana.active)
        self.assertFalse(jm.Job.objects.filter(funcname='purge_files',
            state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[f3sssana.pk]).exists())
        self.assertFalse(jm.Job.objects.filter(funcname='remove_dset_files_servershare',
            state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[f3sssana.pk]).exists())
        self.assertFalse(self.oldsss.active)
        self.assertTrue(jm.Job.objects.filter(funcname='remove_dset_files_servershare',
            state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[self.oldsss.pk]).exists())
        self.assertFalse(jm.Job.objects.filter(funcname='purge_files',
            state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[self.oldsss.pk]).exists())

        for fn in [self.f3sssinbox, reportsfl, anasfl, anasfl2]:
            self.assertTrue(fn.active)
            self.assertFalse(jm.Job.objects.filter(funcname='purge_files',
                state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[fn.pk]).exists())
            self.assertFalse(jm.Job.objects.filter(funcname='remove_dset_files_servershare',
                state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[fn.pk]).exists())

        for fn in [self.tmpsss,  expreportsfl, anasfl3, anasfl4]:
            self.assertFalse(fn.active)
            self.assertTrue(jm.Job.objects.filter(funcname='purge_files',
                state=jj.Jobstates.PENDING, kwargs__sfloc_ids__contains=fn.pk).exists())
            self.assertFalse(jm.Job.objects.filter(funcname='remove_dset_files_servershare',
                state=jj.Jobstates.PENDING, kwargs__sfloc_ids=[fn.pk]).exists())

        self.assertFalse(ana3.deleted)
        ana3.refresh_from_db()
        self.assertTrue(ana3.deleted)


class TestArchiveFile(BaseFilesTest):
    url = '/files/archive/'

    def setUp(self):
        super().setUp()
        self.sfile = rm.StoredFile.objects.create(rawfile=self.registered_raw, filename=self.registered_raw.name,
                md5=self.registered_raw.source_md5, filetype_id=self.ft.id)
        self.sfloc = rm.StoredFileLoc.objects.create(sfile=self.sfile, servershare_id=self.sstmp.id,
                path='', purged=False, active=True)

    def test_fails(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': -1})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'File does not exist')


class TestArchiveFileWithJob(BaseIntegrationTest):
    url = '/files/archive/'

    def setUp(self):
        super().setUp()
        self.registered_raw = rm.RawFile.objects.create(name='file1', producer=self.prod,
                source_md5='b7d55c322fa09ecd8bea141082c5419d', size=100, date=timezone.now(),
                claimed=False, usetype=rm.UploadFileType.RAWFILE)
        self.sfile = rm.StoredFile.objects.create(rawfile=self.registered_raw, filename=self.registered_raw.name,
                md5=self.registered_raw.source_md5, filetype_id=self.ft.id)
        self.sfloc = rm.StoredFileLoc.objects.create(sfile=self.sfile, servershare_id=self.sstmp.id,
                path='', purged=False, active=True)

    def test_archive(self):
        # Archive a file
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 200)
        job = jm.Job.objects.get(funcname='create_pdc_archive', kwargs__sfloc_id=self.sfloc.pk)
        self.assertTrue(job.state, jj.Jobstates.PENDING)
        self.run_job()
        job.refresh_from_db()
        self.assertTrue(job.state, jj.Jobstates.PROCESSING)

        # Already archived
        rm.PDCBackedupFile.objects.filter(storedfile=self.sfile).update(success=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'File is already in archive')
        # File is deleted
        rm.PDCBackedupFile.objects.all().delete()
        self.sfloc.delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'Cannot find copy of file on disk to archive')
