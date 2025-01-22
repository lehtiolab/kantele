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
from base64 import b64encode
import subprocess
from datetime import timedelta, datetime
from tempfile import mkdtemp

from celery import states
from django.utils import timezone
from django.contrib.auth.models import User

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
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.RAWFILE)

        # expired token
        rm.UploadToken.objects.create(user=self.user, token=self.nottoken, 
                expires=timezone.now() - timedelta(1), expired=False, 
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.RAWFILE)

        self.registered_raw, _ = rm.RawFile.objects.get_or_create(name='file1', producer=self.prod,
                source_md5='b7d55c322fa09ecd8bea141082c5419d',
                size=100, date=timezone.now(), claimed=False)


class TestUploadScript(BaseIntegrationTest):
    def setUp(self):
        super().setUp()
        self.token= 'fghihj'

        self.adminprod = rm.Producer.objects.create(name='adminprod', client_id='fjewrialf',
                shortname=settings.PRODUCER_ADMIN_NAME, internal=True)
        rm.MSInstrument.objects.create(producer=self.adminprod, instrumenttype=self.msit, filetype=self.ft)
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.adminprod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.RAWFILE)
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
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
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
        # Check bakcup jobs before and after
        cmsjobs = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sf_id': self.f3sf.pk, 'token': self.token})
        self.assertEqual(cmsjobs.count(), 0)
        # Can use subproc run here since the process will finish - no need to sleep/run jobs
        cmd = ['python3', 'rawstatus/file_inputs/upload.py', '--files', fullp, '--token', self.user_token]
        sp = subprocess.run(cmd, stderr=subprocess.PIPE)
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: done']
        for out, exp in zip(sp.stderr.decode('utf-8').strip().split('\n'), explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        self.assertEqual(cmsjobs.count(), 1)

    def get_token(self, *, uploadtype=False):
        resp = self.cl.post(f'{self.live_server_url}/files/token/', content_type='application/json',
                data={'ftype_id': self.ft.pk, 'archive_only': False,
                    'uploadtype': uploadtype or rm.UploadToken.UploadFileType.RAWFILE})
        self.assertEqual(resp.status_code, 200)
        self.token = resp.json()
        self.user_token = self.token['user_token']

    def test_new_file(self):
        # Use same file as f3sf but actually take its md5 and therefore it is new
        # Testing all the way from register to upload
        # This file is of filetype self.ft which is is_folder
        self.get_token()
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        old_raw = rm.RawFile.objects.last()
        sp = self.run_script(fullp)
        # Give time for running script, so job is created before running it etc
        sleep(2)
        new_raw = rm.RawFile.objects.last()
        self.assertEqual(new_raw.pk, old_raw.pk + 1)
        sf = rm.StoredFile.objects.last()
        self.assertEqual(sf.filename, f'{self.f3sf.filename}.zip')
        self.assertEqual(sf.path, settings.TMPPATH)
        self.assertEqual(sf.servershare.name, settings.TMPSHARENAME)
        self.assertFalse(sf.checked)
        # Run rsync
        self.run_job()
        # Run classify
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
            print(sperr.decode('utf-8'))
            self.fail()
        cjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs__sf_id=sf.pk).get()
        new_raw.refresh_from_db()
        self.assertEqual(cjob.task_set.get().state, states.SUCCESS)
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sf_id=sf.pk)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertFalse(new_raw.claimed) # not QC
        self.assertEqual(new_raw.msfiledata.mstime, 123.456)
        self.assertEqual(jm.Job.objects.filter(funcname='create_pdc_archive', kwargs__sf_id=sf.pk).count(), 1)
        self.assertTrue(sf.checked)
        zipboxpath = os.path.join(os.getcwd(), 'zipbox', f'{self.f3sf.filename}.zip')
        explines = [f'Token OK, expires on {self.token["expires"]}',
                'Registering 1 new file(s)', 
                f'File {new_raw.name} has ID {new_raw.pk}, instruction: transfer',
                f'Uploading {zipboxpath} to {self.live_server_url}',
                f'Succesful transfer of file {zipboxpath}',
                ]
        outlines = sperr.decode('utf-8').strip().split('\n')
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {new_raw.name} has ID {new_raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)
        # FIXME check actual classifying (fake sqlite .d/analysis.tdf file)
        # check user/lib transfer - no classify

    def test_transfer_again(self):
        '''Transfer already existing file, e.g. overwrites of previously
        found to be corrupt file. It needs to be checked=False for that'''
        # Actual raw3 fn md5:
        self.get_token()
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        self.f3raw.source_md5 = self.actual_md5
        self.f3raw.claimed = False
        self.f3raw.producer = self.adminprod
        self.f3raw.save()
        self.f3sf.checked = False
        self.f3sfmz.delete()
        self.f3sf.servershare = self.sstmp
        self.f3sf.path = settings.TMPPATH
        self.f3sf.save()
        jm.Job.objects.create(funcname='rsync_transfer', kwargs={'sf_id': self.f3sf.pk,
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
            print(sperr.decode('utf-8'))
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
        outlines = sperr.decode('utf-8').strip().split('\n')
        for out, exp in zip(outlines, explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            self.assertEqual(out, exp)
        lastexp = f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: done'
        self.assertEqual(re.sub('.* - INFO - .producer.worker - ', '', outlines[-1]), lastexp)

    def test_transfer_same_name(self):
        # Test trying to upload file with same name/path but diff MD5
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        self.f3sf.servershare = self.sstmp
        self.f3sf.path = settings.TMPPATH
        self.f3sf.save()
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
                f'({self.f3sf.servershare.name} - {self.f3sf.path}/{self.f3sf.filename}). Please '
                'investigate, possibly change the file name or location of this or the other file '
                'to enable transfer without overwriting.']
        for out, exp in zip(sperr.decode('utf-8').strip().split('\n'), explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)
        self.assertTrue(os.path.exists(os.path.join(tmpdir, 'skipbox', self.f3sf.filename)))

    def test_transfer_file_namechanged(self):
        self.get_token()
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        rawfn = rm.RawFile.objects.create(source_md5=self.actual_md5, name='fake_oldname',
                producer=self.adminprod, size=123, date=timezone.now(), claimed=False)
        lastsf = rm.StoredFile.objects.last()
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
            print(sperr.decode('utf-8'))
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
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        self.f3raw.claimed = False
        self.f3raw.producer = self.adminprod
        self.f3raw.save()
        self.f3sf.checked = False
        self.f3sfmz.delete()
        self.f3sf.servershare = self.sstmp
        self.f3sf.path = settings.TMPPATH
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
        job = jm.Job.objects.create(funcname='rsync_transfer', kwargs={'sf_id': self.f3sf.pk,
            'src_path': os.path.join(settings.TMP_UPLOADPATH, f'{self.f3raw.pk}.{self.f3sf.filetype.filetype}')},
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
            print(sperr.decode('utf-8'))
            self.fail()
        explines = [f'Token OK, expires on {self.token["expires"]}',
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: wait',
                f'File {self.f3raw.name} has ID {self.f3raw.pk}, instruction: done']
        for out, exp in zip(sperr.decode('utf-8').strip().split('\n'), explines):
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            out = re.sub('.*producer.worker - ', '', out)
            self.assertEqual(out, exp)

    def test_transfer_qc(self):
        # Test trying to upload file with same name/path but diff MD5
        self.token = 'prodtoken_noadminprod'
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.RAWFILE)
        dm.Operator.objects.create(user=self.user) # need operator for QC jobs
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
        with open(os.path.join(fullp, 'HyStarMetadata_template.xml')) as fp:
            template = Template(fp.read())
            meta_xml = template.substitute({'BRUKEY': settings.BRUKERKEY, 'DESC': 'DIAQC'})
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
        newsf = rm.StoredFile.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(newsf.pk, lastsf.pk + 1)
        self.assertFalse(newraw.claimed)
        # Run rsync
        self.run_job()
        classifyjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sf_id': newsf.pk, 'token': self.token})
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sf_id=newsf.pk)
        mvjobs = jm.Job.objects.filter(funcname='move_single_file', kwargs={
            'sf_id': newsf.pk, 'dstsharename': settings.PRIMARY_STORAGESHARENAME,
            'dst_path': os.path.join(settings.QC_STORAGE_DIR, self.prod.name)})
        qcjobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow', kwargs__sf_id=newsf.pk,
                kwargs__params=['--instrument', self.msit.name, '--dia'])
        self.assertEqual(classifyjob.count(), 1)
        self.assertEqual(classifytask.count(), 0)
        self.assertEqual(mvjobs.count(), 0)
        self.assertEqual(qcjobs.count(), 0)
        # Run classify
        self.run_job()
        newraw.refresh_from_db()
        self.assertTrue(newraw.claimed)
        self.assertEqual(newraw.msfiledata.mstime, 123.456)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        self.assertEqual(dashm.QCRun.objects.filter(rawfile=newraw).count(), 1)
        self.assertEqual(mvjobs.count(), 1)
        self.assertEqual(qcjobs.count(), 1)
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
        outlines = sperr.decode('utf-8').strip().split('\n')
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
        anashare = rm.ServerShare.objects.create(name=settings.ANALYSISSHARENAME,
                server=self.newfserver, share='/ana')
        anaft = rm.StoredFileType.objects.create(name='anaft', filetype=settings.ANALYSIS_FT_NAME,
                is_rawdata=False)
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.anaprod, filetype=anaft, uploadtype=rm.UploadToken.UploadFileType.ANALYSIS)
        ana = am.Analysis.objects.create(user=self.user, name='testana', storage_dir='testdir_iso')
        exta = am.ExternalAnalysis.objects.create(analysis=ana, description='bla', last_token=self.uploadtoken)
        need_desc = 0
        self.user_token = b64encode(f'{self.token}|{self.live_server_url}|{need_desc}'.encode('utf-8')).decode('utf-8')
        self.actual_md5 = 'dee94af7703a5beb01e8fdc84da018bb'
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path, self.f3sf.filename)
        self.f3sf.filename = 'analysis.tdf'
        fullp = os.path.join(fpath, self.f3sf.filename)
        old_raw = rm.RawFile.objects.last()
        sp = self.run_script(fullp)
        # Give time for running script, so job is created before running it etc
        sleep(2)
        new_raw = rm.RawFile.objects.last()
        self.assertEqual(new_raw.pk, old_raw.pk + 1)
        sf = rm.StoredFile.objects.last()
        self.assertEqual(sf.filename, self.f3sf.filename)
        self.assertEqual(sf.path, ana.storage_dir)
        self.assertEqual(sf.servershare, anashare)
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
            print(sperr.decode('utf-8'))
            self.fail()
        # Analysis files dont go through classify raw of course
        cjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs__sf_id=sf.pk)
        self.assertFalse(cjob.exists())
        new_raw.refresh_from_db()
        self.assertTrue(new_raw.claimed)
        self.assertEqual(jm.Job.objects.filter(funcname='create_pdc_archive', kwargs__sf_id=sf.pk).count(), 1)
        self.assertTrue(sf.checked)
        self.assertTrue(am.AnalysisResultFile.objects.filter(sfile=sf, analysis=ana).exists())
        explines = [f'Token OK, expires on {datetime.strftime(self.uploadtoken.expires, "%Y-%m-%d, %H:%M")}',
                'Registering 1 new file(s)', 
                f'File {new_raw.name} has ID {new_raw.pk}, instruction: transfer',
                f'Uploading {fullp} to {self.live_server_url}',
                f'Succesful transfer of file {fullp}',
                ]
        outlines = sperr.decode('utf-8').strip().split('\n')
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
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.RAWFILE)
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
        fullp = os.path.join(fpath, self.f3sf.filename)
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
                'filetype_id': self.ft.pk, 'acq_process_names': ['flock'],
                'filetype_ext': os.path.splitext(fullp)[1][1:], 'injection_waittime': 0}, fp)
        # Lock analysis.tdf for 3 seconds to pretend we are the acquisition software
        subprocess.Popen(f'flock {os.path.join(outbox, self.f3sf.filename, "analysis.tdf")} sleep 3', shell=True)
        # Lock another file to simulate that the acquisition software stays open after acquiring
        subprocess.Popen(f'flock manage.py sleep 20', shell=True)
        sp = self.run_script(False, config=os.path.join(tmpdir, 'config.json'), session=True)
        sleep(13)
        newraw = rm.RawFile.objects.last()
        newsf = rm.StoredFile.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(newsf.pk, lastsf.pk + 1)
        # Run rsync
        self.run_job()
        classifyjob = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sf_id': newsf.pk, 'token': self.token})
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sf_id=newsf.pk)
        mvjobs = jm.Job.objects.filter(funcname='move_single_file', kwargs={
            'sf_id': newsf.pk, 'dstsharename': settings.PRIMARY_STORAGESHARENAME,
            'dst_path': os.path.join(settings.QC_STORAGE_DIR, self.prod.name)})
        qcjobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow', kwargs__sf_id=newsf.pk,
                kwargs__params=['--instrument', self.msit.name])
        newsf.refresh_from_db()
        # Run classify
        sleep(1)
        self.assertEqual(classifyjob.count(), 1)
        self.run_job()
        self.assertEqual(classifyjob.count(), 1)
        newraw.refresh_from_db()
        self.assertFalse(newraw.claimed)
        self.assertEqual(newraw.msfiledata.mstime, 123.456)
        self.assertEqual(classifytask.filter(state=states.SUCCESS).count(), 1)
        #self.assertEqual(mvjobs.count(), 1)
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
        outlines = sperr.decode('utf-8').strip().split('\n')
        for out, exp in zip(outlines , explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)

    def do_transfer_dset_assoc(self, *, ds_exists, ds_hasfiles):
        # Test trying to upload file with same name/path but diff MD5
        self.token = 'prodtoken_noadminprod'
        self.uploadtoken = rm.UploadToken.objects.create(user=self.user, token=self.token,
                expires=timezone.now() + timedelta(settings.TOKEN_RENEWAL_WINDOW_DAYS + 1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.RAWFILE)
        dm.Operator.objects.create(user=self.user) # need operator for QC jobs
        fpath = os.path.join(settings.SHAREMAP[self.f3sf.servershare.name], self.f3sf.path)
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
        newsf = rm.StoredFile.objects.last()
        self.assertEqual(newraw.pk, lastraw.pk + 1)
        self.assertEqual(newsf.pk, lastsf.pk + 1)
        self.assertFalse(newraw.claimed)
        # Run rsync
        self.run_job()
        mvjobs = jm.Job.objects.filter(funcname='move_files_storage', kwargs={
            'dset_id': self.oldds.pk, 'rawfn_ids': [newraw.pk]})
        qcjobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow', kwargs__sf_id=newsf.pk,
                kwargs__params=['--instrument', self.msit.name])
        classifytask = jm.Task.objects.filter(job__funcname='classify_msrawfile', job__kwargs__sf_id=newsf.pk)
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
            self.assertEqual(mvjobs.count(), 1)
        self.assertEqual(qcjobs.count(), 0)
        sleep(4)

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
        outlines = sperr.decode('utf-8').strip().split('\n')
        for out, exp in zip(outlines , explines):
            out = re.sub('.* - INFO - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - .producer.inboxcollect - ', '', out)
            out = re.sub('.* - WARNING - .producer.worker - ', '', out)
            out = re.sub('.* - INFO - root - ', '', out)
            out = re.sub('.* - INFO - .producer.main - ', '', out)
            self.assertEqual(out, exp)
        self.assertFalse(os.path.exists(os.path.join(outbox, self.f3sf.filename)))

    def test_transfer_dset_assoc_hasfiles(self):
        self.do_transfer_dset_assoc(ds_exists=True, ds_hasfiles=True)

    def test_transfer_dset_assoc(self):
        self.oldds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(
                state=dm.DCStates.NEW)
        self.do_transfer_dset_assoc(ds_exists=True, ds_hasfiles=False)

#    def test_libfile(self):
#    
#    def test_userfile(self):
 

class TransferStateTest(BaseFilesTest):
    url = '/files/transferstate/'

    def setUp(self):
        super().setUp()
        self.trfraw = rm.RawFile.objects.create(name='filetrf', producer=self.prod, source_md5='defghi123',
                size=100, date=timezone.now(), claimed=False)
        self.doneraw = rm.RawFile.objects.create(name='filedone', producer=self.prod, source_md5='jklmnop123',
                size=100, date=timezone.now(), claimed=False)
        self.multifileraw = rm.RawFile.objects.create(name='filemulti', producer=self.prod, source_md5='jsldjak8',
                size=100, date=timezone.now(), claimed=False)
        self.trfsf = rm.StoredFile.objects.create(rawfile=self.trfraw, filename=self.trfraw.name, servershare=self.sstmp,
                path='', md5=self.trfraw.source_md5, filetype=self.ft)
        self.donesf = rm.StoredFile.objects.create(rawfile=self.doneraw, filename=self.doneraw.name, servershare=self.sstmp,
                path='', md5=self.doneraw.source_md5, checked=True, filetype=self.ft)
        self.multisf1 = rm.StoredFile.objects.create(rawfile=self.multifileraw, filename=self.multifileraw.name, servershare=self.sstmp,
                path='', md5=self.multifileraw.source_md5, filetype=self.ft)
        # ft2 = rm.StoredFileType.objects.create(name='testft2', filetype='tst')
        # FIXME multisf with two diff filenames shouldnt be a problem right?
        multisf2 = rm.StoredFile.objects.create(rawfile=self.multifileraw,
                filename=f'{self.multifileraw.name}.mzML', 
                servershare=self.sstmp, path='', md5='', filetype=self.ft)

    def test_transferstate_done(self):
        sf = self.doneraw.storedfile_set.get()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': self.token, 'fnid': self.doneraw.id, 'desc': False})
        rj = resp.json()
        self.assertEqual(rj['transferstate'], 'done')
        cmsjobs = jm.Job.objects.filter(funcname='classify_msrawfile', kwargs={
            'sf_id': sf.pk, 'token': self.token})
        self.assertEqual(cmsjobs.count(), 1)

    def test_trfstate_done_libfile(self):
        '''Test if state done is correctly reported for uploaded library file,
        and that archiving and move jobs exist for it'''
        # Create lib file which is not claimed yet
        libraw = rm.RawFile.objects.create(name='another_libfiledone',
                producer=self.prod, source_md5='test_trfstate_libfile',
                size=100, claimed=False, date=timezone.now())
        sflib = rm.StoredFile.objects.create(rawfile=libraw, md5=libraw.source_md5,
                filetype=self.ft, checked=True, filename=libraw.name,
                    servershare=self.sstmp, path='')
        libq = am.LibraryFile.objects.filter(sfile=sflib, description='This is a libfile')
        self.assertFalse(libq.exists())
        libtoken = rm.UploadToken.objects.create(user=self.user, token='libtoken123',
                expires=timezone.now() + timedelta(1), expired=False,
                producer=self.prod, filetype=self.ft, uploadtype=rm.UploadToken.UploadFileType.LIBRARY)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'token': libtoken.token, 'fnid': libraw.id, 'desc': 'This is a libfile'})
        self.assertEqual(resp.status_code, 200)
        rj = resp.json()
        self.assertEqual(rj['transferstate'], 'done')
        self.assertTrue(libq.exists())
        pdcjobs = jm.Job.objects.filter(funcname='create_pdc_archive', kwargs={
            'sf_id': sflib.pk, 'isdir': sflib.filetype.is_folder})
        self.assertEqual(pdcjobs.count(), 1)
        lf = libq.get()
        jobs = jm.Job.objects.filter(funcname='move_single_file', kwargs={'sf_id': sflib.pk,
            'dst_path': settings.LIBRARY_FILE_PATH,
            'newname': f'libfile_{lf.pk}_{sflib.filename}'})
        self.assertEqual(jobs.count(), 1)

    def test_trfstate_done_usrfile(self):
        '''Test if state done is correctly reported for uploaded userfile,
        and that archiving and move jobs exist for it'''
        # Create userfile during upload
        usrfraw = rm.RawFile.objects.create(name='usrfiledone', producer=self.prod,
                source_md5='newusrfmd5', size=100, claimed=True, date=timezone.now())
        sfusr = rm.StoredFile.objects.create(rawfile=usrfraw, md5=usrfraw.source_md5, filetype=self.uft,
                filename=usrfraw.name, servershare=self.sstmp, path='', checked=True)
        usertoken = rm.UploadToken.objects.create(user=self.user, token='usrffailtoken',
                expired=False, producer=self.prod, filetype=self.uft,
                uploadtype=rm.UploadToken.UploadFileType.USERFILE, 
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
            'sf_id': sfusr.pk, 'isdir': sfusr.filetype.is_folder})
        self.assertEqual(pdcjobs.count(), 1)
        userfile = ufq.get()
        jobs = jm.Job.objects.filter(funcname='move_single_file', kwargs={'sf_id': sfusr.pk,
            'dst_path': settings.USERFILEDIR,
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
        jm.Job.objects.get_or_create(funcname='rsync_transfer', kwargs={
            'sf_id': self.trfsf.pk, 'src_path': upload_file,
            'dst_sharename': settings.TMPSHARENAME}, timestamp=timezone.now())
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
                size=100, date=timezone.now(), claimed=False)
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
                uploadtype=rm.UploadToken.UploadFileType.USERFILE, 
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
        sfns = rm.StoredFile.objects.filter(rawfile=self.registered_raw)
        self.assertEqual(sfns.count(), 1)
        sf = sfns.get()
        self.assertEqual(sf.md5, self.registered_raw.source_md5)
        self.assertFalse(sf.checked)
        upload_file = os.path.join(settings.TMP_UPLOADPATH,
                f'{self.registered_raw.pk}.{self.uploadtoken.filetype.filetype}')
        jobs = jm.Job.objects.filter(funcname='rsync_transfer', kwargs={
            'src_path': upload_file, 'sf_id': sf.pk})
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
                md5=self.registered_raw.source_md5, servershare=self.sstmp, path='',
                filename=self.registered_raw.name)
        resp, upload_content = self.do_transfer_file()
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.json(), {'error': f'This file is already in the system: {sf.filename}, '
            'but there is no job to put it in the storage. Please contact admin', 'problem': 'NO_RSYNC'})
        # FIXME also test rsync pending,and already_exist

    def test_transfer_same_name(self):
        # Test trying to upload file with same name/path but diff MD5
        other_raw = rm.RawFile.objects.create(name=self.registered_raw.name, producer=self.prod,
                source_md5='fake_existing_md5', size=100, date=timezone.now(), claimed=False)
        rm.StoredFile.objects.create(rawfile=other_raw, filetype=self.ft,
                md5=other_raw.source_md5, servershare=self.sstmp, path=settings.TMPPATH,
                filename=other_raw.name)
        resp, upload_content = self.do_transfer_file()
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {'error': 'Another file in the system has the same name '
            f'and is stored in the same path ({settings.TMPSHARENAME} - {settings.TMPPATH}/{self.registered_raw.name}). '
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
    

class TestArchiveFile(BaseFilesTest):
    url = '/files/archive/'

    def setUp(self):
        super().setUp()
        self.sfile = rm.StoredFile.objects.create(rawfile=self.registered_raw, filename=self.registered_raw.name, servershare_id=self.sstmp.id,
                path='', md5=self.registered_raw.source_md5, filetype_id=self.ft.id)

    def test_get(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)

    def test_wrong_params(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)

    def test_wrong_id(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': -1})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'File does not exist')

    def test_claimed_file(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.oldsf.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('File is in a dataset', resp.json()['error'])

    def test_already_archived(self):
        rm.PDCBackedupFile.objects.create(success=True, storedfile=self.sfile, pdcpath='')
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'File is already archived')

    def test_deleted_file(self):
        sfile1 = rm.StoredFile.objects.create(rawfile=self.registered_raw, filename=self.registered_raw.name, servershare_id=self.sstmp.id,
                path='', md5='deletedmd5', filetype_id=self.ft.id,
                deleted=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': sfile1.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'File is currently marked as deleted, can not archive')
        # purged file to also test the check for it. Unrealistic to have it deleted but
        # not purged obviously, as jobrunner shouldnt trigger that - except for when there is a delete job
        # which is deleted before running (no post-job purge set), which is bad!
        sfile2 = rm.StoredFile.objects.create(rawfile=self.registered_raw,
                filename=f'{self.registered_raw.name}_purged', 
                servershare_id=self.sstmp.id, path='', md5='deletedmd5_2',
                filetype_id=self.ft.id, deleted=False, purged=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': sfile2.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'File is currently marked as deleted, can not archive')

    def test_mzmlfile(self):
        am.MzmlFile.objects.create(sfile=self.sfile, pwiz=self.pwiz)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Derived mzML files are not archived', resp.json()['error'])

    def test_ok(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.sfile.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {'state': 'ok'})


class TestDownloadUploadScripts(BaseFilesTest):
    url = '/files/datainflow/download/'
    zipsizes = {'kantele_upload.sh': 344,
            'kantele_upload.bat': 192,
            'upload.py': 29339,
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
        for fn in ['requests-2.28.1-py3-none-any.whl', 'certifi-2022.9.14-py3-none-any.whl', 
                'requests_toolbelt-0.9.1-py2.py3-none-any.whl', 'idna-3.4-py3-none-any.whl',
                'charset_normalizer-2.1.1-py3-none-any.whl', 'urllib3-1.26.12-py2.py3-none-any.whl',
                'psutil-6.1.0-cp37-abi3-win_amd64.whl',
                ]:
            self.assertIn(fn, names)
        for key,val in {'outbox': f'{datadisk}\outbox',
                'zipbox': f'{datadisk}\zipbox',
                'donebox': f'{datadisk}\donebox',
                'client_id': self.prod.client_id,
                'filetype_id': self.prod.msinstrument.filetype_id,
                'filetype_ext': self.prod.msinstrument.filetype.filetype,
                'raw_is_folder': 1 if self.prod.msinstrument.filetype.is_folder else 0,
                'host': settings.KANTELEHOST,
                }.items():
            self.assertEqual(tfconfig[key], val)
        for fn in ['transfer.bat', 'upload.py', 'setup.bat']:
            self.assertEqual(contents[fn], self.zipsizes[fn])


class TestPurgeFilesJob(ProcessJobTest):
    jobclass = rjobs.PurgeFiles

    def test_fns(self):
        kwargs = {'sf_ids': [self.f3sf.pk, self.oldsf.pk]}
        self.job.process(**kwargs)
        exp_t = [
                ((self.f3sf.servershare.name, os.path.join(self.f3sf.path, self.f3sf.filename),
                    self.f3sf.pk, self.f3sf.filetype.is_folder), {}),
                ((self.oldsf.servershare.name, os.path.join(self.oldsf.path, self.oldsf.filename),
                    self.oldsf.pk, self.oldsf.filetype.is_folder), {})
                ]
        self.check(exp_t)

    def test_is_dir(self):
        self.ft.is_folder = True
        self.ft.save()
        kwargs = {'sf_ids': [self.f3sf.pk, self.f3sfmz.pk]}
        self.job.process(**kwargs)
        exp_t = [
                ((self.f3sf.servershare.name, os.path.join(self.f3sf.path, self.f3sf.filename),
                    self.f3sf.pk, True), {}),
                ((self.f3sfmz.servershare.name, os.path.join(self.f3sfmz.path, self.f3sfmz.filename),
                    self.f3sfmz.pk, False), {})
                ]
        self.check(exp_t)


class TestMoveSingleFile(ProcessJobTest):
    jobclass = rjobs.MoveSingleFile

    def test_mv_fn(self):
        newpath = os.path.split(self.f3sf.path)[0]
        kwargs = {'sf_id': self.f3sf.pk, 'dst_path': newpath}
        self.assertEqual(self.job.check_error(**kwargs), False)
        self.job.process(**kwargs)
        exp_t = [((self.f3sf.filename, self.f3sf.servershare.name, self.f3sf.path, newpath,
            self.f3sf.pk, self.f3sf.servershare.name), {})]
        self.check(exp_t)

    def test_error_duplicatefn(self):
        # Another fn exists w same name
        oldraw = rm.RawFile.objects.create(name=self.f3sf.filename, producer=self.prod,
                source_md5='rename_oldraw_fakemd5', size=10, date=timezone.now(), claimed=True)
        sf = rm.StoredFile.objects.create(rawfile=oldraw, filename=self.f3sf.filename, md5=oldraw.source_md5,
                filetype=self.ft, servershare=self.f3sf.servershare, path='oldpath', checked=True)
        newpath = os.path.split(self.f3path)
        kwargs = {'sf_id': sf.pk, 'dst_path': self.f3sf.path}
        self.assertIn('A file in path', self.job.check_error(**kwargs))
        self.assertIn('already exists. Please choose another', self.job.check_error(**kwargs))

        # A dataset has the same name as the file
        run = dm.RunName.objects.create(name='run1.raw', experiment=self.exp1)
        storloc = os.path.join(self.p1.name, self.exp1.name, self.dtype.name, run.name)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, storageshare=self.ssnewstore, storage_loc=storloc)
        newpath, newname = os.path.split(storloc)
        kwargs = {'sf_id': sf.pk, 'dst_path': newpath, 'newname': newname}
        self.assertIn('A dataset with the same directory name as your new', self.job.check_error(**kwargs))


class TestRenameFile(BaseIntegrationTest):
    url = '/files/rename/'

    def test_renamefile(self):
        # There is no mzML for this sfile:
        self.f3sfmz.delete()
        oldfn = self.f3sf.filename
        oldname, ext = os.path.splitext(oldfn)
        newname = f'renamed_{oldname}'
        newfile_path = os.path.join(self.f3path, f'{newname}{ext}')
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
        self.assertEqual(job.kwargs, kwargs_postdata)
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
                source_md5='rename_oldraw_fakemd5', size=10, date=timezone.now(), claimed=True)
        sf = rm.StoredFile.objects.create(rawfile=oldraw, filename=oldfn, md5=oldraw.source_md5,
                filetype=self.ft, servershare=self.f3sf.servershare, path=self.f3sf.path, checked=True)
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


class TestDeleteFile(BaseIntegrationTest):
    jobname = 'purge_files'

    def test_dir(self):
        kwargs = {'sf_ids': [self.f3sf.pk]}
        file_path = os.path.join(self.f3path, self.f3sf.filename)
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(os.path.isdir(file_path))

        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.SUCCESS)
        self.assertFalse(os.path.exists(file_path))
        self.f3sf.refresh_from_db()
        self.assertTrue(self.f3sf.deleted)
        self.assertTrue(self.f3sf.purged)

    def test_file(self):
        self.f3sf.path = os.path.join(self.f3sf.path, self.f3sf.filename)
        self.f3sf.filename = 'analysis.tdf'
        self.f3sf.save()
        self.ft.is_folder = False
        self.ft.save()
        file_path = os.path.join(self.f3sf.path, self.f3sf.filename)
        file_path = os.path.join(settings.SHAREMAP[self.ssnewstore.name], file_path)
        self.assertTrue(os.path.exists(file_path))
        self.assertFalse(os.path.isdir(file_path))

        kwargs = {'sf_ids': [self.f3sf.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.SUCCESS)
        self.assertFalse(os.path.exists(file_path))
        self.f3sf.refresh_from_db()
        self.assertTrue(self.f3sf.deleted)
        self.assertTrue(self.f3sf.purged)


    def test_no_file(self):
        '''Test without an actual file in a dir will not error, as it will register
        that it has already deleted this file.
        TODO Maybe we SHOULD error, as the file will be not there, so who knows where
        it is now?
        '''
        badfn = 'badraw'
        badraw = rm.RawFile.objects.create(name=badfn, producer=self.prod,
                source_md5='badraw_fakemd5', size=10, date=timezone.now(), claimed=True)
        badsf = rm.StoredFile.objects.create(rawfile=badraw, filename=badfn,
                    md5=badraw.source_md5, filetype=self.ft, servershare=self.ssnewstore,
                    path=self.storloc, checked=True)
        file_path = os.path.join(badsf.path, badsf.filename)
        self.assertFalse(os.path.exists(file_path))

        kwargs = {'sf_ids': [badsf.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        self.assertEqual(job.task_set.get().state, states.SUCCESS)
        badsf.refresh_from_db()
        self.assertTrue(badsf.deleted)
        self.assertTrue(badsf.purged)

    def test_fail_expect_file(self):
        self.ft.is_folder = False
        self.ft.save()
        kwargs = {'sf_ids': [self.f3sf.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.FAILURE)
        path_noshare = os.path.join(self.f3sf.path, self.f3sf.filename)
        full_path = os.path.join(settings.SHAREMAP[self.ssnewstore.name], path_noshare)
        msg = (f'When trying to delete file {path_noshare}, expected a file, but encountered '
                'a directory')
        self.assertEqual(task.taskerror.message, msg)
        self.assertTrue(os.path.exists(full_path))
        self.f3sf.refresh_from_db()
        self.assertFalse(self.f3sf.deleted)
        self.assertFalse(self.f3sf.purged)

    def test_fail_expect_dir(self):
        self.f3sf.path = os.path.join(self.f3sf.path, self.f3sf.filename)
        self.f3sf.filename = 'analysis.tdf'
        self.f3sf.save()
        path_noshare = os.path.join(self.f3sf.path, self.f3sf.filename)
        file_path = os.path.join(settings.SHAREMAP[self.ssnewstore.name], path_noshare)
        self.assertTrue(os.path.exists(file_path))
        self.assertFalse(os.path.isdir(file_path))          

        kwargs = {'sf_ids': [self.f3sf.pk]}
        job = jm.Job.objects.create(funcname=self.jobname, kwargs=kwargs, timestamp=timezone.now(),
                state=jj.Jobstates.PENDING)
        self.run_job()
        task = job.task_set.get()
        self.assertEqual(task.state, states.FAILURE)
        msg = (f'When trying to delete file {path_noshare}, expected a directory, but encountered '
                'a file')
        self.assertEqual(task.taskerror.message, msg)
        self.assertTrue(os.path.exists(file_path))
        self.f3sf.refresh_from_db()
        self.assertFalse(self.f3sf.deleted)
        self.assertFalse(self.f3sf.purged)


#class TestDeleteDataset(BaseIntegrationTest):
