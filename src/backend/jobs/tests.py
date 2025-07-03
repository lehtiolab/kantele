import os
from datetime import datetime
from django.utils import timezone
from django.test import TestCase, Client
from celery import states as cstates


from kantele.tests import BaseTest
from rawstatus import models as rm
from datasets import models as dm
from jobs import models as jm
from kantele import settings


class BaseJobTest(BaseTest):
    def setUp(self):
        super().setUp()
        self.job = jm.Job.objects.create(funcname=self.jobname,
                timestamp=datetime.now(), state='done', kwargs={})
        self.taskid = 'task_abc123'
        self.task = jm.Task.objects.create(job=self.job, asyncid=self.taskid, state='PROCESSING', args=[])


class TestRenamedProject(BaseJobTest):
    url = '/jobs/set/projectname/'
    jobname = 'rename_top_lvl_projectdir'
    p_newname = 'test_newp1'

    def setUp(self):
        super().setUp()
        kwargs={'proj_id': self.p1.pk, 'srcname': self.p1.name, 'newname': self.p_newname}
        self.job.kwargs = kwargs
        self.job.save()

    def test_wrong_client(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'client_id': settings.STORAGECLIENT_APIKEY + 'abc123'})
        self.assertEqual(resp.status_code, 403)
        resp = self.cl.post(self.url, content_type='application/json',
            data={'no_client_id': 1})
        self.assertEqual(resp.status_code, 403)

    def test_normal(self):
        producer = rm.Producer.objects.create(name='testprod', client_id='prod_abc123', shortname='tp')
        sftype = rm.StoredFileType.objects.create(name='test', filetype='tst')
        rf = rm.RawFile.objects.create(name='testrf', producer=producer,
            source_md5='abcdefgh', size=10, date=datetime.now(),
            claimed=True)
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=rf)
        sf = rm.StoredFile.objects.create(rawfile=rf, filename=rf.name,
                md5=rf.source_md5, checked=True, filetype=sftype)
        sfloc = rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssnewstore, path=self.ds.storage_loc,
                purged=False, active=True)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'client_id': settings.STORAGECLIENT_APIKEY, 'task': self.taskid,
            'proj_id': self.p1.pk, 'newname': self.p_newname, 'sfloc_ids': [sfloc.pk]})
        self.assertEqual(resp.status_code, 200)
        newpath = os.path.join(self.p_newname, *self.ds.storage_loc.split(os.path.sep)[1:])
        sfloc.refresh_from_db()
        self.ds.refresh_from_db()
        self.assertEqual(self.ds.storage_loc, newpath)
        self.assertEqual(sfloc.path, newpath)
        self.task.refresh_from_db()
        self.assertEqual(self.task.state, cstates.SUCCESS)


class TestUpdateStorageLocDset(BaseJobTest):
    url = '/jobs/set/storagepathds/'
    jobname = 'rename_dset_storage_loc'

    def test_wrong_client(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'client_id': settings.ANALYSISCLIENT_APIKEY})
        self.assertEqual(resp.status_code, 403)
        resp = self.cl.post(self.url, content_type='application/json',
            data={'no_client_id': 1})
        self.assertEqual(resp.status_code, 403)

    def test_dset_storupdate_ok(self):
        newstorloc = 'another/location'
        sfls = rm.StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset=self.ds,
                servershare_id=self.dss.storageshare_id).values('pk')
        resp = self.cl.post(self.url, content_type='application/json', data={
            'client_id': settings.STORAGECLIENT_APIKEY, 'dss_id': self.dss.pk,
            'dst_path': newstorloc, 'sfloc_ids': [x['pk'] for x in sfls], 'task': self.task.asyncid})
        self.assertEqual(resp.status_code, 200)
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc, newstorloc)
        self.task.refresh_from_db()
        self.assertEqual(self.task.state, cstates.SUCCESS)


# This is already tested in the integration tests
class TestDownloadedFile(BaseJobTest):
    url = '/jobs/set/downloaded/'
    jobname = 'rsync_transfer'

    def setUp(self):
        super().setUp()
        self.fn = 'testrf.zip' 
        self.rf = rm.RawFile.objects.create(name=self.fn, producer=self.prod,
                source_md5='testdownloaded', size=10, date=datetime.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        self.sf = rm.StoredFile.objects.create(rawfile=self.rf, filename=self.rf.name,
            md5=self.rf.source_md5, checked=False, filetype=self.ft)
        self.sfl = rm.StoredFileLoc.objects.create(sfile=self.sf, servershare=self.sstmp, path='',
                purged=False, active=True)
        self.fpath = f'{self.rf.pk}.{self.sf.filetype.filetype}'

    def test_wrong_client(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'client_id': settings.ANALYSISCLIENT_APIKEY})
        self.assertEqual(resp.status_code, 403)
        resp = self.cl.post(self.url, content_type='application/json',
            data={'no_client_id': 1})
        self.assertEqual(resp.status_code, 403)

    def test_md5_but_not_unzip(self):
        with open(os.path.join(settings.TMP_UPLOADPATH, self.fpath), 'w') as fp:
            # touch a file
            pass
        resp = self.cl.post(self.url, content_type='application/json', data={
            'task': self.task.asyncid, 'client_id': settings.STORAGECLIENT_APIKEY,
            'sfloc_id': self.sfl.pk, 'do_md5check': True, 'md5': self.rf.source_md5,
            'unzipped': False})
        self.assertEqual(resp.status_code, 200)
        self.sf.refresh_from_db()
        self.rf.refresh_from_db()
        self.assertTrue(self.sf.checked)
        self.assertEqual(self.sf.filename, self.fn)
        self.assertEqual(self.rf.name, self.fn)
        self.task.refresh_from_db()
        self.assertEqual(self.task.state, cstates.SUCCESS)

    def test_no_md5_but_unzip(self):
        with open(os.path.join(settings.TMP_UPLOADPATH, self.fpath), 'w') as fp:
            # touch a file
            pass
        resp = self.cl.post(self.url, content_type='application/json', data={
            'task': self.task.asyncid, 'client_id': settings.STORAGECLIENT_APIKEY,
            'sfloc_id': self.sfl.pk, 'do_md5check': False, 'md5': self.rf.source_md5,
            'unzipped': True})
        self.assertEqual(resp.status_code, 200)
        self.sf.refresh_from_db()
        self.rf.refresh_from_db()
        self.assertTrue(self.sf.checked)
        self.assertEqual(self.sf.filename, self.fn.rstrip('.zip'))
        self.assertEqual(self.rf.name, self.fn.rstrip('.zip'))
        self.task.refresh_from_db()
        self.assertEqual(self.task.state, cstates.SUCCESS)


class TestUpdateStorageLocFile(BaseJobTest):
    url = '/jobs/set/storagepath/'
    # multiple jobs use this, but take this job
    jobname = 'move_dset_servershare'

    def test_wrong_client(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'client_id': 'fake'})
        self.assertEqual(resp.status_code, 403)
        resp = self.cl.post(self.url, content_type='application/json',
            data={'no_client_id': 1})
        self.assertEqual(resp.status_code, 403)

    def test_one_fnid(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'client_id': settings.ANALYSISCLIENT_APIKEY, 'sfloc_id': self.oldsf.pk,
                    'dst_path': 'new_path', 'task': self.taskid, 'newname': 'newfilename'})
        self.assertEqual(resp.status_code, 200)
        self.oldsss.refresh_from_db()
        self.oldsf.refresh_from_db()
        self.assertEqual(self.oldsss.path, 'new_path')
        self.assertEqual(self.oldsf.filename, 'newfilename')
        self.task.refresh_from_db()
        self.assertEqual(self.task.state, 'SUCCESS')
         
    def test_multifiles(self):
        raw2 = rm.RawFile.objects.create(name='file2', producer=self.prod,
                source_md5='r328j9dqhj32qh98ddh3982q', usetype=rm.UploadFileType.RAWFILE,
                size=100, date=timezone.now(), claimed=False)
        sf2 = rm.StoredFile.objects.create(rawfile=raw2, filename=raw2.name,
                md5=raw2.source_md5, filetype=self.ft)
        sf2loc = rm.StoredFileLoc.objects.create(sfile=sf2, servershare=self.ssnewstore, path='',
                purged=False, active=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'client_id': settings.ANALYSISCLIENT_APIKEY, 'sfloc_ids': [self.oldsf.pk,
                    sf2.pk], 'dst_path': 'new_path', 'task': self.taskid, 'newname': 'newfilename'})
        self.assertEqual(resp.status_code, 200)
        self.oldsss.refresh_from_db()
        self.assertEqual(self.oldsss.path, 'new_path')
        sf2loc.refresh_from_db()
        self.assertEqual(sf2loc.path, 'new_path')
        self.task.refresh_from_db()
        self.assertEqual(self.task.state, 'SUCCESS')
