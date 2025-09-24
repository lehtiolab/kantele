import os
import json
from datetime import datetime
from django.utils import timezone
from django.test import TestCase, Client
from django.contrib.auth.models import User

from kantele import settings
from kantele.tests import BaseTest, BaseIntegrationTest, ProcessJobTest
from datasets import models as dm
from analysis import models as am
from datasets import jobs as dj
from jobs import models as jm
from jobs.jobs import Jobstates
from rawstatus import models as rm


class SaveUpdateDatasetTest(BaseIntegrationTest):
    url = '/datasets/save/dataset/'

    def test_new_dset(self):
        resp = self.post_json(data={'dataset_id': False, 'project_id': self.p1.pk,
            'experiment_id': self.exp1.pk, 'runname': 'newrunname', 'secclass': 1,
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'externalcontact': self.contact.email,
            'storage_shares': [self.ssnewstore.pk]})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.RunName.objects.filter(name='newrunname').count(), 1)
        ds = dm.Dataset.objects.get(runname__name='newrunname', runname__experiment=self.exp1)
        dss = dm.DatasetServer.objects.filter(dataset=ds).first()
        self.assertEqual(ds.datatype_id, self.dtype.pk)
        dspath = os.path.join(self.p1.name, self.exp1.name, self.dtype.name, 'newrunname')
        self.assertEqual(dss.storage_loc_ui, dspath)
        self.assertEqual(dss.storage_loc, dspath)
        self.assertEqual(ds.datasetowner_set.count(), 1)
        self.assertEqual(ds.datasetowner_set.get().user, self.user)
        dsc = ds.datasetcomponentstate_set
        self.assertEqual(dsc.count(), self.dtype.datatypecomponent_set.count())
        self.assertEqual(dsc.filter(state=dm.DCStates.OK).count(), 1)
        self.assertTrue(dsc.filter(state=dm.DCStates.OK, dtcomp=self.dtcompdef).exists())
        self.assertEqual(dsc.filter(state=dm.DCStates.NEW).count(), self.dtype.datatypecomponent_set.count() - 1)
        self.assertEqual(dm.ProjectLog.objects.last().message, f'Dataset {ds.pk} created by user {self.user.id}')

    def test_update_dset_newexp_location(self):
        newexpname = 'edited_exp'
        self.assertEqual(dm.Experiment.objects.filter(name=newexpname).count(), 0)
        new_ds_loc = os.path.join(self.p1.name, newexpname, self.dtype.name, self.run1.name)
        self.assertNotEqual(self.dss.storage_loc, new_ds_loc)
        resp = self.post_json(data={'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name,
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'storage_shares': [self.ssnewstore.pk],
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.Experiment.objects.filter(name=newexpname).count(), 1)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.pk} changed dataset '
                f'{self.ds.pk} path to {new_ds_loc} from {self.dss.storage_loc}').exists())
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc_ui, new_ds_loc)
        self.assertNotEqual(self.dss.storage_loc, new_ds_loc)
        self.assertTrue(os.path.exists(self.f3path))
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        self.run_job()
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc, new_ds_loc)
        self.assertFalse(os.path.exists(self.f3path))
        self.assertTrue(os.path.exists(os.path.join(self.newstorctrl.path,
            self.dss.storage_loc_ui, self.f3sf.filename)))

    def test_remove_files_wait_for_rename(self):
        '''First queue a move dataset job, to new experiment name. Then queue a remove
        files job from dataset. The second job should wait for the first one, so the removed
        file should first be moved to the new dset location'''
        newexpname = 'edited_exp1'
        # move dataset
        mvdsresp = self.post_json({'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name, 
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'storage_shares': [self.ssnewstore.pk],
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(mvdsresp.status_code, 200)
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        self.assertTrue(dm.ProjectLog.objects.filter(message__startswith=f'User {self.user.pk} '
            f'changed dataset {self.ds.pk} path to').exists())
        # remove files results in a job and claimed files still on tmp
        rmresp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'removed_files': {self.f3sf.pk: {'id': self.f3sf.pk}},
            'added_files': {}})
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.pk} '
            f'removed files {self.f3sf.pk} from dataset {self.ds.pk}').exists())
        self.assertEqual(rmresp.status_code, 200)
        self.assertTrue(self.f3raw.claimed)
        self.assertEqual(self.f3sss.servershare, self.ssnewstore)
        self.assertEqual(self.f3sss.path, self.dss.storage_loc)
        self.f3raw.refresh_from_db()
        self.assertFalse(self.f3raw.claimed)
        self.assertFalse(dm.DatasetRawFile.objects.filter(rawfile=self.f3raw).exists())
        # execute dataset move on disk, should also move the removed files and update their DB
        # the move job should be in waiting state still
        self.run_job()
        mvjob = jm.Job.objects.filter(funcname='remove_dset_files_servershare').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.dss.refresh_from_db()
        self.f3sss.refresh_from_db()
        self.assertEqual(self.f3sss.servershare, self.ssnewstore)
        self.assertEqual(self.f3sss.path, self.dss.storage_loc_ui)
        self.assertEqual(self.f3sss.path, self.dss.storage_loc)
        f3sf_path = os.path.join(self.newstorctrl.path, self.f3sss.path, self.f3sf.filename)
        self.assertTrue(os.path.exists(f3sf_path))
        # Now we actually run the rm operation
        self.run_job()
        rename_job.refresh_from_db()
        self.assertEqual(rename_job.state, Jobstates.DONE)
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        self.assertFalse(os.path.exists(f3sf_path))
        self.f3sss.refresh_from_db()
        self.assertTrue(self.f3sss.purged)

    def test_add_files_wait_for_rename(self):
        '''Another job is running on dataset that changed the storage_loc,
        do not add new files to old storage_loc'''
        # FIXME maybe hardcode file paths instead of relying on storage_path
        newexpname = 'edited_exp1'
        # move dataset
        mvdsresp = self.post_json({'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name,
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'storage_shares': [self.ssnewstore.pk],
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(mvdsresp.status_code, 200)
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        # add files results in a job and claimed files still on tmp
        resp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {self.tmpsf.pk: {'id': self.tmpsf.pk}},
            'removed_files': {}})
        self.assertEqual(resp.status_code, 200)
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertFalse(self.tmpraw.claimed)
        self.assertEqual(self.tmpsss.servershare, self.ssinbox)
        self.assertEqual(self.tmpsss.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        # execute dataset move on disk, should not move the added files on tmp (nor update their DB)
        self.run_job()
        mvjob = jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.ds.refresh_from_db()
        self.dss.refresh_from_db()
        newtmpsf_path = os.path.join(self.newstorctrl.path, self.dss.storage_loc, self.tmpsf.filename)
        self.assertFalse(os.path.exists(newtmpsf_path))
        self.run_job()
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # a new file should now exist in dset folder
        self.tmpsss.refresh_from_db()
        self.assertEqual(self.tmpsss.servershare, self.ssinbox)
        self.assertEqual(self.tmpsss.path, '')
        self.assertTrue(os.path.exists(newtmpsf_path))
        newsfl = rm.StoredFileLoc.objects.filter(sfile=self.tmpsss.sfile_id, path=self.dss.storage_loc,
                pk__gt=self.tmpsss.pk, servershare=self.ssnewstore)
        self.assertTrue(newsfl.exists())
    
    def test_fail_storageloc_is_filename(self):
        # Create file with dset storloc path/name
        fpath, fname = os.path.join(self.p1.name, self.exp1.name, self.dtype.name), 'file_dirname'
        raw = rm.RawFile.objects.create(name=fname, producer=self.prod,
                source_md5='storloc_raw_fakemd5', size=100, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        sf = rm.StoredFile.objects.create(rawfile=raw, md5=raw.source_md5,
                filename=raw.name, checked=True, filetype=self.ft)
        rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssnewstore, path=fpath,
                purged=False, active=True)
        # Try to create new dset 
        resp = self.post_json(data={'dataset_id': False, 'project_id': self.p1.pk,
            'experiment_id': self.exp1.pk, 'runname': fname, 'datatype_id': self.dtype.pk,
            'secclass': 1, 'prefrac_id': False, 'externalcontact': self.contact.email,
            'storage_shares': [self.ssnewstore.pk],
            })
        self.assertEqual(resp.status_code, 403)
        self.assertIn('There is already a file with that exact path', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())

        # Try to update existing dataset
        dm.RunName.objects.filter(experiment=self.exp1, name=fname).delete()
        resp = self.post_json(data={'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'experiment_id': self.exp1.pk, 'runname': fname, 'datatype_id': self.dtype.pk,
            'prefrac_id': False, 'ptype_id': self.ptype.pk,
            'storage_shares': [self.ssnewstore.pk],
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('There is already a file with that exact path', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())


class FindFilesTest(BaseTest):
    url = '/datasets/find/files/'
    maxDiff = None

    def test_find_files(self):
        tmpraw2 = rm.RawFile.objects.create(name='raw23', producer=self.prod,
                source_md5='tmpraw23_fakemd5', size=10, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        tmpsf = rm.StoredFile.objects.create(rawfile=tmpraw2, md5=tmpraw2.source_md5,
                filename=tmpraw2.name, checked=True, filetype=self.ft)
        tmpsss = rm.StoredFileLoc.objects.create(sfile=tmpsf, servershare=self.ssinbox,
                path='', active=True, purged=False)
        raw2sf = {tmpraw2.pk: tmpsf.pk, self.tmpraw.pk: self.tmpsf.pk}
        resp = self.cl.get(self.url, data={'q': [self.tmpraw.name[0], self.tmpraw.name[-1]]})
        self.assertEqual(resp.status_code, 200)
        self.assertJSONEqual(resp.content.decode('utf-8'),
                {'newfn_order': [tmpsf.pk, self.tmpsf.pk],
                    'newFiles': {f'{raw2sf[x.pk]}': {'id': raw2sf[x.pk], 'name': x.name, 
                        'size': round(x.size / (2**20), 1),
                        'date': x.date.timestamp() * 1000,
                        'instrument': x.producer.name, 'checked': False}
                        for x in [self.tmpraw, tmpraw2]}})

    def test_find_no_files(self):
        resp = self.cl.get(self.url, data={'q': ['h3289renweq89nd892nwoei']})
        self.assertEqual(resp.status_code, 200)
        self.assertJSONEqual(resp.content.decode('utf-8'), {'newfn_order': [], 'newFiles': {}})


class UpdateFilesTest(BaseIntegrationTest):
    url = '/datasets/save/files/'

    def test_add_files(self):
        '''Add files, check if added, also check if the job waits for another job on the dataset'''
        tmpraw2 = rm.RawFile.objects.create(name='raw2a', producer=self.prod,
                source_md5='tmpraw23_fakemd5', size=10, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        tmpsf2 = rm.StoredFile.objects.create(rawfile=tmpraw2, md5=tmpraw2.source_md5,
                filename=tmpraw2.name, checked=True, filetype=self.ft)
        tmpsss = rm.StoredFileLoc.objects.create(sfile=tmpsf2, servershare=self.ssinbox,
                path='', active=True, purged=False)
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)
        resp = self.post_json({'dataset_id': self.ds.pk, 'added_files': {self.tmpsf.pk: {'id': self.tmpsf.pk}, tmpsf2.pk: {'id':tmpsf2.pk}},
            'removed_files': {}})
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.pk} '
            f'added files {self.tmpsf.pk},{tmpsf2.pk} to dataset {self.ds.pk}').exists())
        self.assertEqual(resp.status_code, 200)
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertFalse(self.tmpraw.claimed)
        self.assertEqual(self.tmpsss.servershare, self.ssinbox)
        self.assertEqual(self.tmpsss.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        newsfl = rm.StoredFileLoc.objects.filter(sfile__in=[self.tmpsf, tmpsf2],
                servershare=self.ssnewstore, path=self.dss.storage_loc, purged=False)
        self.assertEqual(newsfl.count(), 0)
        self.run_job()
        self.assertEqual(newsfl.count(), 2)
        self.assertTrue(os.path.exists(os.path.join(self.newstorctrl.path,  self.dss.storage_loc,
            self.tmpsf.filename)))
        self.assertTrue(os.path.exists(os.path.join(self.inboxctrl.path, self.tmpsf.filename)))
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
    
    def test_add_fails(self):
        # Fail because there is no storedfile
        fn = 'raw_no_sf'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod, claimed=False,
                source_md5='raw_no_sf_fakemd5', size=1024, date=timezone.now(),
                usetype=rm.UploadFileType.RAWFILE)
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)
        resp = self.post_json({'dataset_id': self.ds.pk, 'added_files': {raw.pk: {'id': raw.pk}},
            'removed_files': {}})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Cannot find some of the files asked to store', json.loads(resp.content)['error'])
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=raw)
        self.assertEqual(newdsr.count(), 0)
        self.assertFalse(raw.claimed)
        self.assertFalse(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_remove_files(self):
        resp = self.cl.post(self.url, content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {},
            'removed_files': {self.f3sf.pk: {'id': self.f3sf.pk}}})
        self.f3sf.refresh_from_db()
        self.f3raw.refresh_from_db()
        self.f3sss.refresh_from_db()
        self.assertFalse(self.f3sss.purged)
        self.run_job()
        self.f3sss.refresh_from_db()
        dsr = dm.DatasetRawFile.objects.filter(rawfile=self.f3raw, dataset=self.ds)
        self.assertFalse(dsr.exists())
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(self.f3raw.claimed)
        self.assertFalse(os.path.exists(os.path.join(self.newstorctrl.path,
            self.dss.storage_loc, self.f3sf.filename)))
        self.assertEqual(self.f3sss.servershare, self.dss.storageshare)
        self.assertEqual(self.f3sss.path, self.dss.storage_loc)
        self.assertTrue(self.f3sss.purged)
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} removed files '
                f'{self.f3sf.pk} from dataset {self.ds.pk}').exists())


    def test_trigger_movejob_errors(self):
        # add file with same name as one already in dset
        dupe_raw = rm.RawFile.objects.create(name=self.f3raw.name, producer=self.prod,
                source_md5='tmpraw_dupe_fakemd5', size=100, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.RAWFILE)
        dupe_sf = rm.StoredFile.objects.create(rawfile=dupe_raw, md5=dupe_raw.source_md5,
                filename=dupe_raw.name, checked=True, filetype=self.ft)
        dupeloc = rm.StoredFileLoc.objects.create(sfile=dupe_sf, servershare=self.ssinbox, path='',
                purged=False, active=True)
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {dupe_sf.pk: {'id': dupe_sf.pk}},
            'removed_files': {}})
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=dupe_raw)
        self.assertEqual(newdsr.count(), 0)
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(dupe_raw.claimed)
        self.assertIn(f'There is already a file existing with the same name', resp.json()['error'])
        self.assertEqual(dupeloc.servershare, self.ssinbox)
        self.assertEqual(dupeloc.path, '')
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_dset_is_filename_job_error(self):
        # new file is dir w same name as dset storage dir
        run = dm.RunName.objects.create(name='newrun', experiment=self.exp1)
        newpath, newfn = os.path.split(self.dss.storage_loc)
        self.tmpsf.filename = newfn
        self.tmpsf.save()
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        newdss = dm.DatasetServer.objects.create(dataset=newds, storageshare=self.ssnewstore,
                storage_loc=newpath, storage_loc_ui=newpath, startdate=timezone.now())
        dm.DatasetOwner.objects.get_or_create(dataset=newds, user=self.user)
        dtc = newds.datasetcomponentstate_set.create(dtcomp=self.dtcompfiles, state=dm.DCStates.NEW)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'dataset_id': newds.pk, 'added_files': {self.tmpsf.pk: {'id': self.tmpsf.pk}},
            'removed_files': {}})
        dsr = dm.DatasetRawFile.objects.filter(rawfile=self.tmpraw, dataset=self.ds)
        self.assertEqual(dsr.count(), 0)
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(self.tmpraw.claimed)
        self.assertIn(f'There is already a dataset with the exact path as the target file', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())
        self.tmpsss.refresh_from_db()
        self.assertEqual(self.tmpsss.servershare, self.ssinbox)
        self.assertEqual(self.tmpsss.path, '')
        dtc.refresh_from_db()
        self.assertEqual(dtc.state, dm.DCStates.NEW)


class AcceptRejectPreassocFiles(BaseIntegrationTest):
    url = '/datasets/save/files/pending/'

    def setUp(self):
        super().setUp()
        # Add tmpraw to claimed so it can be accepted/rejected
        self.tmpraw.claimed = True
        self.tmpraw.save()
        self.newsfl = rm.StoredFileLoc.objects.create(sfile=self.tmpsf, path=self.dss.storage_loc,
                servershare=self.dss.storageshare) 
        jm.Job.objects.create(funcname='rsync_dset_files_to_servershare', state=Jobstates.HOLD,
                kwargs={'dss_id': self.dss.pk, 'dstsfloc_ids': [self.newsfl.pk],
                    'dstshare_id': self.ssnewstore.pk, 'sfloc_ids': [self.tmpsss.pk]},
                timestamp=timezone.now())
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)

    def test_accept_all_files(self):
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        resp = self.post_json({'dataset_id': self.ds.pk, 'accepted_files': [self.tmpsf.pk],
            'rejected_files': []})
        self.assertEqual(resp.status_code, 200)
        self.newsfl.refresh_from_db()
        self.assertTrue(self.newsfl.active)
        self.assertTrue(self.newsfl.purged)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} accepted storedfiles '
                f'{self.tmpsf.pk}, to dset {self.ds.pk}').exists())
        self.assertEqual(newdsr.count(), 1)
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        self.run_job()
        self.newsfl.refresh_from_db()
        self.assertFalse(self.newsfl.purged)
        self.assertTrue(os.path.exists(os.path.join(self.newstorctrl.path,
            self.dss.storage_loc, self.tmpsf.filename)))
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
    
    def test_reject_all_files(self):
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        resp = self.post_json({'dataset_id': self.ds.pk, 'rejected_files': [self.tmpsf.pk],
            'accepted_files': []})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(dm.ProjectLog.objects.exists())
        self.newsfl.refresh_from_db()
        self.assertFalse(self.newsfl.active)
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        self.tmpraw.refresh_from_db()
        self.assertFalse(self.tmpraw.claimed)
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.inboxctrl.path, self.tmpsf.filename)))
        self.assertFalse(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())

    def test_accept_some_files(self):
        rejraw = rm.RawFile.objects.create(name='reject.raw', producer=self.prod,
                source_md5='rejectit_fakemd5', size=123, date=timezone.now(),
                claimed=True, usetype=rm.UploadFileType.RAWFILE)
        rejsf = rm.StoredFile.objects.create(rawfile=rejraw, filename=rejraw.name,
                filetype=self.ft, md5=rejraw.source_md5, checked=True)
        rejsfl = rm.StoredFileLoc.objects.create(sfile=rejsf, servershare=self.ssinbox, path='',
                purged=False, active=True)
        rejdstsfl = rm.StoredFileLoc.objects.create(sfile=rejsf, servershare=self.ssnewstore,
                path=self.dss.storage_loc)
        jm.Job.objects.create(funcname='rsync_dset_files_to_servershare', state=Jobstates.HOLD,
                kwargs={'dss_id': self.dss.pk, 'dstsfloc_ids': [rejdstsfl.pk],
                    'dstshare_id': self.ssnewstore.pk, 'sfloc_ids': [rejsfl.pk]},
                timestamp=timezone.now())
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        resp = self.post_json({'dataset_id': self.ds.pk, 'accepted_files': [self.tmpsf.pk],
            'rejected_files': [rejsf.pk]})
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} accepted storedfiles '
                f'{self.tmpsf.pk}, to dset {self.ds.pk}').exists())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(newdsr.count(), 1)
        rejectdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=rejraw)
        self.assertEqual(rejectdsr.count(), 0)
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        self.newsfl.refresh_from_db()
        self.assertTrue(self.newsfl.active)
        self.assertTrue(self.newsfl.purged)
        rejdstsfl.refresh_from_db()
        self.assertFalse(rejdstsfl.active)
        self.run_job()
        self.newsfl.refresh_from_db()
        self.assertFalse(self.newsfl.purged)
        self.assertTrue(os.path.exists(os.path.join(self.newstorctrl.path,
            self.dss.storage_loc, self.tmpsf.filename)))
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())


class RenameProjectTest(BaseIntegrationTest):
    url = '/datasets/rename/project/'

    def test_no_ownership_fail(self):
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dss = dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': 'testnewp'})
        self.assertEqual(resp.status_code, 403)

    def test_id_name_fails(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': self.p1.name})
        self.assertEqual(resp.status_code, 403)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk+1000, 'newname': 'testnewname'})
        self.assertEqual(resp.status_code, 404)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': 'testnewname with spaces'})
        self.assertEqual(resp.status_code, 403)
        self.assertIn(f'cannot contain characters except {settings.ALLOWED_PROJEXPRUN_CHARS}',
                json.loads(resp.content)['error'])
        oldp = dm.Project.objects.create(name='project to rename', pi=self.pi, ptype=self.ptype)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': oldp.pk, 'newname': self.p1.name})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Cannot change name to existing name for project', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_rename_ok(self):
        newname = 'testnewname'
        self.assertEqual(dm.Project.objects.filter(name=newname).count(), 0)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': newname})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} renamed project '
            f'from {self.p1.name} to {newname}').exists())
        old_loc = self.dss.storage_loc
        new_loc = os.path.join(newname, self.exp1.name, self.dtype.name, self.run1.name)
        renamejobs = jm.Job.objects.filter(funcname='rename_dset_storage_loc',
                kwargs={'dss_id': self.dss.pk, 'newpath': new_loc,
                    'sfloc_ids': [self.f3sss.pk, self.f3mzsss.pk]}) 
        self.assertEqual(renamejobs.count(), 1)
        self.p1.refresh_from_db()
        self.assertEqual(self.p1.name, newname)
        self.assertTrue(os.path.exists(self.f3path))
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc_ui, new_loc)
        self.run_job()
        self.assertFalse(os.path.exists(self.f3path))
        self.assertEqual(self.dss.storage_loc, old_loc)
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc, new_loc)
        self.assertTrue(os.path.exists(os.path.join(self.newstorctrl.path,
            self.dss.storage_loc, self.f3sf.filename)))

    def test_if_added_removed_files_ok(self):
        # first rename job http request, then add files, then jobs run:
        newname = 'testnewname'
        self.assertEqual(dm.Project.objects.filter(name=newname).count(), 0)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': newname})
        self.assertEqual(resp.status_code, 200)
        new_loc = os.path.join(newname, self.exp1.name, self.dtype.name, self.run1.name)
        renamejobs = jm.Job.objects.filter(funcname='rename_dset_storage_loc',
                kwargs={'dss_id': self.dss.pk, 'newpath': new_loc,
                    'sfloc_ids': [self.f3sss.pk, self.f3mzsss.pk]}) 
        self.assertEqual(renamejobs.count(), 1)
        self.p1.refresh_from_db()
        self.assertEqual(self.p1.name, newname)
        # add files results in a job and claimed files still on tmp
        mvresp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {self.tmpsf.pk: {'id': self.tmpsf.pk}},
            'removed_files': {}})
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertEqual(mvresp.status_code, 200)
        self.assertFalse(self.tmpraw.claimed)
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        # Now test rename project job
        self.assertTrue(os.path.exists(self.f3path))
        old_loc = self.dss.storage_loc
        new_loc = os.path.join(newname, self.exp1.name, self.dtype.name, self.run1.name)
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc_ui, new_loc)
        self.assertEqual(self.dss.storage_loc, old_loc)
        self.run_job()
        self.assertFalse(os.path.exists(self.f3path))
        self.dss.refresh_from_db()
        self.assertEqual(self.dss.storage_loc, new_loc)
        self.assertTrue(os.path.exists(os.path.join(self.newstorctrl.path,
            self.dss.storage_loc, self.f3sf.filename)))
        # Check if added files are not there yet, being waited
        mvjob = jm.Job.objects.filter(funcname='rsync_dset_files_to_servershare').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.assertEqual(self.tmpsss.servershare, self.ssinbox)
        self.assertEqual(self.tmpsss.path, '')
        self.ds.refresh_from_db()
        self.tmpsss.refresh_from_db()
        self.assertEqual(self.tmpsss.servershare, self.ssinbox)
        self.assertEqual(self.tmpsss.path, '')
        newtmpsf_path = os.path.join(self.newstorctrl.path, new_loc, self.tmpsf.filename)
        self.assertFalse(os.path.exists(newtmpsf_path))
        newsfl = rm.StoredFileLoc.objects.filter(sfile=self.tmpsss.sfile,
                servershare=self.ssnewstore, path=self.dss.storage_loc, purged=False)
        self.assertFalse(newsfl.exists())
        self.run_job()
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # tmp file should now exist in dset folder
        self.assertTrue(os.path.exists(newtmpsf_path))
        self.assertTrue(newsfl.exists())

# FIXME write tests
#class SaveAcquisition(BaseTest):
#class SaveSampleprep(BaseTest):


class SaveSamples(BaseTest):
    url = '/datasets/save/samples/'

    def test_fails(self):
        newrun = dm.RunName.objects.create(name='failrun', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        newdss = dm.DatasetServer.objects.create(dataset=newds, storage_loc=newrun.name,
                storage_loc_ui=newrun.name, storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=newds, user=otheruser)

        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': newds.pk})
        self.assertEqual(resp.status_code, 403)
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        qch2 = dm.QuantChannel.objects.create(name='thech2')
        qtch2 = dm.QuantTypeChannel.objects.create(quanttype=self.qt, channel=qch2) 
        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': 'blah',
                        'sampletypes': [{'id': self.samtype1.pk}],
                        'species': [{'id': self.spec1.pk}],
                        }],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 400)
        self.assertJSONEqual(resp.content.decode('utf-8'),
                {'error': 'Samples and species need to be specified for all files or channels'})

    def test_save_new_samples_multiplex(self):
        newrun = dm.RunName.objects.create(name='newds_nosamples_plex', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=newds, storage_loc=newrun.name,
                storage_loc_ui=newrun.name, storageshare=self.ssnewstore, startdate=timezone.now())
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        samplename = 'new proj sample A'
        dm.DatasetComponentState.objects.get_or_create(dataset=newds,
                defaults={'state': dm.DCStates.NEW, 'dtcomp': self.dtcompsamples})

        psam = dm.ProjectSample.objects.filter(sample=samplename, project=newrun.experiment.project)
        self.assertEqual(psam.count(), 0)

        req = {'dataset_id': newds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': samplename,
                        'sampletypes': [{'id': self.samtype1.pk}, {'id': self.samtype2.pk}],
                        'species': [{'id': self.spec1.pk}],
                        }],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(psam.count(), 1)
        psam = psam.get()
        self.assertEqual(psam.samplematerial_set.count(), 2)
        self.assertEqual(psam.samplespecies_set.count(), 1)
        self.assertEqual(psam.datasetsample_set.filter(dataset=newds).count(), 1)

        self.assertEqual(psam.quantchannelsample_set.filter(dataset=newds, channel=self.qtch).count(), 1)
        self.assertFalse(hasattr(psam, 'quantsamplefile'))

    def test_save_new_samples_files(self):
        # Create dset
        newrun = dm.RunName.objects.create(name='newds_nosamples_fns', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        newdss = dm.DatasetServer.objects.create(dataset=newds, storage_loc=newrun.name,
                storage_loc_ui=newrun.name, storageshare=self.ssnewstore, startdate=timezone.now())
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        dm.DatasetComponentState.objects.get_or_create(dataset=newds,
                defaults={'state': dm.DCStates.NEW, 'dtcomp': self.dtcompsamples})

        # Add file to dset
        fn = 'raw_lf_dset'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod,
                source_md5='rawlf_ds_fakemd5', size=2024, date=timezone.now(),
                claimed=True, usetype=rm.UploadFileType.RAWFILE)
        dsr = dm.DatasetRawFile.objects.create(dataset=newds, rawfile=raw)

        samplename = 'new proj sample B'
        psam = dm.ProjectSample.objects.filter(sample=samplename, project=newrun.experiment.project)
        self.assertEqual(psam.count(), 0)

        req = {'dataset_id': newds.pk,
                'qtype': self.lfqt.pk,
                'multiplex': False,
                'samples': {dsr.pk: {
                    'model': False,
                    'samplename': samplename,
                    'sampletypes': [{'id': self.samtype2.pk}],
                    'species': [{'id': self.spec1.pk}, {'id': self.spec2.pk}],
                    }},
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(psam.count(), 1)
        psam = psam.get()
        self.assertEqual(psam.samplematerial_set.count(), 1)
        self.assertEqual(psam.samplespecies_set.count(), 2)
        self.assertEqual(psam.datasetsample_set.filter(dataset=newds).count(), 1)
        self.assertEqual(psam.quantchannelsample_set.count(), 0)
        self.assertEqual(psam.quantsamplefile_set.filter(rawfile=dsr).count(), 1)

    # FIXME LF -> Plex? etc?
    def test_update_samples_multiplex_newsample(self):
        # New sample on existing multiplex dset
        samplename = 'upd_sam plex new projsample'
        psam = dm.ProjectSample.objects.filter(sample=samplename, project=self.ds.runname.experiment.project)
        self.assertEqual(psam.count(), 0)

        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': samplename,
                        'sampletypes': [{'id': self.samtype1.pk}, {'id': self.samtype2.pk}],
                        'species': [{'id': self.spec1.pk}],
                        }],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(psam.count(), 1)
        psam = psam.get()
        self.assertEqual(psam.samplematerial_set.count(), 2)
        self.assertEqual(psam.samplespecies_set.count(), 1)
        self.assertEqual(psam.datasetsample_set.filter(dataset=self.ds).count(), 1)
        self.assertEqual(psam.quantchannelsample_set.filter(dataset=self.ds, channel=self.qtch).count(), 1)
        self.assertFalse(hasattr(psam, 'quantsamplefile'))

    def test_update_samples_multiplex_samplechange_fail_mixedinput(self):
        # Changed sample info (organism, type) on existing multiplex dset, 
        # FAILS since two samples should be identical in type/species
        qch2 = dm.QuantChannel.objects.create(name='thech2')
        qtch2 = dm.QuantTypeChannel.objects.create(quanttype=self.qt, channel=qch2) 
        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': self.projsam1.sample,
                        'sampletypes': [{'id': self.samtype2.pk}],
                        'species': [{'id': self.spec2.pk}]},
                        {'id': qtch2.pk,
                            'model': False,
                            'samplename': self.projsam1.sample,
                            'sampletypes': [{'id': self.samtype1.pk}],
                            'species': [{'id': self.spec2.pk}],
                            }
                        ],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 400)
        self.assertJSONEqual(resp.content.decode('utf-8'),
                {'error': 'Sampletypes need to be identical for identical sample IDs, '
                f'check {self.projsam1.sample}'})

    def test_update_samples_multiplex_samplechange_onedset(self):
        # New sample info (organism, type) on existing multiplex dset, 
        # updates since sample is NOT in use in another dataset 
        # (otherwise one would change both dataset's samples)
        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': self.projsam1.sample,
                        'sampletypes': [{'id': self.samtype2.pk}],
                        'species': [{'id': self.spec2.pk}]},
                        ],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.projsam1.refresh_from_db()
        self.assertEqual(self.projsam1.samplematerial_set.get().sampletype, self.samtype2)
        self.assertEqual(self.projsam1.samplespecies_set.get().species, self.spec2)
        self.assertEqual(self.projsam1.datasetsample_set.filter(dataset=self.ds).count(), 1)
        self.assertEqual(self.projsam1.quantchannelsample_set.filter(dataset=self.ds, channel=self.qtch).count(), 1)
        self.assertFalse(hasattr(self.projsam1, 'quantsamplefile'))

    def test_update_samples_multiplex_samplechange_alreadyinuse_multidset(self):
        # New sample info (organism, type) on existing multiplex dset, 
        # will not update since sample is in use in another dataset
        newrun = dm.RunName.objects.create(name='newds_samples_plex', experiment=self.ds.runname.experiment)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=newds, storage_loc=newrun.name,
                storageshare=self.ssnewstore, startdate=timezone.now())
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        dm.DatasetSample.objects.create(dataset=newds, projsample=self.projsam1)
        dm.QuantChannelSample.objects.create(dataset=newds, channel=self.qtch, projsample=self.projsam1)
        dm.DatasetComponentState.objects.get_or_create(dataset=newds,
                defaults={'state': dm.DCStates.NEW, 'dtcomp': self.dtcompsamples})
        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': self.projsam1.sample,
                        'sampletypes': [{'id': self.samtype2.pk}],
                        'species': [{'id': self.spec2.pk}],
                        }],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 400)
        errjson = json.loads(resp.content)
        self.assertEqual(errjson['error'],
                'Project samples exist in database, please validate the sample IDs')
        projsam_err = {self.qtch.pk : {'id': self.projsam1.pk,
            'duprun_example': f'{newrun.experiment.name} - {newrun.name}',
            'sampletypes_error': [
                {'id': self.samtype1.pk, 'name': self.samtype1.name, 'add': True, 'remove': False},
                {'id': self.samtype2.pk, 'name': self.samtype2.name, 'add': False, 'remove': True}],
            'species_error': [
                {'id': self.spec1.pk, 'name': self.spec1.popname,
                'linnean': self.spec1.linnean, 'add': True, 'remove': False},
                {'id': self.spec2.pk, 'name': self.spec2.popname,
                'linnean': self.spec2.linnean, 'add': False, 'remove': True}],
            }}
        self.assertEqual(errjson['sample_dups'], json.loads(json.dumps(projsam_err)))
        self.projsam1.refresh_from_db()
        self.assertEqual(self.projsam1.samplematerial_set.get().sampletype, self.samtype1)
        self.assertEqual(self.projsam1.samplespecies_set.get().species, self.spec1)
        self.assertEqual(self.projsam1.datasetsample_set.count(), 2)
        self.assertEqual(self.projsam1.quantchannelsample_set.count(), 2)
        self.assertFalse(hasattr(self.projsam1, 'quantsamplefile'))

    def test_update_samples_multiplex_already_exist_identical(self):
        # Test case for sample that exists already in the project but not dataset
        # Samples passed have identical species/sampletype as existing ones
        projsam = dm.ProjectSample.objects.create(sample='sample test yoyo', project=self.p1)
        dm.SampleMaterial.objects.create(sample=projsam, sampletype=self.samtype2)
        dm.SampleSpecies.objects.create(sample=projsam, species=self.spec2)

        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': projsam.sample,
                        'sampletypes': [{'id': self.samtype2.pk}],
                        'species': [{'id': self.spec2.pk}]},
                        ],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 400)
        errjson = json.loads(resp.content)
        self.assertEqual(errjson['error'],
                'Project samples exist in database, please validate the sample IDs')
        projsam_err = {self.qtch.pk : {'id': projsam.pk,
            'duprun_example': 'not used in dataset, only registered',
            'sampletypes_error': [],
            'species_error': []}}
        self.assertEqual(errjson['sample_dups'], json.loads(json.dumps(projsam_err)))

    def test_update_samples_files_newsample(self):
        # Change proj sample for existing datasetrawfile to another sample
        samplename = 'new proj sample update_files'
        psam = dm.ProjectSample.objects.filter(sample=samplename, project=self.oldds.runname.experiment.project)
        self.assertEqual(psam.count(), 0)

        req = {'dataset_id': self.oldds.pk,
                'qtype': self.lfqt.pk,
                'multiplex': False,
                'samples': {self.olddsr.pk: {
                    'model': False,
                    'samplename': samplename,
                    'sampletypes': [{'id': self.samtype2.pk}],
                    'species': [{'id': self.spec1.pk}]
                    }},
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(psam.count(), 1)
        psam = psam.get()
        self.assertEqual(psam.samplematerial_set.count(), 1)
        self.assertEqual(psam.samplespecies_set.count(), 1)
        self.assertEqual(psam.datasetsample_set.filter(dataset=self.oldds).count(), 1)
        self.assertEqual(psam.quantchannelsample_set.count(), 0)
        self.assertEqual(psam.quantsamplefile_set.filter(rawfile=self.olddsr).count(), 1)

    def test_update_samples_multiplex_two_identical_new_samples(self):
        # Multiple identical new samples on existing multiplex dset
        qch2 = dm.QuantChannel.objects.create(name='thech2')
        qtch2 = dm.QuantTypeChannel.objects.create(quanttype=self.qt, channel=qch2) 
        samplename = 'upd_sam plex new projsample twosamples'
        psam = dm.ProjectSample.objects.filter(sample=samplename, project=self.ds.runname.experiment.project)
        self.assertEqual(psam.count(), 0)

        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': samplename,
                        'sampletypes': [{'id': self.samtype1.pk}, {'id': self.samtype2.pk}],
                        'species': [{'id': self.spec1.pk}],
                        },
                        {'id': qtch2.pk,
                        'model': False,
                        'samplename': samplename,
                        'sampletypes': [{'id': self.samtype1.pk}, {'id': self.samtype2.pk}],
                        'species': [{'id': self.spec1.pk}],
                        }],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(psam.count(), 1)
        psam = psam.get()
        self.assertEqual(psam.samplematerial_set.count(), 2)
        self.assertEqual(psam.samplespecies_set.count(), 1)
        self.assertEqual(psam.datasetsample_set.filter(dataset=self.ds).count(), 1)
        self.assertEqual(psam.quantchannelsample_set.filter(dataset=self.ds, channel=self.qtch).count(), 1)
        self.assertEqual(psam.quantchannelsample_set.filter(dataset=self.ds, channel=qtch2).count(), 1)
        self.assertFalse(hasattr(psam, 'quantsamplefile'))

    def test_update_samples_multiplex_two_identical_existing_samples(self):
        # Multiple identical existing samples on existing multiplex dset
        qch2 = dm.QuantChannel.objects.create(name='thech2')
        qtch2 = dm.QuantTypeChannel.objects.create(quanttype=self.qt, channel=qch2) 
        # EXISTING samples on existing multiplex dset, update sampletype
        req = {'dataset_id': self.ds.pk,
                'qtype': self.qt.pk,
                'multiplex': {
                    'chans': [{'id': self.qtch.pk,
                        'model': False,
                        'samplename': self.projsam1.sample,
                        'sampletypes': [{'id': self.samtype1.pk}, {'id': self.samtype2.pk}],
                        'species': [{'id': self.spec1.pk}],
                        },
                        {'id': qtch2.pk,
                        'model': False,
                        'samplename': self.projsam1.sample,
                        'sampletypes': [{'id': self.samtype1.pk}, {'id': self.samtype2.pk}],
                        'species': [{'id': self.spec1.pk}],
                        }],
                    },
                }
        resp = self.cl.post(self.url, content_type='application/json', data=req)
        self.assertEqual(resp.status_code, 200)
        self.projsam1.refresh_from_db()
        self.assertEqual(self.projsam1.samplematerial_set.count(), 2)
        self.assertEqual(self.projsam1.samplespecies_set.count(), 1)
        self.assertEqual(self.projsam1.quantchannelsample_set.filter(dataset=self.ds, channel=self.qtch).count(), 1)
        self.assertEqual(self.projsam1.quantchannelsample_set.filter(dataset=self.ds, channel=qtch2).count(), 1)
        self.assertFalse(hasattr(self.projsam1, 'quantsamplefile'))


class MergeProjectsTest(BaseTest):
    url = '/datasets/merge/projects/'

    def setUp(self):
        super().setUp()
        # make projects
        self.p2 = dm.Project.objects.create(name='p2', pi=self.pi, ptype=self.ptype)
        self.exp2 = dm.Experiment.objects.create(name='e2', project=self.p2)

    def test_merge_fails(self):
        # GET req
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # No data
        resp = self.cl.post(self.url, content_type='application/json',
                data={})
        self.assertEqual(resp.status_code, 400)
        # Only one project
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [1]})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(dm.ProjectLog.objects.exists())
       
    def test_no_ownership_fail(self):
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp2)
        ds = dm.Dataset.objects.create(date=self.p2.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(dm.ProjectLog.objects.exists())
        
    def test_dataset_exp_run_collision_fail(self):
        """When datasets of different projects have identical experiment and run names,
        we cannot merge projects"""
        exp3 = dm.Experiment.objects.create(name='e1', project=self.p2)
        run3 = dm.RunName.objects.create(name=self.run1.name, experiment=exp3)
        ds3 = dm.Dataset.objects.create(date=self.p2.registered, runname=run3,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dss3 = dm.DatasetServer.objects.create(dataset=ds3, storage_loc='testloc3',
                storage_loc_ui='testloc3', storageshare=self.ssnewstore, startdate=timezone.now())
        dm.DatasetOwner.objects.create(dataset=ds3, user=self.user)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp.status_code, 500)
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_merge_diff_exps(self):
        """
        assert ds1/ds2 under same project
        assert storage loc has been changed
        """
        run2 = dm.RunName.objects.create(name='run2', experiment=self.exp2)
        oldstorloc = 'testloc2'
        newstorloc = os.path.join(self.p1.name, self.exp2.name, self.dtype.name, run2.name)
        ds2 = dm.Dataset.objects.create(date=self.p2.registered, runname=run2,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dss2 = dm.DatasetServer.objects.create(dataset=ds2, storage_loc=oldstorloc,
                storage_loc_ui=oldstorloc, storageshare=self.ssnewstore, startdate=timezone.now())
        dm.DatasetOwner.objects.create(dataset=ds2, user=self.user)
        raw = rm.RawFile.objects.create(name='testmerge', producer=self.prod,
                source_md5='testmerge_fake', size=1234, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        dm.DatasetRawFile.objects.create(dataset=ds2, rawfile=raw)
        sf = rm.StoredFile.objects.create(rawfile=raw, filename=raw.name, md5=raw.source_md5,
                filetype=self.ft, checked=True)
        sfl = rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssnewstore, path=oldstorloc,
                purged=False, active=True)

        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp.status_code, 200)
        # Press merge button twice
        resp2 = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp2.status_code, 400)
        ds2.refresh_from_db()
        dss2.refresh_from_db()
        self.assertEqual(ds2.runname.experiment.project, self.ds.runname.experiment.project)
        self.assertEqual(dss2.storage_loc_ui, newstorloc)
        renamejobs = jm.Job.objects.filter(funcname='rename_dset_storage_loc') 
        ds2jobs = renamejobs.filter(kwargs={'dss_id': dss2.pk, 'sfloc_ids': [sfl.pk],
            'newpath': newstorloc})
        self.assertEqual(ds2jobs.count(), 1)
        self.assertEqual(renamejobs.count(), 1)
        self.assertEqual(dm.Project.objects.filter(pk=self.p2.pk).count(), 0)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} merged in '
            f'project {self.p2.pk}', project=self.p1).exists())

    def test_merge_identical_expnames(self):
        """
        assert ds1/ds2 under same project
        assert storage loc has been changed
        assert old exp has been deleted
        """
        exp3 = dm.Experiment.objects.create(name=self.exp1.name, project=self.p2)
        run3 = dm.RunName.objects.create(name='run3', experiment=exp3)
        oldstorloc = 'testloc3'
        newstorloc = os.path.join(self.p1.name, self.exp1.name, self.dtype.name, run3.name)
        ds3 = dm.Dataset.objects.create(date=self.p2.registered, runname=run3,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dss3 = dm.DatasetServer.objects.create(dataset=ds3, storage_loc=oldstorloc,
                storage_loc_ui=oldstorloc, storageshare=self.ssnewstore, startdate=timezone.now())
        own3 = dm.DatasetOwner.objects.create(dataset=ds3, user=self.user)
        raw = rm.RawFile.objects.create(name='testmerge', producer=self.prod,
                source_md5='testmerge_fake', size=1234, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        dm.DatasetRawFile.objects.create(dataset=ds3, rawfile=raw)
        sf = rm.StoredFile.objects.create(rawfile=raw, filename=raw.name, md5=raw.source_md5,
                filetype=self.ft, checked=True)
        sfl = rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssnewstore, path=oldstorloc,
                purged=False, active=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        ds3.refresh_from_db()
        dss3.refresh_from_db()
        self.assertEqual(ds3.runname.experiment, self.ds.runname.experiment)
        self.assertEqual(dm.Experiment.objects.filter(pk=exp3.pk).count(), 0)
        self.assertEqual(dss3.storage_loc_ui, newstorloc)
        renamejobs = jm.Job.objects.filter(funcname='rename_dset_storage_loc') 
        ds3jobs = renamejobs.filter(kwargs={'dss_id': dss3.pk, 'sfloc_ids': [sfl.pk],
            'newpath': newstorloc})
        self.assertEqual(ds3jobs.count(), 1)
        self.assertEqual(renamejobs.count(), 1)
        self.assertEqual(dm.Project.objects.filter(pk=self.p2.pk).count(), 0)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} merged in '
            f'project {self.p2.pk}', project=self.p1).exists())


class TestDeleteDataset(ProcessJobTest):
    jobclass = dj.RemoveDatasetFilesFromServershare

    def test_files(self):
        # Delete both raw and mzML file, pretend they are files
        self.ft.is_folder = False
        self.ft.save()
        kwargs = {'dss_id': self.ds.pk, 'sfloc_ids': [self.f3sss.pk, self.f3mzsss.pk]}
        queue = f'{self.storagecontroller.name}__{settings.QUEUE_FASTSTORAGE}'
        self.job.process(**kwargs)
        exp_t = [
                ((self.newstorctrl.path, self.f3sss.path, self.f3sf.filename,
                    self.f3sss.pk, self.f3sf.filetype.is_folder), queue),
                ((self.newstorctrl.path, self.f3mzsss.path, self.f3sfmz.filename,
                    self.f3mzsss.pk, self.f3sfmz.filetype.is_folder), queue)
                ]
        self.check(exp_t)

    def test_is_dir(self):
        # Delete both raw and mzML file, where raw is a folder
        kwargs = {'dss_id': self.ds.pk, 'sfloc_ids': [self.f3sss.pk, self.f3mzsss.pk]}
        queue = f'{self.storagecontroller.name}__{settings.QUEUE_FASTSTORAGE}'
        self.job.process(**kwargs)
        exp_t = [
                ((self.newstorctrl.path, self.f3sss.path, self.f3sf.filename,
                    self.f3sss.pk, self.f3sf.filetype.is_folder), queue),
                ((self.newstorctrl.path, self.f3mzsss.path, self.f3sfmz.filename,
                    self.f3mzsss.pk, False), queue)
                ]
        self.check(exp_t)


class TestUnlockDataset(BaseTest):
    url = '/datasets/save/dataset/unlock/'

    def setUp(self):
        super().setUp()
        self.ds.locked = True
        self.ds.save()
        ana = am.Analysis.objects.create(user=self.user, name='testana_unlock', storage_dir='testdir_unclock')
        self.dsa = am.DatasetAnalysis.objects.create(analysis=ana, dataset=self.ds)
        self.anajob = jm.Job.objects.create(funcname='testjob', kwargs={},
                state=Jobstates.WAITING, timestamp=timezone.now())
        nfw = am.NextflowWorkflowRepo.objects.create(description='a wf', repo='gh/wf')
        pset = am.ParameterSet.objects.create(name='ps1')
        nfwf = am.NextflowWfVersionParamset.objects.create(update='an update', commit='abc123',
           filename='main.nf', profiles=[], nfworkflow=nfw, paramset=pset, nfversion='22', active=True)
        wftype = am.UserWorkflow.WFTypeChoices.STD
        wf = am.UserWorkflow.objects.create(name='testwf', wftype=wftype, public=True)
        wf.nfwfversionparamsets.add(nfwf)
        nfs = am.NextflowSearch.objects.create(analysis=ana, nfwfversionparamset=nfwf,
                workflow=wf, token='tok123', job=self.anajob)

    def test_fail(self):
        # Wrong id
        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': False})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'Can not find dataset to unlock')

        # Wrong user
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, locked=True, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'You are not authorized to edit this dataset')

        # In a running analysis
        self.anajob.state = Jobstates.PROCESSING
        self.anajob.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'],
                'Dataset is currently in an analysis and you cannot edit it')

        # Already unlocked
        self.ds.locked = False
        self.ds.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'Dataset already unlocked')


    def test_has_ana_wait(self):
        # Analysis in WAIT
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'],
                f'WARNING - Dataset {self.ds.pk} has one or more analyses that are '
            'not yet run. Unlocking and editing can potentially change the analyses')
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} unlocked dataset {self.ds.pk}')

    def test_has_ana_done(self):
        # Analysis DONE
        self.anajob.state = Jobstates.DONE
        self.anajob.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'],
                f'WARNING - Dataset {self.ds.pk} has one or more analyses that are '
            'already finished. Editing the dataset or adding/removing files can distort the view '
            'of the data that analysis has resulted in')
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} unlocked dataset {self.ds.pk}')

    def test_has_no_ana(self):
        self.dsa.delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'Dataset {self.ds.pk} unlocked')
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} unlocked dataset {self.ds.pk}')


class TestLockDataset(BaseTest):
    url = '/datasets/save/dataset/lock/'

    def test_fail(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': False})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'Can not find dataset to lock')

        # Wrong user
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'You are not authorized to edit this dataset')

        self.ds.locked = True
        self.ds.save()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'You cannot edit a locked dataset')

    def test_ok(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} locked dataset {self.ds.pk}')


class TestArchiveDataset(BaseTest):
    url = '/datasets/archive/dataset/'

    def test_fail(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': False})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'Dataset does not exist')

        # Wrong user
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'Cannot archive dataset, no permission for user')

    def test_ok(self):
        self.user.is_staff = True
        self.user.save()
        self.ds.deleted = True
        self.ds.save()
        dsfiles = rm.StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset=self.ds)
        dsfiles.filter(sfile__mzmlfile__isnull=False).delete()
        dsfiles.update(active=False)
        rm.PDCBackedupFile.objects.create(storedfile=self.f3sf, pdcpath=self.f3sf.md5, success=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.ds.pk})
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.json()['error'], 'Cannot archive dataset, it is already in deleted state')

        self.ds.deleted = False
        self.ds.save()
        rm.PDCBackedupFile.objects.all().delete()
        dsfiles.update(active=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} deactivated dataset {self.ds.pk}')



class TestReactivateDataset(BaseTest):
    url = '/datasets/undelete/dataset/'

    def test_fail(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': False})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'Dataset does not exist')

        # Wrong user
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json', data={'dataset_id': ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'Cannot reactivate dataset, no permission for user')

        # Already active
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk, 'storage_shares': [self.ssnewstore.pk]})
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.json()['error'], 'Dataset already in active storage')

    def test_ok(self):
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=self.tmpraw)
        self.ds.deleted = True
        self.ds.save()
        self.dss.active = False
        self.dss.save()
        dsfiles = rm.StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset=self.ds)
        dsfiles.filter(sfile__mzmlfile__isnull=False).delete()
        dsfiles.update(active=False, purged=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk, 'storage_shares': [self.ssnewstore.pk]})
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.json()['error'], 'Cannot reactivate purged dataset')

        rm.PDCBackedupFile.objects.create(storedfile=self.f3sf, pdcpath=self.f3sf.md5, success=True)
        rm.PDCBackedupFile.objects.create(storedfile=self.tmpsf, pdcpath=self.tmpsf.md5, success=True)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk, 'storage_shares': [self.ssnewstore.pk]})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} reactivated dataset {self.ds.pk}')

        jobs = jm.Job.objects.all()
        self.assertEqual(jobs.count(), 3)
        jobs = jm.Job.objects.all().iterator()
        bupjob = next(jobs)
        self.assertEqual(bupjob.funcname, 'reactivate_dataset')
        bupdss = dm.DatasetServer.objects.get(dataset=self.ds, storageshare=self.ssinbox)
        self.assertEqual(bupjob.kwargs['dss_id'], bupdss.pk)
        bupsfl = [self.f3sssinbox.pk, self.tmpsss.pk]
        self.assertCountEqual(bupjob.kwargs['sfloc_ids'], bupsfl)

        rsjob = next(jobs)
        self.assertEqual(rsjob.funcname, 'rsync_dset_files_to_servershare')
        self.assertEqual(rsjob.kwargs['dss_id'], self.dss.pk)
        self.assertCountEqual(rsjob.kwargs['sfloc_ids'], bupsfl)
        dstsfls = [x['pk'] for x in dsfiles.filter(servershare=self.ssnewstore).values('pk')]
        self.assertEqual(sorted(rsjob.kwargs['dstsfloc_ids']), sorted(dstsfls))

        rmjob = next(jobs)
        self.assertEqual(rmjob.funcname, 'remove_dset_files_servershare')
        dssinbox = dm.DatasetServer.objects.get(dataset=self.ds, storageshare=self.ssinbox)
        self.assertEqual(rmjob.kwargs['dss_id'], dssinbox.pk)
        self.assertCountEqual(rmjob.kwargs['sfloc_ids'], bupsfl)

        self.ds.deleted = True
        self.ds.save()
        self.dss.active = False
        self.dss.save()
        self.tmpsss.active = False
        self.tmpsss.save()
        rm.PDCBackedupFile.objects.filter(storedfile=self.f3sf).delete()
        resp = self.cl.post(self.url, content_type='application/json',
                data={'dataset_id': self.ds.pk, 'storage_shares': [self.ssnewstore.pk]})
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.json()['error'], 'Cannot reactivate dataset, files missing in backup storage')


class TestArchiveProject(BaseTest):
    url = '/datasets/archive/project/'

    def test_fail(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': False})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'No project specified for closing')

        # Wrong user
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.p1.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json()['error'], 'User has no permission to retire this project, does not own all datasets in project')

    def test_ok(self):
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.p1.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.ProjectLog.objects.last().message,
                f'User {self.user.pk} closed project')


# Currently disabled
#class TestReactivateProject(BaseTest):
#    url = '/datasets/undelete/project/'
#
#    def setUp(self):
#        super().setUp()
#        self.p1.active = False
#        self.p1.save()
#
#    def test_fail(self):
#        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': False})
#        self.assertEqual(resp.status_code, 400)
#        self.assertEqual(resp.json()['error'], 'No project specified for reactivating')
#
#        # Wrong user
#        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
#        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
#                datatype=self.dtype, securityclass=max(rm.DataSecurityClass))
#        dm.DatasetServer.objects.create(dataset=ds, storage_loc='test',
#                storage_loc_ui='test', storageshare=self.ssnewstore, startdate=timezone.now())
#        otheruser = User.objects.create(username='test', password='test')
#        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
#        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.p1.pk})
#        self.assertEqual(resp.status_code, 403)
#        self.assertEqual(resp.json()['error'], 'User has no permission to reactivate this project, does not own all datasets in project')
#
#    def test_ok(self):
#        dsfiles = rm.StoredFileLoc.objects.filter(sfile__rawfile__datasetrawfile__dataset__runname__experiment__project=self.p1)
#        dsfiles.update(active=False)
#        resp = self.cl.post(self.url, content_type='application/json',
#                data={'item_id': self.p1.pk})
#        self.assertEqual(resp.status_code, 500)
#        self.assertEqual(resp.json()['error'], 'Not all project datasets could be reactivated. '
#                f'Problem with dataset {self.ds.pk}: Cannot reactivate purged dataset')
#
#        rm.PDCBackedupFile.objects.bulk_create([rm.PDCBackedupFile(storedfile=sfl.sfile,
#            pdcpath=sfl.sfile.md5, success=True)
#            for sfl in dsfiles.filter(sfile__mzmlfile__isnull=True)])
#        resp = self.cl.post(self.url, content_type='application/json',
#                data={'item_id': self.p1.pk})
#        self.assertEqual(resp.status_code, 200)
#        self.assertEqual(dm.ProjectLog.objects.last().message,
#                f'User {self.user.pk} reopened project')


class TestChangeDatasetOwner(BaseTest):
    url = '/datasets/save/owner/'
