import os
import json
from datetime import datetime
from django.utils import timezone
from django.test import TestCase, Client
from django.contrib.auth.models import User

from kantele import settings
from kantele.tests import BaseTest, BaseIntegrationTest, ProcessJobTest
from datasets import models as dm
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
            'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.RunName.objects.filter(name='newrunname').count(), 1)
        ds = dm.Dataset.objects.get(runname__name='newrunname', runname__experiment=self.exp1)
        self.assertEqual(ds.datatype_id, self.dtype.pk)
        self.assertEqual(ds.storage_loc, os.path.join(self.p1.name, self.exp1.name, self.dtype.name, 'newrunname'))
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
        resp = self.post_json(data={'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name,
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.Experiment.objects.filter(name=newexpname).count(), 1)
        self.assertTrue(os.path.exists(self.f3path))
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        self.run_job()
        self.assertFalse(os.path.exists(self.f3path))
        new_ds_loc = os.path.join(self.p1.name, newexpname, self.dtype.name, self.run1.name)
        self.assertNotEqual(self.ds.storage_loc, new_ds_loc)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.pk} changed dataset '
                f'{self.ds.pk} path to {new_ds_loc} from {self.ds.storage_loc}').exists())
        self.ds.refresh_from_db()
        self.assertEqual(self.ds.storage_loc, new_ds_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ds.storageshare.name],
            self.ds.storage_loc, self.f3sf.filename)))

    def test_remove_files_wait_for_rename(self):
        '''First queue a move dataset job, to new experiment name. Then queue a remove
        files job from dataset. The second job should wait for the first one, so the removed
        file should first be moved to the new dset location'''
        newexpname = 'edited_exp1'
        # move dataset
        mvdsresp = self.post_json({'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name, 
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(mvdsresp.status_code, 200)
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        self.assertTrue(dm.ProjectLog.objects.filter(message__startswith=f'User {self.user.pk} '
            f'changed dataset {self.ds.pk} path to').exists())
        # remove files results in a job and claimed files still on tmp
        rmresp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'removed_files': {self.f3raw.pk: {'id': self.f3raw.pk}},
            'added_files': {}})
        print(dm.ProjectLog.objects.last().message)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.pk} '
            f'removed files {self.f3raw.pk} from dataset {self.ds.pk}').exists())
        self.assertEqual(rmresp.status_code, 200)
        self.assertTrue(self.f3raw.claimed)
        self.assertEqual(self.f3sf.servershare, self.ssnewstore)
        self.assertEqual(self.f3sf.path, self.ds.storage_loc)
        self.f3raw.refresh_from_db()
        self.assertFalse(self.f3raw.claimed)
        # execute dataset move on disk, should also move the removed files and update their DB
        # the move job should be in waiting state still
        self.run_job()
        mvjob = jm.Job.objects.filter(funcname='move_stored_files_tmp').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.ds.refresh_from_db()
        self.f3sf.refresh_from_db()
        self.assertEqual(self.f3sf.servershare, self.ssnewstore)
        self.assertEqual(self.f3sf.path, self.ds.storage_loc)
        newf3sf_path = os.path.join(settings.SHAREMAP[self.sstmp.name], self.f3sf.filename)
        self.assertFalse(os.path.exists(newf3sf_path))
        self.run_job()
        rename_job.refresh_from_db()
        self.assertEqual(rename_job.state, Jobstates.DONE)
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # f3 file should now exist in tmp
        self.f3sf.refresh_from_db()
        self.assertTrue(os.path.exists(newf3sf_path))
        self.assertEqual(self.f3sf.path, '')
        self.assertEqual(self.f3sf.servershare, self.sstmp)

    def test_add_files_wait_for_rename(self):
        '''Another job is running on dataset that changed the storage_loc,
        do not add new files to old storage_loc'''
        # FIXME maybe hardcode file paths instead of relying on ds.storage_path
        newexpname = 'edited_exp1'
        # move dataset
        mvdsresp = self.post_json({'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name,
            'datatype_id': self.dtype.pk, 'prefrac_id': False,
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(mvdsresp.status_code, 200)
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        # add files results in a job and claimed files still on tmp
        resp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {self.tmpraw.pk: {'id': self.tmpraw.pk}},
            'removed_files': {}})
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(self.tmpraw.claimed)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        # execute dataset move on disk, should not move the added files on tmp (nor update their DB)
        self.run_job()
        mvjob = jm.Job.objects.filter(funcname='move_files_storage').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.ds.refresh_from_db()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        newtmpsf_path = os.path.join(settings.SHAREMAP[self.ssnewstore.name], self.ds.storage_loc,
                self.tmpsf.filename)
        self.assertFalse(os.path.exists(newtmpsf_path))
        self.run_job()
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # tmp file should now exist in dset folder
        self.tmpsf.refresh_from_db()
        self.assertTrue(os.path.exists(newtmpsf_path))
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)
    

    def test_fail_storageloc_is_filename(self):
        # Create file with dset storloc path/name
        fpath, fname = os.path.join(self.p1.name, self.exp1.name, self.dtype.name), 'file_dirname'
        raw = rm.RawFile.objects.create(name=fname, producer=self.prod,
                source_md5='storloc_raw_fakemd5', size=100, date=timezone.now(), claimed=False)
        sf = rm.StoredFile.objects.create(rawfile=raw, md5=raw.source_md5, path=fpath,
                filename=raw.name, servershare=self.ssnewstore, checked=True, filetype=self.ft)
        # Try to create new dset 
        resp = self.post_json(data={'dataset_id': False, 'project_id': self.p1.pk,
            'experiment_id': self.exp1.pk, 'runname': fname, 'datatype_id': self.dtype.pk,
            'secclass': 1, 'prefrac_id': False, 'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('storage location not unique, there is either a file', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())

        # Try to update existing dataset
        dm.RunName.objects.filter(experiment=self.exp1, name=fname).delete()
        resp = self.post_json(data={'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'experiment_id': self.exp1.pk, 'runname': fname, 'datatype_id': self.dtype.pk,
            'prefrac_id': False, 'ptype_id': self.ptype.pk,
            'secclass': 1, 'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('There is already a file with that exact path', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())


class UpdateFilesTest(BaseIntegrationTest):
    url = '/datasets/save/files/'

    def test_add_files(self):
        '''Add files, check if added, also check if the job waits for another job on the dataset'''
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)
        resp = self.post_json({'dataset_id': self.ds.pk, 'added_files': {self.tmpraw.pk: {'id': self.tmpraw.pk}},
            'removed_files': {}})
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.pk} '
            f'added files {self.tmpraw.pk} to dataset {self.ds.pk}').exists())
        self.assertEqual(resp.status_code, 200)
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertFalse(self.tmpraw.claimed)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        self.run_job()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ssnewstore.name], 
            self.ds.storage_loc, self.tmpsf.filename)))
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
    
    def test_add_fails(self):
        # Fail because there is no storedfile
        fn = 'raw_no_sf'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod, claimed=False,
                source_md5='raw_no_sf_fakemd5', size=1024, date=timezone.now())
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)
        resp = self.post_json({'dataset_id': self.ds.pk, 'added_files': {raw.pk: {'id': raw.pk}},
            'removed_files': {}})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('cannot be saved to dataset', json.loads(resp.content)['error'])
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=raw)
        self.assertEqual(newdsr.count(), 0)
        self.assertFalse(raw.claimed)
        self.assertFalse(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_trigger_movejob_errors(self):
        # add files are already in dset
        dupe_raw = rm.RawFile.objects.create(name=self.f3raw.name, producer=self.prod,
                source_md5='tmpraw_dupe_fakemd5', size=100, date=timezone.now(), claimed=False)
        dupe_sf = rm.StoredFile.objects.create(rawfile=dupe_raw, md5=dupe_raw.source_md5, path='',
                filename=dupe_raw.name, servershare=self.sstmp, checked=True, filetype=self.ft)
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {dupe_raw.pk: {'id': dupe_raw.pk}},
            'removed_files': {}})
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=dupe_raw)
        self.assertEqual(newdsr.count(), 0)
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(dupe_raw.claimed)
        self.assertIn(f'Cannot move files selected to dset {self.ds.storage_loc}', resp.json()['error'])
        self.assertEqual(dupe_sf.servershare, self.sstmp)
        self.assertEqual(dupe_sf.path, '')
        self.assertFalse(dm.ProjectLog.objects.exists())

        # remove files results in a job and claimed files still on tmp
        # dupe_raw above is needed!
        resp = self.cl.post(self.url, content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {},
            'removed_files': {self.f3raw.pk: {'id': self.f3raw.pk}}})
        self.f3sf.refresh_from_db()
        dsr = dm.DatasetRawFile.objects.get(rawfile=self.f3raw, dataset=self.ds)
        self.assertEqual(dsr.pk, self.f3dsr.pk)
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(self.f3raw.claimed)
        self.assertIn(f'Cannot move files from dataset {self.ds.pk}', resp.json()['error'])
        self.assertEqual(self.f3sf.servershare, self.ds.storageshare)
        self.assertEqual(self.f3sf.path, self.ds.storage_loc)
        self.assertFalse(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_dset_is_filename_job_error(self):
        # new file is dir w same name as dset storage dir
        run = dm.RunName.objects.create(name='newrun', experiment=self.exp1)
        newpath, newfn = os.path.split(self.ds.storage_loc)
        self.tmpsf.filename = newfn
        self.tmpsf.save()
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, storageshare=self.ssnewstore, storage_loc=newpath,
                securityclass=max(dm.DatasetSecurityClass))
        dm.DatasetOwner.objects.get_or_create(dataset=newds, user=self.user)
        dtc = newds.datasetcomponentstate_set.create(dtcomp=self.dtcompfiles, state=dm.DCStates.NEW)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'dataset_id': newds.pk, 'added_files': {self.tmpraw.pk: {'id': self.tmpraw.pk}},
            'removed_files': {}})
        dsr = dm.DatasetRawFile.objects.filter(rawfile=self.tmpraw, dataset=self.ds)
        self.assertEqual(dsr.count(), 0)
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(self.tmpraw.claimed)
        self.assertIn(f'Cannot move selected files to path {newds.storage_loc}', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        dtc.refresh_from_db()
        self.assertEqual(dtc.state, dm.DCStates.NEW)


class AcceptRejectPreassocFiles(BaseIntegrationTest):
    url = '/datasets/save/files/pending/'

    def setUp(self):
        super().setUp()
        self.tmpraw.claimed = True
        self.tmpraw.save()
        jm.Job.objects.create(funcname='move_files_storage', state=Jobstates.HOLD, kwargs={
            'dset_id': self.ds.pk, 'rawfn_ids': [self.tmpraw.pk]}, timestamp=timezone.now())
        self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles).update(state=dm.DCStates.NEW)

    def test_accept_all_files(self):
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        resp = self.post_json({'dataset_id': self.ds.pk, 'accepted_files': [self.tmpraw.pk],
            'rejected_files': []})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} accepted files '
                f'{self.tmpraw.pk}, to dset {self.ds.pk}').exists())
        self.assertEqual(newdsr.count(), 1)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        self.run_job()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ssnewstore.name], 
            self.ds.storage_loc, self.tmpsf.filename)))
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())
    
    def test_reject_all_files(self):
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        resp = self.post_json({'dataset_id': self.ds.pk, 'rejected_files': [self.tmpraw.pk],
            'accepted_files': []})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(dm.ProjectLog.objects.exists())
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.tmpraw.refresh_from_db()
        self.assertFalse(self.tmpraw.claimed)
        self.run_job()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.sstmp.name],
            self.tmpsf.filename)))
        self.assertFalse(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())

    def test_accept_some_files(self):
        rejectraw = rm.RawFile.objects.create(name='reject.raw', producer=self.prod,
                source_md5='rejectit_fakemd5', size=123, date=timezone.now(),
                claimed=True)
        jm.Job.objects.create(funcname='move_files_storage', state=Jobstates.HOLD, kwargs={
            'dset_id': self.ds.pk, 'rawfn_ids': [rejectraw.pk]}, timestamp=timezone.now())
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 0)
        resp = self.post_json({'dataset_id': self.ds.pk, 'accepted_files': [self.tmpraw.pk],
            'rejected_files': [rejectraw.pk]})
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} accepted files '
                f'{self.tmpraw.pk}, to dset {self.ds.pk}').exists())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(newdsr.count(), 1)
        rejectdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=rejectraw)
        self.assertEqual(rejectdsr.count(), 0)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        self.run_job()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ssnewstore.name], 
            self.ds.storage_loc, self.tmpsf.filename)))
        self.assertTrue(self.ds.datasetcomponentstate_set.filter(dtcomp=self.dtcompfiles,
            state=dm.DCStates.OK).exists())


class RenameProjectTest(BaseIntegrationTest):
    url = '/datasets/rename/project/'

    def test_no_ownership_fail(self):
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, storage_loc='test', storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
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
        self.assertIn('There is already a project by that name', resp.json()['error'])
        self.assertFalse(dm.ProjectLog.objects.exists())

    def test_rename_ok(self):
        newname = 'testnewname'
        self.assertEqual(dm.Project.objects.filter(name=newname).count(), 0)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': newname})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} renamed project '
            f'from {self.p1.name} to {newname}').exists())
        renamejobs = jm.Job.objects.filter(funcname='rename_top_lvl_projectdir',
                kwargs={'proj_id': self.p1.pk, 'newname': newname}) 
        self.assertEqual(renamejobs.count(), 1)
        self.p1.refresh_from_db()
        self.assertEqual(self.p1.name, newname)
        self.assertTrue(os.path.exists(self.f3path))
        old_loc = self.ds.storage_loc
        self.run_job()
        self.assertFalse(os.path.exists(self.f3path))
        new_loc = os.path.join(newname, self.exp1.name, self.dtype.name, self.run1.name)
        self.assertEqual(self.ds.storage_loc, old_loc)
        self.ds.refresh_from_db()
        self.assertEqual(self.ds.storage_loc, new_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ds.storageshare.name],
            self.ds.storage_loc, self.f3sf.filename)))

    def test_if_added_removed_files_ok(self):
        # first rename job http request, then add files, then jobs run:
        newname = 'testnewname'
        self.assertEqual(dm.Project.objects.filter(name=newname).count(), 0)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projid': self.p1.pk, 'newname': newname})
        self.assertEqual(resp.status_code, 200)
        renamejobs = jm.Job.objects.filter(funcname='rename_top_lvl_projectdir',
                kwargs={'proj_id': self.p1.pk, 'newname': newname}) 
        self.assertEqual(renamejobs.count(), 1)
        self.p1.refresh_from_db()
        self.assertEqual(self.p1.name, newname)
        # add files results in a job and claimed files still on tmp
        mvresp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'added_files': {self.tmpraw.pk: {'id': self.tmpraw.pk}},
            'removed_files': {}})
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertEqual(mvresp.status_code, 200)
        self.assertFalse(self.tmpraw.claimed)
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        # Now test rename project job
        self.assertTrue(os.path.exists(self.f3path))
        old_loc = self.ds.storage_loc
        self.run_job()
        self.assertFalse(os.path.exists(self.f3path))
        new_loc = os.path.join(newname, self.exp1.name, self.dtype.name, self.run1.name)
        self.assertEqual(self.ds.storage_loc, old_loc)
        self.ds.refresh_from_db()
        self.assertEqual(self.ds.storage_loc, new_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ds.storageshare.name],
            self.ds.storage_loc, self.f3sf.filename)))
        # Check if added files are not there yet, being waited
        mvjob = jm.Job.objects.filter(funcname='move_files_storage').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.ds.refresh_from_db()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        newtmpsf_path = os.path.join(settings.SHAREMAP[self.ssnewstore.name], self.ds.storage_loc,
                self.tmpsf.filename)
        self.assertFalse(os.path.exists(newtmpsf_path))
        self.run_job()
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # tmp file should now exist in dset folder
        self.tmpsf.refresh_from_db()
        self.assertTrue(os.path.exists(newtmpsf_path))
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)


# FIXME write tests
#class SaveAcquisition(BaseTest):
#class SaveSampleprep(BaseTest):


class SaveSamples(BaseTest):
    url = '/datasets/save/samples/'

    def test_fails(self):
        newrun = dm.RunName.objects.create(name='failrun', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
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
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
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
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        dm.DatasetComponentState.objects.get_or_create(dataset=newds,
                defaults={'state': dm.DCStates.NEW, 'dtcomp': self.dtcompsamples})

        # Add file to dset
        fn = 'raw_lf_dset'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod,
                source_md5='rawlf_ds_fakemd5', size=2024, date=timezone.now(),
                claimed=True)
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
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
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
                datatype=self.dtype, storage_loc='test', storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
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
                datatype=self.dtype, storage_loc='testloc3', storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
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
        ds2 = dm.Dataset.objects.create(date=self.p2.registered, runname=run2,
                datatype=self.dtype, storage_loc=oldstorloc, storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
        dm.DatasetOwner.objects.create(dataset=ds2, user=self.user)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp.status_code, 200)
        # Press merge button twice
        resp2 = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp2.status_code, 400)
        
        oldstorloc = ds2.storage_loc
        ds2.refresh_from_db()
        self.assertEqual(ds2.runname.experiment.project, self.ds.runname.experiment.project)
        self.assertEqual(ds2.storage_loc, oldstorloc)
        renamejobs = jm.Job.objects.filter(funcname='rename_dset_storage_loc') 
        ds2jobs = renamejobs.filter(kwargs={'dset_id': ds2.pk,
            'dstpath': os.path.join(self.p1.name, self.exp2.name, self.dtype.name, run2.name)})
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
        ds3 = dm.Dataset.objects.create(date=self.p2.registered, runname=run3,
                datatype=self.dtype, storage_loc=oldstorloc, storageshare=self.ssnewstore,
                securityclass=max(dm.DatasetSecurityClass))
        own3 = dm.DatasetOwner.objects.create(dataset=ds3, user=self.user)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        oldstorloc = ds3.storage_loc
        ds3.refresh_from_db()
        self.assertEqual(ds3.runname.experiment, self.ds.runname.experiment)
        self.assertEqual(dm.Experiment.objects.filter(pk=exp3.pk).count(), 0)
        self.assertEqual(ds3.storage_loc, oldstorloc)
        renamejobs = jm.Job.objects.filter(funcname='rename_dset_storage_loc') 
        ds3jobs = renamejobs.filter(kwargs={'dset_id': ds3.pk, 
            'dstpath': os.path.join(self.p1.name, self.exp1.name, self.dtype.name, run3.name)})
        self.assertEqual(ds3jobs.count(), 1)
        self.assertEqual(renamejobs.count(), 1)
        self.assertEqual(dm.Project.objects.filter(pk=self.p2.pk).count(), 0)
        self.assertTrue(dm.ProjectLog.objects.filter(message=f'User {self.user.id} merged in '
            f'project {self.p2.pk}', project=self.p1).exists())


class TestDeleteDataset(ProcessJobTest):
    jobclass = dj.DeleteActiveDataset

    def test_files(self):
        # Delete both raw and mzML file, pretend they are files
        self.ft.is_folder = False
        self.ft.save()
        kwargs = {'dset_id': self.ds.pk}
        self.job.process(**kwargs)
        exp_t = [
                ((self.f3sf.servershare.name, os.path.join(self.f3sf.path, self.f3sf.filename),
                    self.f3sf.pk, self.f3sf.filetype.is_folder), {}),
                ((self.f3sfmz.servershare.name, os.path.join(self.f3sfmz.path, self.f3sfmz.filename),
                    self.f3sfmz.pk, self.f3sfmz.filetype.is_folder), {})
                ]
        self.check(exp_t)

    def test_is_dir(self):
        # Delete both raw and mzML file, where raw is a folder
        kwargs = {'dset_id': self.ds.pk}
        self.job.process(**kwargs)
        exp_t = [
                ((self.f3sf.servershare.name, os.path.join(self.f3sf.path, self.f3sf.filename),
                    self.f3sf.pk, self.f3sf.filetype.is_folder), {}),
                ((self.f3sfmz.servershare.name, os.path.join(self.f3sfmz.path, self.f3sfmz.filename),
                    self.f3sfmz.pk, False), {})
                ]
        self.check(exp_t)
