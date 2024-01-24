import os
import json
from time import sleep
from datetime import datetime
from django.utils import timezone
from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.core.management import call_command

from kantele import settings
from kantele.tests import BaseTest, BaseIntegrationTest
from datasets import models as dm
from jobs import models as jm
from jobs.jobs import Jobstates
from rawstatus import models as rm


class UpdateDatasetTest(BaseIntegrationTest):
    url = '/datasets/save/dataset/'

    def test_update_dset_newexp_location(self):
        newexpname = 'edited_exp'
        self.assertEqual(dm.Experiment.objects.filter(name=newexpname).count(), 0)
        resp = self.post_json(data={'dataset_id': self.ds.pk, 'project_id': self.p1.pk,
            'newexperimentname': newexpname, 'runname': self.run1.name,
            'datatype_id': self.dtype.pk, 'prefrac_id': False, 'ptype_id': self.ptype.pk,
            'externalcontact': self.contact.email})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(dm.Experiment.objects.filter(name=newexpname).count(), 1)
        self.assertTrue(os.path.exists(self.f3path))
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        call_command('runjobs')
        sleep(3)
        self.assertFalse(os.path.exists(self.f3path))
        new_ds_loc = os.path.join(self.p1.name, newexpname, self.dtype.name, self.run1.name)
        self.assertNotEqual(self.ds.storage_loc, new_ds_loc)
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
            'datatype_id': self.dtype.pk, 'prefrac_id': False, 'ptype_id': self.ptype.pk, 
            'externalcontact': self.contact.email})
        self.assertEqual(mvdsresp.status_code, 200)
        rename_job = jm.Job.objects.filter(funcname='rename_dset_storage_loc').last()
        self.assertEqual(rename_job.state, Jobstates.PENDING)
        # remove files results in a job and claimed files still on tmp
        rmresp = self.cl.post('/datasets/save/files/', content_type='application/json', data={
            'dataset_id': self.ds.pk, 'removed_files': {self.f3raw.pk: {'id': self.f3raw.pk}},
            'added_files': {}})
        self.assertEqual(rmresp.status_code, 200)
        self.assertTrue(self.f3raw.claimed)
        self.assertEqual(self.f3sf.servershare, self.ssnewstore)
        self.assertEqual(self.f3sf.path, self.ds.storage_loc)
        self.f3raw.refresh_from_db()
        self.assertFalse(self.f3raw.claimed)
        # execute dataset move on disk, should also move the removed files and update their DB
        # the move job should be in waiting state still
        call_command('runjobs')
        sleep(3)
        mvjob = jm.Job.objects.filter(funcname='move_stored_files_tmp').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.ds.refresh_from_db()
        self.f3sf.refresh_from_db()
        self.assertEqual(self.f3sf.servershare, self.ssnewstore)
        self.assertEqual(self.f3sf.path, self.ds.storage_loc)
        newf3sf_path = os.path.join(settings.SHAREMAP[self.sstmp.name], self.f3sf.filename)
        self.assertFalse(os.path.exists(newf3sf_path))
        # call twice to execute tmp-move files on disk, first call only resolves rename dset job
        call_command('runjobs')
        rename_job.refresh_from_db()
        self.assertEqual(rename_job.state, Jobstates.DONE)
        call_command('runjobs')
        sleep(3)
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
            'datatype_id': self.dtype.pk, 'prefrac_id': False, 'ptype_id': self.ptype.pk,
            'externalcontact': self.contact.email})
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
        call_command('runjobs')
        sleep(3)
        mvjob = jm.Job.objects.filter(funcname='move_files_storage').last()
        self.assertEqual(mvjob.state, Jobstates.PENDING)
        self.ds.refresh_from_db()
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        newtmpsf_path = os.path.join(settings.SHAREMAP[self.ssnewstore.name], self.ds.storage_loc,
                self.tmpsf.filename)
        self.assertFalse(os.path.exists(newtmpsf_path))
        # call twice to execute add files on disk, first call only resolves rename dset job
        call_command('runjobs')
        call_command('runjobs')
        sleep(3)
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # tmp file should now exist in dset folder
        self.tmpsf.refresh_from_db()
        self.assertTrue(os.path.exists(newtmpsf_path))
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)

        # clean up
        newdsr.delete()


class UpdateFilesTest(BaseIntegrationTest):
    url = '/datasets/save/files/'

    def test_add_files(self):
        '''Add files, check if added, also check if the job waits for another job on the dataset'''
        resp = self.post_json({'dataset_id': self.ds.pk, 'added_files': {self.tmpraw.pk: {'id': self.tmpraw.pk}},
            'removed_files': {}})
        self.assertEqual(resp.status_code, 200)
        newdsr = dm.DatasetRawFile.objects.filter(dataset=self.ds, rawfile=self.tmpraw)
        self.assertEqual(newdsr.count(), 1)
        self.assertFalse(self.tmpraw.claimed)
        self.assertEqual(self.tmpsf.servershare, self.sstmp)
        self.assertEqual(self.tmpsf.path, '')
        self.tmpraw.refresh_from_db()
        self.assertTrue(self.tmpraw.claimed)
        call_command('runjobs')
        sleep(3)
        self.tmpsf.refresh_from_db()
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ssnewstore.name], 
            self.ds.storage_loc, self.tmpsf.filename)))

        # clean up
        newdsr.delete()


class RenameProjectTest(BaseIntegrationTest):
    url = '/datasets/rename/project/'

    def test_no_ownership_fail(self):
        ####
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp1)
        ds = dm.Dataset.objects.create(date=self.p1.registered, runname=run,
                datatype=self.dtype, storage_loc='test', storageshare=self.ssnewstore)
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
        # existing proj name? proj name identical to old projname

    def test_rename_ok(self):
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
        self.assertTrue(os.path.exists(self.f3path))
        old_loc = self.ds.storage_loc
        call_command('runjobs')
        sleep(3)
        self.assertFalse(os.path.exists(self.f3path))
        new_loc = os.path.join(newname, self.exp1.name, self.dtype.name, self.run1.name)
        self.assertEqual(self.ds.storage_loc, old_loc)
        self.ds.refresh_from_db()
        self.assertEqual(self.ds.storage_loc, new_loc)
        self.assertTrue(os.path.exists(os.path.join(settings.SHAREMAP[self.ds.storageshare.name],
            self.ds.storage_loc, self.f3sf.filename)))

    def test_if_added_removed_files_ok(self):
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
        call_command('runjobs')
        sleep(3)
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
        # call twice to execute add files on disk, first call only resolves rename dset job
        call_command('runjobs')
        call_command('runjobs')
        sleep(3)
        mvjob.refresh_from_db()
        self.assertEqual(mvjob.state, Jobstates.PROCESSING)
        # tmp file should now exist in dset folder
        self.tmpsf.refresh_from_db()
        self.assertTrue(os.path.exists(newtmpsf_path))
        self.assertEqual(self.tmpsf.path, self.ds.storage_loc)
        self.assertEqual(self.tmpsf.servershare, self.ssnewstore)

        # clean up
        newdsr.delete()


class SaveSamples(BaseTest):
    url = '/datasets/save/samples/'

    def test_fails(self):
        newrun = dm.RunName.objects.create(name='failrun', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore)
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
                {'error': 'Samples and species need to be specified for all files'})

    def test_save_new_samples_multiplex(self):
        newrun = dm.RunName.objects.create(name='newds_nosamples_plex', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore)
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
        newrun = dm.RunName.objects.create(name='newds_nosamples_fns', experiment=self.exp1)
        newds = dm.Dataset.objects.create(date=self.p1.registered, runname=newrun,
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore)
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        dm.DatasetComponentState.objects.get_or_create(dataset=newds,
                defaults={'state': dm.DCStates.NEW, 'dtcomp': self.dtcompsamples})

        # Add file to dset
        fn = 'raw_lf_dset'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod,
                source_md5='rawlf_ds_fakemd5', size=2024, date=timezone.now(),
                claimed=True)
        dsr, _ = dm.DatasetRawFile.objects.get_or_create(dataset=newds, rawfile=raw)

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
        # New sample info (organism, type) on existing multiplex dset, 
        # updates since sample is NOT in use in another dataset
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
                datatype=self.dtype, storage_loc=newrun.name, storageshare=self.ssnewstore)
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

    def test_update_samples_files_newsample(self):
        # Change proj sample
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


class MergeProjectsTest(BaseTest):
    url = '/datasets/merge/projects/'

    def setUp(self):
        super().setUp()
        # make projects
        self.p2 = dm.Project.objects.create(name='p2', pi=self.pi)
        pt2 = dm.ProjType.objects.create(project=self.p2, ptype=self.ptype)
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
       
    def test_no_ownership_fail(self):
        run = dm.RunName.objects.create(name='someoneelsesrun', experiment=self.exp2)
        ds = dm.Dataset.objects.create(date=self.p2.registered, runname=run,
                datatype=self.dtype, storage_loc='test', storageshare=self.ssnewstore)
        otheruser = User.objects.create(username='test', password='test')
        dm.DatasetOwner.objects.create(dataset=ds, user=otheruser)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp.status_code, 403)
        
    def test_dataset_exp_run_collision_fail(self):
        """When datasets of different projects have identical experiment and run names,
        we cannot merge projects"""
        exp3 = dm.Experiment.objects.create(name='e1', project=self.p2)
        run3 = dm.RunName.objects.create(name=self.run1.name, experiment=exp3)
        ds3 = dm.Dataset.objects.create(date=self.p2.registered, runname=run3,
                datatype=self.dtype, storage_loc='testloc3', storageshare=self.ssnewstore)
        dm.DatasetOwner.objects.create(dataset=ds3, user=self.user)
        resp = self.cl.post(self.url, content_type='application/json',
                data={'projids': [self.p1.pk, self.p2.pk]})
        self.assertEqual(resp.status_code, 500)

    def test_merge_diff_exps(self):
        """
        assert ds1/ds2 under same project
        assert storage loc has been changed
        """
        run2 = dm.RunName.objects.create(name='run2', experiment=self.exp2)
        oldstorloc = 'testloc2'
        ds2 = dm.Dataset.objects.create(date=self.p2.registered, runname=run2,
                datatype=self.dtype, storage_loc=oldstorloc, storageshare=self.ssnewstore)
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
                datatype=self.dtype, storage_loc=oldstorloc, storageshare=self.ssnewstore)
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
