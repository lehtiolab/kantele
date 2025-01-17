import os

from django.utils import timezone
from kantele.tests import BaseTest
from kantele import settings
from rawstatus import models as rm
from datasets import models as dm
from jobs import models as jm
from analysis import models as am
from dashboard import models as dashm


class RerunSingleQCTest(BaseTest):
    url = '/manage/qc/rerunsingle/'

    def setUp(self):
        super().setUp()
        self.user.is_staff = True
        self.user.save()
        dm.Operator.objects.create(user=self.user)
        self.bup_jobs = jm.Job.objects.filter(funcname='restore_from_pdc_archive',
                kwargs__sf_id=self.oldsf.pk)
        self.qc_jobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow',
                kwargs__sf_id=self.oldsf.pk)
        ana = am.Analysis.objects.create(name='previousrun', user=self.user, storage_dir='blbala')
        self.qcdata = dashm.QCData.objects.create(rawfile=self.oldraw, analysis=ana,
                runtype=dm.AcquisistionMode.DDA)
        self.oldsf.path = os.path.join(settings.QC_STORAGE_DIR, 'test')
        self.olddsr.delete()
        self.oldsf.save()

    def test_fail(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)

    def test_run(self):
        # Run single file
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'Queued {self.oldsf.filename} QC raw for rerun')
        self.assertEqual(self.bup_jobs.count(), 0)
        self.assertEqual(self.qc_jobs.count(), 1)
        
        # Run again, dont care about duplicate
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'Queued {self.oldsf.filename} QC raw for rerun')
        self.assertEqual(self.qc_jobs.count(), 2)

    def test_run_with_archived_file(self):
        self.oldsf.deleted, self.oldsf.purged = True, True
        self.oldsf.save()
        rm.PDCBackedupFile.objects.create(storedfile=self.oldsf, pdcpath='testpath', success=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'Queued {self.oldsf.filename} QC raw for retrieval '
                'from archive and rerun')
        self.assertEqual(self.bup_jobs.count(), 1)
        self.assertEqual(self.qc_jobs.count(), 1)

    def test_no_qcdata(self):
        self.qcdata.delete()
        # Run single file
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'File {self.oldsf.filename} is annotated as QC but has'
                ' not been run previously and has no acquisition mode stored. Please queue manually')
        self.assertEqual(self.bup_jobs.count(), 0)
        self.assertEqual(self.qc_jobs.count(), 0)

    def test_not_find_archive(self):
        self.oldsf.deleted, self.oldsf.purged = True, True
        self.oldsf.save()
        # No backup
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'QC file {self.oldsf.filename} is marked as deleted, '
                'but cannot be restored, contact admin')
        self.assertEqual(self.bup_jobs.count(), 0)
        self.assertEqual(self.qc_jobs.count(), 0)

        # backup file deleted
        rm.PDCBackedupFile.objects.create(storedfile=self.oldsf, pdcpath='testpath', success=True,
                deleted=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'QC file {self.oldsf.filename} is marked as deleted, '
                'but cannot be restored, contact admin')
        self.assertEqual(self.bup_jobs.count(), 0)
        self.assertEqual(self.qc_jobs.count(), 0)



class RerunManyQCsTest(BaseTest):
    url = '/manage/qc/rerunmany/'

    def setUp(self):
        super().setUp()
        self.user.is_staff = True
        self.user.save()
        dm.Operator.objects.create(user=self.user)
        self.qc_jobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow')
        nfrepo = am.NextflowWorkflowRepo.objects.create()
        pset = am.ParameterSet.objects.create()
        uwf = am.UserWorkflow.objects.create(wftype=am.UserWorkflow.WFTypeChoices.QC, public=False)
        uwf.nfwfversionparamsets.create(nfworkflow=nfrepo, paramset=pset)
        ana = am.Analysis.objects.create(name='previousrun', user=self.user, storage_dir='blbala')
        dashm.QCData.objects.create(rawfile=self.tmpraw, analysis=ana, runtype=dm.AcquisistionMode.DDA)

    def test_fail(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)

    def test_rerun(self):
        # Multiple producers
        prod2 = rm.Producer.objects.create(name='prod2', client_id='hfdsioahd', shortname='p2',
                internal=True)

        days_back = 3
        # One too-old file:
        self.oldsf.path = os.path.join(settings.QC_STORAGE_DIR, 'test')
        self.oldsf.save()
        self.olddsr.delete()
        self.oldraw.date = (timezone.now() - timezone.timedelta(days_back + 1)).date()
        self.oldraw.save()

        # One just old enough
        self.f3sf.path = os.path.join(settings.QC_STORAGE_DIR, 'test')
        self.f3sf.save()
        self.f3dsr.delete()
        self.f3raw.date = (timezone.now() - timezone.timedelta(days_back)).date()
        self.f3raw.save()

        # One somewhat newer (date is already "now")
        self.tmpsf.path = os.path.join(settings.QC_STORAGE_DIR, 'test')
        self.tmpsf.save()
        self.tmpraw.claimed = True
        self.tmpraw.prod = prod2
        self.tmpraw.save()

        nr_files = 2
        nr_duplicates = 2
        nr_deleted = 2
        nr_ignored = 1
        nr_archived = 1
        cur_nr_queued = 0
        confirm_msg = f'You have selected {nr_files} QC raw files.'
        dup_msg = (f'{nr_duplicates - nr_ignored} seem to be obsolete reruns ran with the same workflow'
                ' version as the current latest (Tick the ignore box '
                'to include these in the analysis.')
        del_msg = (f'{nr_deleted} seem to be deleted, of which {nr_archived} are '
            ' in backup. (Tick the retrieve box to include these in the analysis.')
        ignore_msg = (f'Ignored {nr_ignored} files which were labeled as QC but had no run data, '
            'and thus no acquisition mode stored - please find these manually')
        queue_msg = f'Queued {nr_files - nr_ignored} QC raw files for running'

        # Get confirm dialog
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': False,
            'ignore_obsolete': False, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(confirm_msg, msg)
        self.assertIn(ignore_msg, msg)
        self.assertNotIn(dup_msg, msg)
        self.assertNotIn(del_msg, msg)
        self.assertEqual(self.qc_jobs.count(), 0)

        # Actually confirm
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': True,
            'ignore_obsolete': False, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertEqual(self.qc_jobs.count(), nr_files - nr_ignored)
        self.assertEqual(self.qc_jobs.filter(kwargs__sf_id=self.tmpsf.pk).count(), 1)
        self.assertEqual(self.qc_jobs.filter(kwargs__sf_id=self.f3sf.pk).count(), 0)
        self.assertIn(ignore_msg, msg)
        self.assertIn(queue_msg, msg)
        cur_nr_queued += nr_files - nr_ignored

        # Identical rerun which gets warning since they already been ran
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': False,
            'ignore_obsolete': False, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(confirm_msg, msg)
        self.assertIn(dup_msg, msg)
        self.assertNotIn(queue_msg, msg)
        self.assertNotIn(del_msg, msg)
        self.assertIn(ignore_msg, msg)
        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)

        # Identical rerun confirm to do it
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': True,
            'ignore_obsolete': True, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(queue_msg, msg)
        self.assertIn(ignore_msg, msg)
        cur_nr_queued += nr_files - nr_ignored
        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)

        # Identical rerun do not confirm to do it - reply with no files are run
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': True,
            'ignore_obsolete': False, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(f'Queued {nr_files - nr_duplicates} QC raw files for running', msg)
        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)

        # Rerun with backed up files asks for confirm
        rm.PDCBackedupFile.objects.create(storedfile=self.tmpsf, success=True, deleted=False)
        self.f3sf.deleted = True
        self.f3sf.save()
        self.tmpsf.deleted = True
        self.tmpsf.save()
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': False,
            'ignore_obsolete': False, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(confirm_msg, msg)
        self.assertIn(dup_msg, msg)
        self.assertNotIn(queue_msg, msg)
        self.assertIn(del_msg, msg)
        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)

        # Confirm to also retrieve files
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': True,
            'ignore_obsolete': True, 'retrieve_archive': True})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(f'Queued {nr_files - nr_deleted + nr_archived} QC raw files for running', msg)
        self.assertIn(f'Queued {nr_archived} QC raw files for retrieval from archive', msg)
        cur_nr_queued += nr_files - nr_deleted + nr_archived

        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)
        bup_jobs = jm.Job.objects.filter(funcname='restore_from_pdc_archive',
                kwargs__sf_id=self.tmpsf.pk)
        self.assertEqual(bup_jobs.count(), nr_archived)
        self.assertEqual(jm.Job.objects.filter(funcname='restore_from_pdc_archive').count(),
                nr_archived)
