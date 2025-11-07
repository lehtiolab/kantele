import os
import re

from django.utils import timezone
from kantele.tests import BaseTest
from kantele import settings
from rawstatus import models as rm
from datasets import models as dm
from jobs import models as jm
from jobs import jobs as jj
from analysis import models as am
from dashboard import models as dashm


class BaseQCFileTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.user.is_staff = True
        self.user.save()

class QueueNewFile(BaseQCFileTest):
    url = '/manage/qc/newfile/'

    def test_bad(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={
            'sfid': self.tmpsf.pk, 'acqtype': ['DIA']})
        self.assertEqual(noid_resp.status_code, 400)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={
            'sfid': self.tmpsf.pk, 'acqtype': 'DIAA'})
        self.assertEqual(noid_resp.status_code, 400)

    def test_run_new_qc(self):
        mv_jobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare', state=jj.Jobstates.PENDING,
                kwargs__sfloc_id=self.tmpsss.pk)
        qc_jobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow',
                state=jj.Jobstates.PENDING)
        self.assertEqual(mv_jobs.count(), 0)
        self.assertEqual(mv_jobs.count(), 0)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'sfid': self.tmpsf.pk, 'acqtype': 'DIA'})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mv_jobs.count(), 1)
        qc_jobs = qc_jobs.filter(kwargs__sfloc_id=mv_jobs.first().kwargs['dstsfloc_id'])
        self.assertEqual(qc_jobs.count(), 1)

    def test_run_qc_file_already_moved(self):
        jm.Job.objects.create(funcname='run_longit_qc_workflow', state=jj.Jobstates.ERROR,
                kwargs={'sfloc_id': self.tmpsss.pk}, timestamp=timezone.now())
        mv_jobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare', state=jj.Jobstates.PENDING,
                kwargs__sfloc_id=self.tmpsss.pk)
        qc_jobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow',
                state=jj.Jobstates.PENDING)
        self.assertEqual(mv_jobs.count(), 0)
        self.assertEqual(mv_jobs.count(), 0)
        resp = self.cl.post(self.url, content_type='application/json', data={
            'sfid': self.tmpsf.pk, 'acqtype': 'DIA'})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mv_jobs.count(), 1)
        qc_jobs = qc_jobs.filter(kwargs__sfloc_id=mv_jobs.first().kwargs['dstsfloc_id'])
        self.assertEqual(qc_jobs.count(), 1)


class RerunSingleQCTest(BaseQCFileTest):
    url = '/manage/qc/rerunsingle/'

    def setUp(self):
        super().setUp()
        self.bup_jobs = jm.Job.objects.filter(funcname='restore_from_pdc_archive',
                kwargs__sfloc_id__in=[x.pk for x in self.oldsf.storedfileloc_set.all()])
        self.qc_jobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow')
        self.rs_jobs = jm.Job.objects.filter(funcname='rsync_otherfiles_to_servershare',
                kwargs__sfloc_id__in=[x.pk for x in self.oldsf.storedfileloc_set.all()])
        ana = am.Analysis.objects.create(name='previousrun', user=self.user, base_rundir='blbala',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        self.qcdata = dashm.QCRun.objects.create(rawfile=self.oldraw, analysis=ana,
                runtype=dm.AcquisistionMode.DDA)
        self.olddsr.delete()
        self.oldraw.usetype = rm.UploadFileType.QC
        self.oldraw.save()

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
        self.oldsf.deleted, self.oldsss.purged, self.oldsss.active = True, True, False
        self.oldsf.save()
        self.oldsss.servershare = self.ssinbox
        self.oldsss.save()
        rm.PDCBackedupFile.objects.create(storedfile=self.oldsf, pdcpath='testpath', success=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['msg'], f'Queued {self.oldsf.filename} QC raw for rerun')
        self.assertEqual(self.bup_jobs.count(), 1)
        self.assertEqual(self.rs_jobs.count(), 1)
        self.assertEqual(self.qc_jobs.count(), 1)

    def test_not_find_archive(self):
        self.oldsf.deleted, self.oldsss.purged, self.oldsss.active = True, True, False
        self.oldsf.save()
        self.oldsss.save()
        # No backup
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['msg'], f'QC file {self.oldsf.filename} is marked as deleted, '
                'but cannot be restored, contact admin')
        self.assertEqual(self.bup_jobs.count(), 0)
        self.assertEqual(self.qc_jobs.count(), 0)

        # backup file deleted
        rm.PDCBackedupFile.objects.create(storedfile=self.oldsf, pdcpath='testpath', success=True,
                deleted=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'sfid': self.oldsf.pk})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['msg'], f'QC file {self.oldsf.filename} is marked as deleted, '
                'but cannot be restored, contact admin')
        self.assertEqual(self.bup_jobs.count(), 0)
        self.assertEqual(self.qc_jobs.count(), 0)



class RerunManyQCsTest(BaseQCFileTest):
    url = '/manage/qc/rerunmany/'

    def setUp(self):
        super().setUp()
        self.qc_jobs = jm.Job.objects.filter(funcname='run_longit_qc_workflow')
        nfrepo = am.NextflowWorkflowRepo.objects.create()
        pset = am.ParameterSet.objects.create()
        uwf = am.UserWorkflow.objects.create(wftype=am.UserWorkflow.WFTypeChoices.QC, public=False)
        uwf.nfwfversionparamsets.create(nfworkflow=nfrepo, paramset=pset)
        oldana = am.Analysis.objects.create(name='previousrun', user=self.user, base_rundir='blbala',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        dashm.QCRun.objects.create(rawfile=self.oldraw, analysis=oldana, runtype=dm.AcquisistionMode.DDA)
        tmpana = am.Analysis.objects.create(name='previousrun', user=self.user, base_rundir='blbala3',
                securityclass=rm.DataSecurityClass.NOSECURITY)
        dashm.QCRun.objects.create(rawfile=self.tmpraw, analysis=tmpana, runtype=dm.AcquisistionMode.DDA)

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
        self.olddsr.delete()
        self.oldraw.date = (timezone.now() - timezone.timedelta(days_back + 1)).date()
        self.oldraw.save()

        # One just old enough
        self.f3raw.usetype = rm.UploadFileType.QC
        self.f3sss.save()
        self.f3dsr.delete()
        self.f3raw.date = (timezone.now() - timezone.timedelta(days_back)).date()
        self.f3raw.save()

        # One somewhat newer (date is already "now")
        self.tmpraw.usetype = rm.UploadFileType.QC
        self.tmpsss.save()
        self.tmpraw.claimed = True
        self.tmpraw.prod = prod2
        self.tmpraw.save()

        nr_files = 2 # Amount inside date
        nr_duplicates = 2
        nr_deleted = 1
        nr_ignored = 1 # no QC run
        nr_archived = 1
        cur_nr_queued = 0
        confirm_msg = f'You have selected {nr_files - nr_ignored} QC raw files.'
        dup_msg = (f'{nr_duplicates - nr_ignored} seem to be obsolete reruns ran with the same workflow'
                ' version as the current latest (Tick the ignore box '
                'to include these in the analysis.')
        del_msg = (f'{nr_deleted} seem to be deleted, of which {nr_archived} are '
            ' in backup. (Tick the retrieve box to include these in the analysis.')
        queue_msg = f'Queued {nr_files - nr_ignored} QC raw files for running'

        # Get confirm dialog
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': False,
            'ignore_obsolete': False, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(confirm_msg, msg)
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
        self.assertEqual(self.qc_jobs.filter(kwargs__sfloc_id__in=[x.pk for x in self.tmpsf.storedfileloc_set.all()]).count(), 1)
        self.assertEqual(self.qc_jobs.filter(kwargs__sfloc_id=[x.pk for x in self.f3sf.storedfileloc_set.all()]).count(), 0)
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
        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)

        # Identical rerun confirm to do it
        resp = self.cl.post(self.url, content_type='application/json', data={
            'instruments': [self.prod.pk, prod2.pk], 'days': days_back, 'confirm': True,
            'ignore_obsolete': True, 'retrieve_archive': False})
        self.assertEqual(resp.status_code, 200)
        msg = resp.json()['msg']
        self.assertIn(queue_msg, msg)
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
        self.assertIn(f'Queued {nr_files - nr_ignored - nr_deleted + nr_archived} QC raw files for running', msg)
        self.assertIn(f'Queued {nr_archived} QC raw files for retrieval from archive', msg)
        cur_nr_queued += nr_files - nr_ignored - nr_deleted + nr_archived

        self.assertEqual(self.qc_jobs.count(), cur_nr_queued)
        bup_jobs = jm.Job.objects.filter(funcname='restore_from_pdc_archive',
                kwargs__sfloc_id__in=[x.pk for x in self.tmpsf.storedfileloc_set.all()])
        self.assertEqual(bup_jobs.count(), nr_archived)
        self.assertEqual(jm.Job.objects.filter(funcname='restore_from_pdc_archive').count(),
                nr_archived)


class TestSaveTrackPeptides(BaseQCFileTest):
    url = '/manage/qc/trackpeptides/save/'

    def test_bad(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)
        nopep_resp = self.cl.post(self.url, content_type='application/json', data={'tpsname': 'test',
            'peptides': [{'hello': 'fail'}]})
        self.assertEqual(nopep_resp.status_code, 400)
        self.assertIn('Sequence and charge are mandatory', nopep_resp.json()['msg'])

    def test_save(self):
        noexist = self.cl.post(self.url, content_type='application/json', data={'tpsname': 'test',
            'peptides': [{'seq': 'PEPTIDE', 'charge': 2}]})
        self.assertEqual(noexist.status_code, 200)
        pepset = noexist.json()
        self.assertEqual(pepset['state'], 'ok')
        alreadyexist = self.cl.post(self.url, content_type='application/json', data={'tpsname': 'test',
            'peptides': [{'seq': 'PEPTIDE', 'charge': 2}]})
        self.assertEqual(alreadyexist.status_code, 403)
        self.assertIn('Name for peptide set already exists', alreadyexist.json()['msg'])
        edit = self.cl.post(self.url, content_type='application/json', data={'tpsname': 'editedname',
            'tpsid': pepset['data']['id'], 'peptides': [{'seq': 'newPEPTIDE', 'charge': 3}]})
        self.assertEqual(edit.status_code, 200)
        psdb = dashm.PeptideInSet.objects.filter(peptideset_id=pepset['data']['id'])
        self.assertEqual(psdb.count(), 1)
        self.assertEqual(psdb.values('peptide__sequence').get()['peptide__sequence'], 'NEWPEPTIDE')

        freeze = self.cl.post(self.url, content_type='application/json', data={
            'tpsid': pepset['data']['id'], 'peptides': [{'seq': 'PEPTIDE', 'charge': 2}],
            'publish': True, 'tpsname': pepset['data']['name']})
        self.assertEqual(freeze.status_code, 200)
        self.assertTrue(freeze.json()['data']['frozen'])
        frozen = self.cl.post(self.url, content_type='application/json', data={
            'tpsid': pepset['data']['id'], 'peptides': [{'seq': 'PEPTIDE', 'charge': 2}],
            'tpsname': 'newname'})
        self.assertEqual(frozen.status_code, 403)
        self.assertIn('it has likely been published already', frozen.json()['msg'])


class TestDeleteTrackPeptides(BaseQCFileTest):
    url = '/manage/qc/trackpeptides/delete/'

    def test_bad(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)
        noexist = self.cl.post(self.url, content_type='application/json', data={'tpsid': 10})
        self.assertEqual(noexist.status_code, 403)

    def test_delete(self):
        tps = dashm.TrackedPeptideSet.objects.create(name='testps', acqmode=dm.AcquisistionMode.DIA)
        tps2 = dashm.TrackedPeptideSet.objects.create(name='testps2', acqmode=dm.AcquisistionMode.DIA)
        pep = dashm.TrackedPeptide.objects.create(sequence='PEPTIDE', charge=3)
        pep2 = dashm.TrackedPeptide.objects.create(sequence='IAMAPEPTIDE', charge=2)
        pins = dashm.PeptideInSet.objects.create(peptideset=tps, peptide=pep)
        pins2 = dashm.PeptideInSet.objects.create(peptideset=tps, peptide=pep2)
        pins3 = dashm.PeptideInSet.objects.create(peptideset=tps2, peptide=pep2)
        delresp = self.cl.post(self.url, content_type='application/json', data={'tpsid': tps.pk})
        self.assertEqual(delresp.status_code, 200)
        pins3.refresh_from_db()
        for deleted_pins in [pins2, pins]:
            try:
                deleted_pins.refresh_from_db()
            except dashm.PeptideInSet.DoesNotExist:
                pass
            else:
                self.fail()


class TestSaveServers(BaseQCFileTest):
    url = '/manage/servers/save/'
    
    def test_bad(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)
        noexist = self.cl.post(self.url, content_type='application/json',
                data={'show_analysis_profile': False, 'pk': 100001})
        self.assertEqual(noexist.status_code, 404)
        jsonfail = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'show_analysis_profile': True, 'nfprofiles': 'hello', 'queue_name': 'sajda'})
        self.assertEqual(jsonfail.status_code, 400)
        self.assertEqual(jsonfail.json()['msg'], 'Not valid JSON for nextflow profiles')
        queuefail = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'show_analysis_profile': True, 'nfprofiles': '["hello"]'})
        self.assertEqual(queuefail.status_code, 400)
        self.assertEqual(queuefail.json()['msg'], 'Need to enter analysis server information')

    def test_new_server(self):
        anaq = rm.AnalysisServerProfile.objects.filter(server__active=True)
        mountq = rm.FileserverShare.objects
        new = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'test', 'uri': 'uri123', 'fqdn': 'a.b.c.d', 'active': True, 'can_backup': True,
            'show_analysis_profile': False, 'can_rsync_remote': False, 'rsyncusername': 'testname',
            'rsynckeyfile': 'testfile', 'mounted': []})
        self.assertIn('Saved server test with ID ', new.json()['msg'])
        fs = rm.FileServer.objects.get(name='test', uri='uri123', fqdn='a.b.c.d')
        self.assertFalse(anaq.filter(server=fs).exists())
        self.assertFalse(mountq.filter(server=fs).exists())
        self.assertEqual(new.status_code, 200)

        # New server with mount
        new_mount = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'testmount', 'uri': 'urimount', 'fqdn': 'a.b.c.d', 'active': True, 'can_backup': True,
            'show_analysis_profile': False, 'can_rsync_remote': True, 'rsyncusername': '',
            'rsynckeyfile': '', 'mounted': [{'share': self.ssana.pk, 'path': 'fake'}]})
        self.assertIn('Saved server testmount with ID ', new_mount.json()['msg'])
        fs = rm.FileServer.objects.get(name='testmount', can_rsync_remote=True, uri='urimount')
        self.assertFalse(anaq.filter(server=fs).exists())
        self.assertTrue(mountq.filter(server=fs, share=self.ssana, path='fake').exists())
        self.assertEqual(new_mount.status_code, 200)

        # New server with analysis
        new_ana = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'testana', 'uri': 'uriana', 'fqdn': 'a.b.c.d', 'active': True, 'can_backup': True,
            'can_rsync_remote': False, 'rsyncusername': 'testname', 'rsynckeyfile': 'testfile',
            'mounted': [], 'show_analysis_profile': True, 'nfprofiles': '["prof1"]',
            'queue_name': 'q1', 'scratchdir': '/path/to/scratch'})
        self.assertIn('Saved server testana with ID ', new_ana.json()['msg'])
        fs = rm.FileServer.objects.get(name='testana', can_rsync_remote=False, uri='uriana')
        x = anaq.get(server=fs)
        self.assertTrue(anaq.filter(server=fs, scratchdir='/path/to/scratch', queue_name='q1',
            nfprofiles=['prof1']).exists())
        self.assertFalse(mountq.filter(server=fs).exists())
        self.assertEqual(new_ana.status_code, 200)


        # New server with mount and analysis
        new_mounts_ana = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'test_mount_ana', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': True, 'can_backup': True,
            'can_rsync_remote': False, 'rsyncusername': 'testname', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssana.pk, 'path': 'fake2'}], 'show_analysis_profile': True,
            'nfprofiles': '["prof2"]', 'queue_name': 'q2', 'scratchdir': '/path/to/otherscratch'})
        self.assertIn('Saved server test_mount_ana', new_mounts_ana.json()['msg'])
        fs = rm.FileServer.objects.get(name='test_mount_ana', can_backup=True, rsyncusername='testname')
        self.assertTrue(mountq.filter(server=fs, share=self.ssana, path='fake2').exists())
        self.assertTrue(anaq.filter(server=fs, scratchdir='/path/to/otherscratch', queue_name='q2',
            nfprofiles=['prof2']).exists())
        self.assertEqual(new_mounts_ana.status_code, 200)

    def test_update_server(self):
        new_mounts_ana = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'test_mount_ana', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': True, 'can_backup': True,
            'can_rsync_remote': False, 'rsyncusername': 'testname', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssana.pk, 'path': 'fake2'}], 'show_analysis_profile': True,
            'nfprofiles': '["prof2"]', 'queue_name': 'q2', 'scratchdir': '/path/to/otherscratch'})
        pk = re.match('Saved server test_mount_ana with ID ([0-9]+)', new_mounts_ana.json()['msg']).group(1)
        fs = rm.FileServer.objects.get(pk=pk)
        self.assertEqual(fs.name, 'test_mount_ana')
        self.assertTrue(fs.can_backup)
        self.assertEqual(fs.rsyncusername, 'testname')
        fss = rm.FileserverShare.objects.filter(server=fs)
        self.assertEqual(fss.count(), 1)
        self.assertTrue(fss.filter(share=self.ssana, path='fake2').exists())
        asp = rm.AnalysisServerProfile.objects.filter(server=fs)
        self.assertTrue(asp.filter(queue_name='q2', scratchdir='/path/to/otherscratch',
            nfprofiles=['prof2']).exists())

        # Add new mount, change some names
        upd_resp = self.cl.post(self.url, content_type='application/json', data={'pk': pk, 
            'name': 'test_mount_ana_upd', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': True, 'can_backup': False,
            'can_rsync_remote': False, 'rsyncusername': '', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssana.pk, 'path': 'fake2'}, {'share': self.ssanaruns.pk, 'path': 'fake3'}],
            'show_analysis_profile': True,
            'nfprofiles': '["prof2"]', 'queue_name': 'q2', 'scratchdir': '/path/to/otherscratch'})
        self.assertEqual(f'Saved server test_mount_ana_upd with ID {pk}', upd_resp.json()['msg'])
        fs.refresh_from_db()
        self.assertEqual(fs.name, 'test_mount_ana_upd')
        self.assertEqual(fs.rsyncusername, '')
        self.assertFalse(fs.can_backup)
        self.assertEqual(fss.count(), 2)
        self.assertTrue(fss.filter(share=self.ssana, path='fake2').exists())
        self.assertTrue(fss.filter(share=self.ssanaruns, path='fake3').exists())


        # Remove 1 mount and analysis profile
        upd_resp = self.cl.post(self.url, content_type='application/json', data={'pk': pk, 
            'name': 'test_mount_ana_upd', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': True,
            'can_backup': False,
            'can_rsync_remote': False, 'rsyncusername': '', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssanaruns.pk, 'path': 'fake3'}], 'show_analysis_profile': False})
        self.assertEqual(f'Saved server test_mount_ana_upd with ID {pk}', upd_resp.json()['msg'])
        fs.refresh_from_db()
        self.assertEqual(fs.name, 'test_mount_ana_upd')

        self.assertEqual(fss.count(), 1)
        self.assertTrue(fss.filter(share=self.ssanaruns, path='fake3').exists())
        self.assertFalse(asp.exists())

        # Deactivate, name change, remove analysis and added mount -- but that should not save
        upd_resp = self.cl.post(self.url, content_type='application/json', data={'pk': pk, 
            'name': 'test_mount_ana_upd_deac', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': False,
            'can_backup': False,
            'can_rsync_remote': False, 'rsyncusername': '', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssana.pk, 'path': 'fake2'}, {'share': self.ssanaruns.pk, 'path': 'fake3'}],
            'show_analysis_profile': True,
            'nfprofiles': '["prof2"]', 'queue_name': 'q2', 'scratchdir': '/path/to/otherscratch'})
        self.assertEqual(f'Deactivated server with ID {pk}', upd_resp.json()['msg'])
        fs.refresh_from_db()
        self.assertEqual(fs.name, 'test_mount_ana_upd')
        self.assertEqual(fss.count(), 1)
        self.assertFalse(asp.exists())

        # Fail to update a deactivated server
        upd_resp = self.cl.post(self.url, content_type='application/json', data={'pk': pk, 
            'name': 'test_mount_ana_upd_deac', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': False,
            'can_backup': False,
            'can_rsync_remote': False, 'rsyncusername': '', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssana.pk, 'path': 'fake2'}, {'share': self.ssanaruns.pk, 'share': 'fake3'}],
            'show_analysis_profile': True,
            'nfprofiles': '["prof2"]', 'queue_name': 'q2', 'scratchdir': '/path/to/otherscratch'})
        self.assertEqual(f'Cannot update deactivated server', upd_resp.json()['msg'])
        self.assertEqual(upd_resp.status_code, 403)
        fs.refresh_from_db()
        self.assertEqual(fs.name, 'test_mount_ana_upd')
        self.assertEqual(fss.count(), 1)
        self.assertFalse(asp.exists())

        # Reactivate and update
        upd_resp = self.cl.post(self.url, content_type='application/json', data={'pk': pk, 
            'name': 'test_mount_ana_upd_act', 'uri': 'uritma', 'fqdn': 'a.b.c.d', 'active': True,
            'can_backup': False,
            'can_rsync_remote': False, 'rsyncusername': '', 'rsynckeyfile': 'testfile',
            'mounted': [{'share': self.ssana.pk, 'path': 'fake2'}, {'share': self.ssanaruns.pk, 'path': 'fake3'}],
            'show_analysis_profile': True,
            'nfprofiles': '["prof2"]', 'queue_name': 'q2', 'scratchdir': '/path/to/otherscratch'})
        self.assertEqual(f'Saved server test_mount_ana_upd_act with ID {pk}', upd_resp.json()['msg'])
        fs.refresh_from_db()
        self.assertEqual(fs.name, 'test_mount_ana_upd_act')
        self.assertEqual(fss.count(), 2)
        self.assertTrue(fss.filter(share=self.ssana, path='fake2').exists())
        self.assertTrue(fss.filter(share=self.ssanaruns, path='fake3').exists())
        self.assertTrue(asp.filter(queue_name='q2', scratchdir='/path/to/otherscratch',
            nfprofiles=['prof2']).exists())


class TestSaveShares(BaseQCFileTest):
    url = '/manage/shares/save/'

    def test_bad(self):
        getresp = self.cl.get(self.url)
        self.assertEqual(getresp.status_code, 405)
        noid_resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(noid_resp.status_code, 400)
        noexist = self.cl.post(self.url, content_type='application/json', data={'pk': 100001})
        self.assertEqual(noexist.status_code, 404)

    def test_new_share(self):
        lastpk = rm.ServerShare.objects.last().pk
        new = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'testshare', 'active': True, 'max_security': rm.DataSecurityClass.NOSECURITY,
            'description': 'hello', 'function': rm.ShareFunction.RAWDATA, 'maxdays_data': 1})
        self.assertIn('Saved new share testshare with ID ', new.json()['msg'])
        pk = re.match('Saved new share testshare with ID ([0-9]+)', new.json()['msg']).group(1)
        self.assertEqual(lastpk+1, int(pk))
        ss = rm.ServerShare.objects.get(pk=pk, active=True)
        self.assertEqual(ss.name, 'testshare')
        self.assertEqual(ss.max_security, rm.DataSecurityClass.NOSECURITY)
        self.assertEqual(new.status_code, 200)

    def test_update_server(self):
        new = self.cl.post(self.url, content_type='application/json', data={'pk': False,
            'name': 'testshare', 'active': True, 'max_security': rm.DataSecurityClass.NOSECURITY,
            'description': 'hello', 'function': rm.ShareFunction.RAWDATA, 'maxdays_data': 1})
        pk = re.match('Saved new share testshare with ID ([0-9]+)', new.json()['msg']).group(1)
        ss = rm.ServerShare.objects.get(pk=pk, active=True)
        self.assertEqual(ss.name, 'testshare')

        # Update name, desc, function
        upd = self.cl.post(self.url, content_type='application/json', data={'pk': pk,
            'name': 'testshare_upd', 'active': True, 'max_security': rm.DataSecurityClass.NOSECURITY,
            'description': 'hello hello', 'function': rm.ShareFunction.INBOX, 'maxdays_data': 2})
        self.assertEqual(f'Saved share testshare_upd with ID {pk}', upd.json()['msg'])
        ss.refresh_from_db()
        self.assertEqual(ss.name, 'testshare_upd')
        self.assertEqual(ss.description, 'hello hello')
        self.assertEqual(ss.function, rm.ShareFunction.INBOX)
        self.assertEqual(ss.maxdays_data, 2)
        self.assertTrue(ss.active)

        # Deactivate, dont save changes to name, desc, function
        upd = self.cl.post(self.url, content_type='application/json', data={'pk': pk,
            'name': 'testshare_upd_deac', 'active': False,
            'max_security': rm.DataSecurityClass.NOSECURITY, 'description': 'hello hello',
            'function': rm.ShareFunction.ANALYSIS_DELIVERY, 'maxdays_data': 2})
        self.assertEqual(f'Deactivated share with ID {pk}', upd.json()['msg'])
        ss.refresh_from_db()
        self.assertEqual(ss.name, 'testshare_upd')
        self.assertEqual(ss.function, rm.ShareFunction.INBOX)
        self.assertFalse(ss.active)

        # Reactivate and update
        upd = self.cl.post(self.url, content_type='application/json', data={'pk': pk,
            'name': 'testshare_upd_ac', 'active': True,
            'max_security': rm.DataSecurityClass.NOSECURITY, 'description': 'hello hello',
            'function': rm.ShareFunction.ANALYSIS_DELIVERY, 'maxdays_data': 2})
        self.assertEqual(f'Saved share testshare_upd_ac with ID {pk}', upd.json()['msg'])
        ss.refresh_from_db()
        self.assertEqual(ss.name, 'testshare_upd_ac')
        self.assertEqual(ss.function, rm.ShareFunction.ANALYSIS_DELIVERY)
        self.assertTrue(ss.active)
