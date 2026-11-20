#from django.test import TestCase

from mstulos import models as m
from analysis import models as am
from jobs import models as jm
from jobs import jobs as jj
from rawstatus import models as rm
from datasets import models as dm
from kantele.tests import BaseTest, BaseIntegrationTest
from kantele import settings

from django.contrib.auth.models import User
from django.utils import timezone
from django.test import tag


@tag('mstulos')
class BaseTC(BaseIntegrationTest):
    def setUp(self):
        super().setUp()
        self.expn = 'exp1'
        self.token = 'token1234'
        self.ana = am.Analysis.objects.create(user=self.user, name='testana_mst', storage_dir='testdir_mst')
        outfieldkwargs = {
                'peppeptidefield': 'Peptide sequence',
                'peppepfield': 'PEP',
                'psmprotfield': 'Protein',
                'psmms1field': 'MS1 area',
                'pepms1field': 'MS1 area (highest of all PSMs)',
                'pepfdrfield': 'q-value',
                'psmfdrfield': 'PSM q-value',
                'psmpepfield': 'PSM PEP',
                'psmchargefield': 'Charge',
                'psmmzfield': 'Precursor',
                'psmfnfield': 'SpectraFile',
                'scanfield': 'ScanNum',
                'rtfield': 'Retention time(min)',
                'psmscorefield': 'MSGFScore',
                'psmsetname': 'Biological set',
                'psmpeptide': 'Peptide',
                'genetablegenefield': 'Gene Name',
                }
        wfokw = {'description': 'test', 'psmfile': 'psms.txt', 'pepfile': 'peps.txt',
                'genefile': 'genes', 'fasta_arg': '--tdb'}
        for fieldname, field in outfieldkwargs.items():
            ofn = am.OutputFieldName.objects.create(description=field, fieldname=field)
            wfokw[fieldname] = ofn

        wfo = am.WfOutput.objects.create(**wfokw)
        nfw = am.NextflowWorkflowRepo.objects.create(description='a wf', repo='gh/wf')
        pset = am.ParameterSet.objects.create(name='ps1')
        pmod = am.Param.objects.create(name='mods', nfparam='--mods', ptype=am.Param.PTypes.MULTI, help='help')
        p_opt1 = am.ParamOption.objects.create(param=pmod, name='oxi', value='Oxidation')
        p_opt2 = am.ParamOption.objects.create(param=pmod, name='carba', value='Carbamidomethyl')
        am.PsetParam.objects.create(pset=pset, param=pmod)
        am.AnalysisParam.objects.create(analysis=self.ana, param=pmod, value=[p_opt1.pk, p_opt2.pk])

        self.nfwf = am.NextflowWfVersionParamset.objects.create(update='test update', commit='abc123',
                filename='main.nf', profiles=[], nfworkflow=nfw, paramset=pset, envvars='NXF_VER=22', active=True)
        am.PipelineVersionOutput.objects.create(nfwfversion=self.nfwf, output=wfo)
        self.wf = am.UserWorkflow.objects.create(name='testwf', public=True,
                wftype=am.UserWorkflow.WFTypeChoices.STD)
        self.wf.nfwfversionparamsets.add(self.nfwf)
        
        sp = dm.SelectParameter.objects.create(title='dtype', category=1)
        spo = dm.SelectParameterOption.objects.create(param=sp, value='DDA')
        dm.SelectParameterValue.objects.create(dataset=self.ds, value=spo)

        # For convenience all files are stored in tulosdir, including fasta
        testpath = 'mstulos'
        ssana = rm.ServerShare.objects.create(name=settings.ANALYSISSHARENAME,
                server=self.newfserver, share='/home/analysis')
        psmraw = rm.RawFile.objects.create(name='psms.txt', producer=self.anaprod,
                source_md5='psm.result', size=100, date=timezone.now(), claimed=True)
        psmsf = rm.StoredFile.objects.create(rawfile=psmraw, filetype_id=self.ft.id,
            filename=psmraw.name, servershare=ssana, path=testpath, md5=psmraw.source_md5)
        am.AnalysisResultFile.objects.create(analysis=self.ana, sfile=psmsf)
        pepraw = rm.RawFile.objects.create(name='peps.txt', producer=self.anaprod,
                source_md5='pep.result', size=100, date=timezone.now(), claimed=True)
        pepsf = rm.StoredFile.objects.create(rawfile=pepraw, filetype_id=self.ft.id,
            filename=pepraw.name, servershare=ssana, path=testpath, md5=pepraw.source_md5)
        am.AnalysisResultFile.objects.create(analysis=self.ana, sfile=pepsf)
        generaw = rm.RawFile.objects.create(name='genes.txt', producer=self.anaprod,
                source_md5='gene.result', size=100, date=timezone.now(), claimed=True)
        genesf = rm.StoredFile.objects.create(rawfile=generaw, filetype_id=self.ft.id,
            filename=generaw.name, servershare=ssana, path=testpath, md5=generaw.source_md5)
        am.AnalysisResultFile.objects.create(analysis=self.ana, sfile=genesf)
        fastaraw = rm.RawFile.objects.create(name='fasta.txt', producer=self.anaprod,
                source_md5='fasta.result', size=100, date=timezone.now(), claimed=True)
        self.fastasf = rm.StoredFile.objects.create(rawfile=fastaraw, filetype_id=self.ft.id,
            filename=fastaraw.name, servershare=ssana, path=testpath, md5=fastaraw.source_md5)
        am.AnalysisSampletable.objects.create(analysis=self.ana, samples=[['126', 'set01', 'sample', 'group']])
        asn = am.AnalysisSetname.objects.create(analysis=self.ana, setname='set01')
        dsa = am.DatasetAnalysis.objects.create(analysis=self.ana, dataset=self.ds)
        am.AnalysisDSInputFile.objects.create(dsanalysis=dsa, sfile=self.f3sfmz, analysisset=asn)
        am.AnalysisIsoquant.objects.create(analysis=self.ana, setname=asn, value=[])

        job = jm.Job.objects.create(funcname='anajob', kwargs={'inputs': {'params': {'--mods': 'Carbamidomethyl;Oxidation'}, 'singlefiles': {'--tdb': self.fastasf.pk}}},
                state=jj.Jobstates.DONE, timestamp=timezone.now())
        self.nfs = am.NextflowSearch.objects.create(analysis=self.ana, nfwfversionparamset=self.nfwf,
                workflow=self.wf, token='mstulos_ana', job=job)

        m.Modification.objects.create(mass=15.994915, unimod_name='Oxidation', unimod_id=35, predefined_aa_list=[["M", "var", "stable"]])
        m.Modification.objects.create(mass=57.021464, unimod_name='Carbamidomethyl', unimod_id=4, predefined_aa_list=[["C", "fix", "stable"]])
        tmt = m.Modification.objects.create(mass=304.207146, unimod_name='TMTpro', unimod_id=2016, predefined_aa_list=[])
        m.QuantLabelMod.objects.create(quanttype=self.qt, mod=tmt)

        self.username='adminuser'
        email = 'admin@test.test'
        self.password='12345'
        user = User(username=self.username, email=email, is_staff=True)
        user.set_password(self.password)
        user.save() 
        login = self.cl.login(username=self.username, password=self.password)
        #self.exp = m.Experiment.objects.create(analysis=ana, token=self.token, wfoutput_found=wfo)
        #self.gene1 = m.Gene.objects.create(name='gene1', organism=self.species)
        #self.prot1 = m.Protein.objects.create(name='protein-1')
        #pseq = m.PeptideSeq.objects.create(seq='IAMAPEPTIDE')
        #pepprot = m.PeptideProtein.objects.create(peptide=pseq, protein=self.prot1, experiment=self.exp)
        #m.ProteinGene.objects.create(pepprot=pepprot, gene=self.gene1)
        #self.pep1 = m.PeptideMolecule.objects.create(encoded_pep='+123.345IAMAPEPTIDE', sequence=pseq)
        #CT = m.Condition.Condtype
        #m.Condition.objects.filter(experiment=self.exp).delete()
        #self.cond_sam = m.Condition.objects.create(cond_type=CT.SAMPLE, name='sam1', experiment=self.exp)
        #self.cond_set = m.Condition.objects.create(cond_type=CT.SAMPLESET, name='sam1', experiment=self.exp)
        #self.cond_ch1 = m.Condition.objects.create(cond_type=CT.CHANNEL, name='ch1', experiment=self.exp)
        #self.cond_ch2 = m.Condition.objects.create(cond_type=CT.CHANNEL, name='ch2', experiment=self.exp)
        #self.cond_fn = m.Condition.objects.create(cond_type=CT.FILE, name='file1', experiment=self.exp)


class TestUploadAnalysis(BaseTC):
    jobname = 'ingest_search_results'

    def test_with_genes(self):
        self.url = f'/mstulos/add/{self.ana.pk}/'
        resp = self.post_json({})
        self.assertEqual(resp.status_code, 200)

        job = jm.Job.objects.last()
        self.assertEqual(job.kwargs['analysis_id'], self.ana.pk)
        self.assertEqual(job.kwargs['organism_id'], self.spec1.pk)
        self.assertIn('token', job.kwargs.keys())
        # FIXME This line is a problem!? Other liveservertestcase cannot run, address already in use, 
        # unless we comment out below (to end of test, else it fails). Even when the test fails, this
        # problem remains. 
        self.run_job()

        job.refresh_from_db()
        self.assertEqual(job.state, jj.Jobstates.PROCESSING)
        self.run_job()
        job.refresh_from_db()
        self.assertEqual(job.state, jj.Jobstates.DONE)
        self.assertEqual(m.PSM.objects.count(), 4)
        self.assertEqual(m.Gene.objects.count(), 5)
        self.assertEqual(m.Protein.objects.count(), 10)
        self.assertEqual(m.PSM.objects.count(), 4)


#class TestUploadProteins(BaseTC):
#    url = '/mstulos/upload/proteins/'
#
#    def test_fail(self):
#        resp = self.cl.get(self.url)
#        self.assertEqual(resp.status_code, 405)
#        resp = self.post_json({'token': 'nottoken'})
#        self.assertEqual(resp.status_code, 403)
#
#    def test_upload_proteins(self):
#        resp = self.post_json({'token': self.token, 'organism_id': self.species.pk,
#            'protgenes': [('prot2', 'gene2'), (self.prot1.name, self.gene1.name), ('prot3', 'gene2')]})
#        self.assertEqual(resp.status_code, 200)
#        self.assertEqual(m.Protein.objects.count(), 3)
#        self.assertEqual(m.Gene.objects.count(), 2)
#        self.assertEqual(m.ProteinGene.objects.count(), 3)
#        rj = resp.json()
#        p2m = m.Protein.objects.get(name='prot2')
#        p3m = m.Protein.objects.get(name='prot3')
#        for pn, ppk in [('prot2', p2m.pk), (self.prot1.name, self.prot1.pk), ('prot3', p3m.pk)]:
#            self.assertEqual(rj['protein_ids'][pn], ppk)
#
#    def test_nogenes(self):
#        self.fail()
#
#
#    def test_upload_pepprots(self):
#        p2, p3 = '+456IAMAPEPTIDE', 'IAMAPEPTIDE'
#        resp = self.post_json({'token': self.token, 'pepprots': [
#            (self.pep1.encoded_pep, self.pep1.sequence.seq, self.prot1.pk),
#            (p2, self.pep1.sequence.seq, self.prot1.pk),
#            (p3, 'ANOTHERONE', self.prot1.pk),
#            ]})
#        self.assertEqual(resp.status_code, 200)
#        self.assertEqual(m.PeptideMolecule.objects.count(), 3)
#        self.assertEqual(m.PeptideSeq.objects.count(), 2)
#        self.assertEqual(m.PeptideProtein.objects.count(), 2)
#        p2m = m.PeptideMolecule.objects.get(encoded_pep=p2)
#        p3m = m.PeptideMolecule.objects.get(encoded_pep=p3)
#        rj = resp.json()
#        self.assertEqual(rj['pep_ids'], {self.pep1.encoded_pep: self.pep1.pk, p2: p2m.pk, p3: p3m.pk})


#class TestUploadPeptides(BaseTC):
#    url = '/mstulos/upload/peptides/'
#
#    def test_fail(self):
#        resp = self.cl.get(self.url)
#        self.assertEqual(resp.status_code, 405)
#        resp = self.post_json({'token': 'nottoken'})
#        self.assertEqual(resp.status_code, 403)
#
#    def test_upload_peptides(self):
#        resp = self.post_json({'token': self.token, 'peptides': [{
#            'qval': [(self.cond_set.pk, 0.01)], 'psmcount': [(self.cond_set.pk, 3)], 
#            'isobaric': [(self.cond_ch1.pk, 100), (self.cond_ch2.pk, 200)],
#            'pep_id': self.pep1.pk}]})
#        self.assertEqual(resp.status_code, 200)
#        self.assertEqual(m.PeptideFDR.objects.get(peptide=self.pep1, condition=self.cond_set).fdr, 0.01)
#        self.assertEqual(m.AmountPSMsPeptide.objects.get(peptide=self.pep1, condition=self.cond_set).value, 3)
#        self.assertEqual(m.PeptideIsoQuant.objects.get(peptide=self.pep1, condition=self.cond_ch1).value, 100)
#        self.assertEqual(m.PeptideIsoQuant.objects.get(peptide=self.pep1, condition=self.cond_ch2).value, 200)
#
#    def test_noisobaric(self):
#        self.fail()
#
#class TestUploadPSMs(BaseTC):
#    url = '/mstulos/upload/psms/'
#
#    def test_fail(self):
#        resp = self.cl.get(self.url)
#        self.assertEqual(resp.status_code, 405)
#        resp = self.post_json({'token': 'nottoken'})
#        self.assertEqual(resp.status_code, 403)
#
#    def test_upload_psms(self):
#        resp = self.post_json({'token': self.token, 'psms': [
#            {'pep_id': self.pep1.pk, 'qval': 0.01, 'scan': 123, 'fncond': self.cond_fn.pk,
#                'score': 15}]})
#        self.assertEqual(m.PSM.objects.count(), 1)
#        psm = m.PSM.objects.get()
#        self.assertEqual(psm.fdr, 0.01)
#        self.assertEqual(psm.scan, 123)
#        self.assertEqual(psm.score, 15)
#
#
#class TestUploadDone(BaseTC):
#    url = '/mstulos/upload/done/'
#
#    def test_fail(self):
#        resp = self.cl.get(self.url)
#        self.assertEqual(resp.status_code, 405)
#        resp = self.post_json({'token': 'nottoken'})
#        self.assertEqual(resp.status_code, 403)
#
#    def test_uploaddone(self):
#        self.fail()
