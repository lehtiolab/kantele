from datetime import datetime, timedelta
import os
import json
from glob import glob
from time import sleep

from django.core.management import call_command
from django.utils import timezone
from django.db.models import Max
from django.contrib.auth.models import User

from kantele.tests import BaseTest, BaseIntegrationTest
from kantele import settings
from analysis import models as am
from rawstatus import models as rm
from jobs import models as jm
from jobs import jobs as jj
from datasets import models as dm


class TestDownloadFasta(BaseIntegrationTest):

    def test_ensembl(self):
        call_command('download_latest_fasta', species='escherichia_coli_str_k_12_substr_mg1655_gca_000005845', dbtype='ensembl', ensembl_ver=61)
        self.run_job()
        sleep(2)
        fafns = glob(os.path.join(self.inboxctrl.path, settings.LIBRARY_FILE_PATH_INBOX, 'ENS61*'))
        self.assertEqual(len(fafns), 1)
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.libctrl.path, fafns[0])))

    def test_uniprot(self):
        call_command('download_latest_fasta', species='Escherichia coli K12', dbtype='uniprot', uptype='SWISS')
        self.run_job()
        sleep(2)
        fafns = glob(os.path.join(self.inboxctrl.path, settings.LIBRARY_FILE_PATH_INBOX, 'Uniprot*'))
        self.assertEqual(len(fafns), 1)
        self.run_job()
        self.assertTrue(os.path.exists(os.path.join(self.libctrl.path, fafns[0])))


class AnalysisPageTest(BaseIntegrationTest):
    def setUp(self):
        super().setUp()
        self.oldds.locked = True
        self.oldds.save()
        self.ds.locked = True
        self.ds.save()
        self.usrfraw = rm.RawFile.objects.create(name='usrfiledone', producer=self.prod, 
                source_md5='usrfmd5', size=100, claimed=True, date=timezone.now(),
                usetype=rm.UploadFileType.USERFILE)
        self.sfusr = rm.StoredFile.objects.create(rawfile=self.usrfraw, md5=self.usrfraw.source_md5,
                filetype=self.uft, filename=self.usrfraw.name, checked=True)
        rm.StoredFileLoc.objects.create(sfile=self.sfusr, servershare=self.sslib, path='',
                purged=False, active=True)
        self.usedtoken = rm.UploadToken.objects.create(user=self.user, token='usrffailtoken',
                expired=False, producer=self.prod, filetype=self.uft,
                uploadtype=rm.UploadFileType.USERFILE, expires=timezone.now() + timedelta(1))
        self.userfile = rm.UserFile.objects.create(sfile=self.sfusr, description='This is a userfile',
            upload=self.usedtoken)

        self.pset = am.ParameterSet.objects.create(name='ps1')
        self.param1 = am.Param.objects.create(name='a flag', nfparam='--flag', ptype=am.Param.PTypes.FLAG, help='flag help')
        self.param2 = am.Param.objects.create(name='a chbox', nfparam='--multi', ptype=am.Param.PTypes.MULTI, help='help')
        self.param3 = am.Param.objects.create(name='a num', nfparam='--num', ptype=am.Param.PTypes.NUMBER, help='help')
        self.param4 = am.Param.objects.create(name='a select', nfparam='--select', ptype=am.Param.PTypes.SELECT, help='help')
        self.popt1 = am.ParamOption.objects.create(param=self.param2, name='opt 1', value='nr1')
        self.popt2 = am.ParamOption.objects.create(param=self.param2, name='opt 2', value='nr2')
        self.popt3 = am.ParamOption.objects.create(param=self.param4, name='opt 3', value='nr3')
        self.popt4 = am.ParamOption.objects.create(param=self.param4, name='opt 4', value='nr4')
        self.pfn1 = am.FileParam.objects.create(name='fp1', nfparam='--fp1', filetype=self.lft, help='help')

        self.ft2 = rm.StoredFileType.objects.create(name='result ft', filetype='txt')
        self.pfn2 = am.FileParam.objects.create(name='fp1', nfparam='--fp2', filetype=self.ft2, help='helppi')
        self.txtraw = rm.RawFile.objects.create(name='txtfn', producer=self.anaprod,
                source_md5='txtraw_fakemd5', size=1234, date=timezone.now(), claimed=False,
                usetype=rm.UploadFileType.ANALYSIS)
        self.txtsf = rm.StoredFile.objects.create(rawfile=self.txtraw, md5=self.txtraw.source_md5,
                filename=self.txtraw.name, checked=True, filetype=self.ft2)
        rm.StoredFileLoc.objects.create(sfile=self.txtsf, servershare=self.sstmp, path='',
                purged=False, active=True)

        c_ch = am.PsetComponent.ComponentChoices
        self.inputdef = am.PsetComponent.objects.create(pset=self.pset, component=c_ch.INPUTDEF, value=['file_path', 'plate', 'setname', 'instrument', 'fake'])
        am.PsetComponent.objects.create(pset=self.pset, component=c_ch.ISOQUANT)
        am.PsetComponent.objects.create(pset=self.pset, component=c_ch.ISOQUANT_SAMPLETABLE)
        am.PsetComponent.objects.create(pset=self.pset, component=c_ch.PREFRAC, value='.*fr([0-9]+).*mzML$')
        am.PsetParam.objects.create(pset=self.pset, param=self.param1)
        am.PsetParam.objects.create(pset=self.pset, param=self.param2)
        am.PsetParam.objects.create(pset=self.pset, param=self.param3)
        am.PsetParam.objects.create(pset=self.pset, param=self.param4)
        am.PsetMultiFileParam.objects.create(pset=self.pset, param=self.pfn1)
        am.PsetFileParam.objects.create(pset=self.pset, param=self.pfn2, allow_resultfiles=True)

        self.nfwf = am.NextflowWfVersionParamset.objects.create(update='an update', commit='abc123',
                filename='main.nf', profiles=[], nfworkflow=self.nfw, paramset=self.pset, nfversion='22', active=True)
        self.wftype = am.UserWorkflow.WFTypeChoices.STD
        self.wf = am.UserWorkflow.objects.create(name='testwf', wftype=self.wftype, public=True)
        self.wf.nfwfversionparamsets.add(self.nfwf)

        # Web server for reports
        webserver = rm.FileServer.objects.create(name='web', uri='kantele.test',
                fqdn='web', can_rsync_remote=False, is_analysis=False, rsyncusername='',
                rsynckeyfile='')
        self.webshare = rm.FileserverShare.objects.create(server=webserver,
                share=self.ssweb, path=os.path.join(self.rootdir, 'web'))

        # Create analysis for isoquant:
        self.ana = am.Analysis.objects.create(user=self.user, name='testana_iso', storage_dir='testdir_iso')
        self.dsa = am.DatasetAnalysis.objects.create(analysis=self.ana, dataset=self.ds)
        self.anajob = jm.Job.objects.create(funcname='testjob', kwargs={'fserver_id': self.anaserver.pk}, state=jj.Jobstates.WAITING,
                timestamp=timezone.now())
        self.nfs = am.NextflowSearch.objects.create(analysis=self.ana, nfwfversionparamset=self.nfwf,
                workflow=self.wf, token='tok123', job=self.anajob)
        am.AnalysisParam.objects.create(analysis=self.ana, param=self.param1, value=True)
        self.anamcparam = am.AnalysisParam.objects.create(analysis=self.ana, param=self.param2,
                value=[self.popt1.pk])
        self.ananormparam = am.AnalysisParam.objects.create(analysis=self.ana,
                param=self.param3, value=3)
        self.anaselectparam = am.AnalysisParam.objects.create(analysis=self.ana,
                param=self.param4, value=self.popt4.pk)
        self.anamfparam = am.AnalysisFileParam.objects.create(analysis=self.ana,
                param=self.pfn1, sfile=self.tmpsf)
        self.anafparam = am.AnalysisFileParam.objects.create(analysis=self.ana,
                param=self.pfn2, sfile=self.txtsf)
        self.resultfn = am.AnalysisResultFile.objects.create(analysis=self.ana,
                sfile=self.anasfile)

        # Create analysis for LF
        self.analf = am.Analysis.objects.create(user=self.user, name='testana_lf', storage_dir='testdirlf')
        self.dsalf = am.DatasetAnalysis.objects.create(analysis=self.analf, dataset=self.oldds)
        self.anajoblf = jm.Job.objects.create(funcname='testjob', kwargs={}, state=jj.Jobstates.WAITING,
                timestamp=timezone.now())
        self.nfslf = am.NextflowSearch.objects.create(analysis=self.analf, nfwfversionparamset=self.nfwf,
                workflow=self.wf, token='tok12344', job=self.anajoblf)

        am.AnalysisParam.objects.create(analysis=self.analf, param=self.param1, value=True)
        self.anamcparamlf = am.AnalysisParam.objects.create(analysis=self.analf, param=self.param2,
                value=[self.popt1.pk])
        self.ananormparamlf = am.AnalysisParam.objects.create(analysis=self.analf,
                param=self.param3, value=3)
        self.anaselectparamlf = am.AnalysisParam.objects.create(analysis=self.analf,
                param=self.param4, value=self.popt3.pk)
        self.anamfparamlf = am.AnalysisFileParam.objects.create(analysis=self.analf,
                param=self.pfn1, sfile=self.tmpsf)
        self.anafparamlf = am.AnalysisFileParam.objects.create(analysis=self.analf,
                param=self.pfn2, sfile=self.txtsf)
        self.resultfnlf = am.AnalysisResultFile.objects.create(analysis=self.analf,
                sfile=self.anasfile2)

        self.anaset = am.AnalysisSetname.objects.create(analysis=self.ana, setname='set1')
        self.ads1 = am.AnalysisDatasetSetValue.objects.create(analysis=self.ana,
                dataset=self.ds, setname=self.anaset, field='__regex', value='hej')
        self.adsif = am.AnalysisDSInputFile.objects.create(sfile=self.f3sfmz, analysisset=self.anaset,
                dsanalysis=self.dsa)
        self.isoqvals = {'denoms': {self.qch.pk: True}, 'sweep': False, 'report_intensity': False, 'remove': {}}
        am.AnalysisIsoquant.objects.create(analysis=self.ana, setname=self.anaset,
                value=self.isoqvals)
        self.samples = am.AnalysisSampletable.objects.create(analysis=self.ana,
                samples=[[self.qch.name, self.anaset.setname, self.projsam1.sample, 'thegroup']])


class AnalysisLabelfreeSamples(AnalysisPageTest):
    '''For preloaded LF analysis (base or new) we load file/sample annotations'''

    def setUp(self):
        super().setUp()
        self.afs2, _ = am.AnalysisFileValue.objects.get_or_create(analysis=self.analf,
                value='newname2', field='__sample', sfile=self.oldsf)


class TestNewAnalysis(BaseTest):
    url = '/analysis/new/'

    def test_ok(self):
        resp = self.cl.get(self.url, data={'dsids': self.ds.pk})
        self.assertEqual(resp.status_code, 200)

    def test_post(self):
        resp = self.cl.post(self.url)
        self.assertEqual(resp.status_code, 405)


class LoadBaseAnaTestIso(AnalysisPageTest):
    url = '/analysis/baseanalysis/load/'

    def setUp(self):
        super().setUp()
        # create "added_results", i.e. a param in analysis which uses results from another ana
        self.newana = am.Analysis.objects.create(user=self.user, name='testana_iso', storage_dir='testdir_iso')
        job = jm.Job.objects.create(funcname='testjob', kwargs={}, state=jj.Jobstates.WAITING,
                timestamp=timezone.now())
        am.NextflowSearch.objects.create(analysis=self.newana, nfwfversionparamset=self.nfwf,
                workflow=self.wf, token='addedres_ana', job=job)
        raw = rm.RawFile.objects.create(name='new_ana_file', producer=self.anaprod,
                source_md5='added_ana.result', size=100, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.ANALYSIS)
        self.newanasfile = rm.StoredFile.objects.create(rawfile=raw, filetype_id=self.ft.id,
                filename=raw.name, md5=raw.source_md5)
        rm.StoredFileLoc.objects.create(sfile=self.newanasfile, servershare=self.sstmp, path='',
                purged=False, active=True)
        new_resultfn = am.AnalysisResultFile.objects.create(analysis=self.newana,
                sfile=self.newanasfile)
        self.anafparam.sfile = self.newanasfile
        self.anafparam.save()

    def test_diff_dsets(self):
        '''Base analysis requested has a single dataset connected, this one asks for two, so we 
        need to get resultfiles from the base analysis as they will not be included in the 
        dropdowns already (any resultfile from an analysis with identical dsets as input will be)'''
        url = f'{self.url}{self.nfwf.pk}/{self.ana.pk}/'
        resp = self.cl.get(url, data={'dsids': f'{self.ds.pk},{self.oldds.pk}', 'added_ana_ids': ''})
        self.assertEqual(resp.status_code, 200)
        rj = resp.json()
        newana_date = datetime.strftime(self.newana.date, '%Y-%m-%d')
        newana_name = f'STD_{self.newana.name}'
        checkjson = {'base_analysis': {'analysis_id': self.ana.pk, 'dsets_identical': False,
                'flags': [self.param1.pk],
                'multicheck': [f'{self.param2.pk}___{self.anamcparam.value[0]}'],
                'inputparams': {f'{self.param3.pk}': self.ananormparam.value,
                    f'{self.param4.pk}': f'{self.anaselectparam.value}'},
                'multifileparams': {f'{self.pfn1.pk}': {'0': self.tmpsf.pk}},
                'fileparams': {f'{self.pfn2.pk}': self.newanasfile.pk},
                'isoquants': {self.ads1.setname.setname: {**self.isoqvals,
                    'chemistry': self.ds.quantdataset.quanttype.shortname,
                    'channels': {self.qch.name: [self.projsam1.sample, self.qch.pk]},
                    'samplegroups': {self.samples.samples[0][0]: self.samples.samples[0][3]}}},
                'added_results': {self.newana.pk: {'analysisname': newana_name,
                    'date': newana_date, 'fns': [{'id': x.sfile.pk, 'fn': x.sfile.filename,
                        'ana': newana_name, 'date': newana_date}
                        for x in self.newana.analysisresultfile_set.all()]}
                    },
                },
                'resultfiles': [{'id': self.resultfn.sfile.pk, 'fn': self.resultfn.sfile.filename,
                    'ana': f'{self.wftype.name}_{self.ana.name}',
                    'date': datetime.strftime(self.ana.date, '%Y-%m-%d')}],
                'datasets': {f'{self.ds.pk}': {'fields': {'__regex': f'{self.ads1.value}'},
                    'setname': f'{self.ads1.setname.setname}', 'allfilessamesample': True,
                    'files': {}, 'picked_ftype': f'mzML (pwiz {self.f3sfmz.mzmlfile.pwiz.version_description})'}},
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), json.dumps(checkjson))

    def test_same_dsets(self):
        '''Base analysis requested has a single dset connected, this analysis too, so we need
        output which has no base analysis resultfiles as they will already be loaded as part
        of the other same analysis
        '''
        url = f'{self.url}{self.nfwf.pk}/{self.ana.pk}/'
        resp = self.cl.get(url, data={'dsids': self.ds.pk, 'added_ana_ids': ''})
        self.assertEqual(resp.status_code, 200)
        rj = resp.json()
        newana_date = datetime.strftime(self.newana.date, '%Y-%m-%d')
        newana_name = f'STD_{self.newana.name}'
        checkjson = {'base_analysis': {'analysis_id': self.ana.pk, 'dsets_identical': True,
                'flags': [self.param1.pk],
                'multicheck': [f'{self.param2.pk}___{self.anamcparam.value[0]}'],
                'inputparams': {f'{self.param3.pk}': self.ananormparam.value,
                    f'{self.param4.pk}': f'{self.anaselectparam.value}'},
                'multifileparams': {f'{self.pfn1.pk}': {'0': self.tmpsf.pk}},
                'fileparams': {f'{self.pfn2.pk}': self.newanasfile.pk},
                'isoquants': {self.ads1.setname.setname: {**self.isoqvals,
                    'chemistry': self.ds.quantdataset.quanttype.shortname,
                    'channels': {self.qch.name: [self.projsam1.sample, self.qch.pk]},
                    'samplegroups': {self.samples.samples[0][0]: self.samples.samples[0][3]}}},
                'added_results': {self.newana.pk: {'analysisname': newana_name,
                    'date': newana_date, 'fns': [{'id': x.sfile.pk, 'fn': x.sfile.filename,
                        'ana': newana_name, 'date': newana_date}
                        for x in self.newana.analysisresultfile_set.all()]}
                    },
                },
                'resultfiles': [],
                'datasets': {f'{self.ds.pk}': {'fields': {'__regex': f'{self.ads1.value}'},
                    'setname': f'{self.ads1.setname.setname}',
                    'picked_ftype': f'mzML (pwiz {self.f3sfmz.mzmlfile.pwiz.version_description})',
                    'allfilessamesample': True, 'files': {}},
                    },
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), json.dumps(checkjson))

    def test_no_params_or_post(self):
        url = f'{self.url}1/1/'
        resp = self.cl.get(url)
        self.assertEqual(resp.status_code, 400)
        resp = self.cl.post(url)
        self.assertEqual(resp.status_code, 405)


class LoadBaseAnaTestLF(AnalysisLabelfreeSamples):
    url = '/analysis/baseanalysis/load/'

    def test_diff_dsets_no_mzmlfile(self):
        '''Base analysis has a single dset attached, this one has two, so we will
        not have dsets_identical and thus we will deliver resultfiles
        '''
        url = f'{self.url}{self.nfwf.pk}/{self.analf.pk}/'
        resp = self.cl.get(url, data={'dsids': f'{self.oldds.pk},{self.ds.pk}', 'added_ana_ids': ''})
        self.assertEqual(resp.status_code, 200)
        rj = resp.json()
        checkjson = {'base_analysis': {'analysis_id': self.analf.pk, 'dsets_identical': False,
                'flags': [self.param1.pk],
                'multicheck': [f'{self.param2.pk}___{self.anamcparamlf.value[0]}'],
                'inputparams': {f'{self.param3.pk}': self.ananormparam.value,
                    f'{self.param4.pk}': f'{self.anaselectparamlf.value}'},
                'multifileparams': {f'{self.pfn1.pk}': {'0': self.tmpsf.pk}},
                'fileparams': {f'{self.pfn2.pk}': self.txtsf.pk},
                'isoquants': {},
                'added_results': {},
                },
                'resultfiles': [{'id': self.resultfnlf.sfile.pk, 'fn': self.resultfnlf.sfile.filename,
                    'ana': f'{self.wftype.name}_{self.analf.name}',
                    'date': datetime.strftime(self.ana.date, '%Y-%m-%d')}],
                'datasets': {f'{self.oldds.pk}': {'allfilessamesample': False, 'fields': {},
                    'picked_ftype': self.afs2.sfile.filetype.name,
                    'files': {f'{self.afs2.sfile_id}': {'id': self.afs2.sfile_id,
                        'fields': {'__sample': self.afs2.value}}}},
                    },
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), json.dumps(checkjson))


class TestGetAnalysis(AnalysisPageTest):
    url = '/analysis/'

    # FIXME load_base and get_analysis do the same serialization on the inputs I think,
    # maybe it's worth centralizing that function
    def test_no_params_or_post(self):
        url = f'{self.url}1abc/'
        resp = self.cl.get(url)
        self.assertEqual(resp.status_code, 404)
        url = f'{self.url}1/'
        resp = self.cl.post(url)
        self.assertEqual(resp.status_code, 405)
        
#        # Non-existing dataset FIXME
#        maxds = dm.Dataset.objects.aggregate(Max('pk'))['pk__max']
#        resp = self.cl.get(f'{self.url}', data={'dsids': f'{maxds + 10}', 'anid': 0})
#        self.assertEqual(resp.status_code, 200)
#        self.assertEqual(resp.json()['errmsg'], ['Some datasets could not be found, they may not exist'])

    def test_ok(self):
        url = f'{self.url}{self.ana.pk}/'
        resp = self.cl.get(url)
        self.assertEqual(resp.status_code, 200)
        resphtml = resp.content.decode('utf-8')
        html_dsids = f'''<script>
        let existing_analysis = JSON.parse(document.getElementById('analysis_data').textContent);
        const dbwfs = JSON.parse(document.getElementById('allwfs').textContent);
        const allsecclasses = JSON.parse(document.getElementById('allsecclass').textContent);
        const initial_dsets = JSON.parse(document.getElementById('dsets').textContent);
        const allwfs = dbwfs.wfs;
        const wforder = dbwfs.order;
        const ds_errors = [


        ];
        </script>
        '''
        self.assertInHTML(html_dsids, resphtml)
        self.isoqvals = {'denoms': {self.qch.pk: True}, 'sweep': False, 'report_intensity': False, 'remove': {}}
        html_ana = f'''<script id="analysis_data" type="application/json">
        {{"analysis_id": {self.ana.pk}, "analysisname": "{self.ana.name}", "flags": [{self.param1.pk}], "multicheck": ["{self.param2.pk}___{self.anamcparam.value[0]}"], "inputparams": {{"{self.param3.pk}": {self.ananormparam.value}, "{self.param4.pk}": "{self.anaselectparam.value}"}}, "multifileparams": {{"{self.pfn1.pk}": {{"0": {self.tmpsf.pk}}}}}, "fileparams": {{"{self.pfn2.pk}": {self.txtsf.pk}}}, "isoquants": {{"{self.anaset.setname}": {{"chemistry": "{self.ds.quantdataset.quanttype.shortname}", "channels": {{"{self.qch.name}": ["{self.projsam1.sample}", {self.qch.pk}]}}, "samplegroups": {{"{self.samples.samples[0][0]}": "{self.samples.samples[0][3]}"}}, "denoms": {{"{self.qch.pk}": true}}, "remove": {{}}, "report_intensity": false, "sweep": false}}}}, "added_results": {{}}, "editable": true, "jobstate": "{self.anajob.state}", "external_desc": "", "wfversion_id": {self.nfwf.pk}, "wfid": {self.wf.pk}, "analysisserver_id": {self.anaserver.pk}, "external_results": false, "base_analysis": {{}}}}
        </script>
        '''
        self.assertInHTML(html_ana, resphtml)


class TestGetDatasetsBad(AnalysisPageTest):
    url = '/analysis/dsets/'

    def test_bad_req(self):
        resp = self.cl.post(f'{self.url}{self.nfwf.pk}/')
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(['Something wrong when asking datasets, contact admin'], resp.json()['errmsg'])

    def test_error_dset(self):
        '''This test is on single dataset which will fail, in various ways'''
        # No quant details
        fn = 'noqt_fn'
        path = 'noqt_stor'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod, source_md5='noqt_fakemd5',
                usetype=rm.UploadFileType.RAWFILE, size=100, date=timezone.now(), claimed=True)
        sf = rm.StoredFile.objects.create(rawfile=raw, filename=fn, md5=raw.source_md5, 
                filetype=self.ft, checked=True)
        rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssnewstore, path=path,
                purged=False, active=True)
        newrun = dm.RunName.objects.create(experiment=self.ds.runname.experiment, name='noqt_ds')
        newds = dm.Dataset.objects.create(runname=newrun, datatype=self.ds.datatype, 
                date=timezone.now(), securityclass=1)
        dm.DatasetServer.objects.create(dataset=newds, storage_loc=path,
                storageshare=self.dss.storageshare, startdate=timezone.now())
        dsr = dm.DatasetRawFile.objects.create(dataset=newds, rawfile=raw)
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/', data={'dsids': f'{newds.pk}', 'anid': 0})
        self.assertEqual(resp.status_code, 400)
        dsname = f'{self.ds.runname.experiment.project.name} / {self.ds.runname.experiment.name} / {newrun.name}'
        self.assertIn(f'File(s) or channels in dataset {dsname} do not have sample annotations, '
                'please edit the dataset first', resp.json()['errmsg'])
        self.assertNotIn('dsets', resp.json())

        # raw file without a storedfile and some more
        dsname = f'{self.ds.runname.experiment.project.name} / {self.ds.runname.experiment.name} / {self.ds.runname.name}'
        raw = rm.RawFile.objects.create(name='nosf', producer=self.prod, source_md5='nosf_fakemd5',
                usetype=rm.UploadFileType.RAWFILE, size=100, date=timezone.now(), claimed=True)
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=raw)
        raw2 = rm.RawFile.objects.create(name='nosf2', producer=self.prod, source_md5='nosf2_fakemd5',
                usetype=rm.UploadFileType.RAWFILE, size=100, date=timezone.now(), claimed=True)
        sf2 = rm.StoredFile.objects.create(rawfile=raw2, filename=raw2.name, md5=raw.source_md5,
                filetype=self.ft2, checked=True)
        rm.StoredFileLoc.objects.create(sfile=sf2, servershare=self.ssnewstore, path=path,
                purged=False, active=True)
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=raw2)
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/', data={'dsids': f'{self.ds.pk}', 'anid': 0})
        self.assertEqual(resp.status_code, 400)
        rj = resp.json()
        self.assertIn(f'Dataset {dsname} contains registered files that dont have a storage '
                'entry yet. Maybe the transferring hasnt been finished, or they are deleted.', rj['errmsg'])
        self.assertIn(f'Multiple different file types in dataset {dsname}, not allowed', rj['errmsg'])
        self.assertNotIn('dsets', rj)


class TestGetDatasetsIso(AnalysisPageTest):
    url = '/analysis/dsets/'

    def test_new_ok(self):
        '''New analysis with datasets, try both LF and isobaric'''
        # Isobaric
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/', data={'dsids': f'{self.ds.pk}', 'anid': 0})
        self.assertEqual(resp.status_code, 200)
        mztype = f'mzML (pwiz {self.pwiz.version_description})'
        checkjson = {
                'dsets': {f'{self.ds.pk}': {
                    'id': self.ds.pk,
                    'proj': self.ds.runname.experiment.project.name,
                    'exp': self.ds.runname.experiment.name,
                    'run': self.ds.runname.name,
                    'dtype': self.ds.datatype.name,
                    'secclass':  self.ds.securityclass.value,
                    'prefrac': False,
                    'hr': False,
                    'setname': '',
                    'locked': self.ds.locked,
                    'storage': f'{self.p1.name} - {self.exp1.name} - {self.dtype.name} - {self.run1.name}',
                    'fields': {'fake': '', '__regex': ''},
                    'instruments': [self.prod.name],
                    'instrument_types': [self.prod.shortname],
                    'qtype': {'name': self.ds.quantdataset.quanttype.name,
                        'short': self.ds.quantdataset.quanttype.shortname,
                        'is_isobaric': True},
                    'nrstoredfiles': [1, self.ft.name],
                    'channels': {self.qch.name: [self.projsam1.sample, self.qch.pk]},
                    'ft_files': {mztype: [{'ft_name': mztype, 'id': self.f3sfmz.pk, 'name': self.f3sfmz.filename, 'fr': '', 'dsetsample': '', 'fields': {'__sample': '', 'fake': ''}}],
                        self.ft.name: [{'ft_name': self.ft.name, 'id': self.f3sf.pk, 'name': self.f3sf.filename, 'fr': '', 'dsetsample': '', 'fields': {'__sample': '', 'fake': ''}}],
                        },
                    'incomplete_files': [],
                    'picked_ftype': mztype,
                    'allfilessamesample': True,
                    }},
                'field_order': self.inputdef.value[-1:],
                'error': False,
                'errmsg': [],
                'servers': [{'id': self.anaserver.pk, 'name': self.anaserver.name},
                    {'id': self.remoteanaserver.pk, 'name': self.remoteanaserver.name}
                    ]
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)

    def test_with_saved_analysis(self):
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/', data={'dsids': f'{self.ds.pk}', 'anid': self.ana.pk})
        self.assertEqual(resp.status_code, 200)
        mztype = f'mzML (pwiz {self.pwiz.version_description})'
        checkjson = {
                'dsets': {f'{self.ds.pk}': {
                    'id': self.ds.pk,
                    'proj': self.ds.runname.experiment.project.name,
                    'exp': self.ds.runname.experiment.name,
                    'run': self.ds.runname.name,
                    'dtype': self.ds.datatype.name,
                    'secclass': self.ds.securityclass.value,
                    'prefrac': False,
                    'hr': False,
                    'setname': self.ads1.setname.setname,
                    'locked': self.ds.locked,
                    'storage': f'{self.p1.name} - {self.exp1.name} - {self.dtype.name} - {self.run1.name}',
                    'fields': {'fake': '', '__regex': self.ads1.value},
                    'instruments': [self.prod.name],
                    'instrument_types': [self.prod.shortname],
                    'qtype': {'name': self.ds.quantdataset.quanttype.name,
                        'short': self.ds.quantdataset.quanttype.shortname,
                        'is_isobaric': True},
                    'nrstoredfiles': [1, self.ft.name],
                    'channels': {self.qch.name: [self.projsam1.sample, self.qch.pk]},
                    'ft_files': {mztype: [{'ft_name': mztype, 'id': self.f3sfmz.pk, 'name': self.f3sfmz.filename, 'fr': '', 'dsetsample': '', 'fields': {'__sample': '', 'fake': ''}}],
                        self.ft.name: [{'ft_name': self.ft.name, 'id': self.f3sf.pk, 'name': self.f3sf.filename, 'fr': '', 'dsetsample': '', 'fields': {'__sample': '', 'fake': ''}}],
                        },
                    'incomplete_files': [],
                    'picked_ftype': mztype,
                    'allfilessamesample': True,
                    }},
                'field_order': self.inputdef.value[-1:],
                'error': False,
                'errmsg': [],
                'servers': [{'id': self.anaserver.pk, 'name': self.anaserver.name},
                    {'id': self.remoteanaserver.pk, 'name': self.remoteanaserver.name}
                    ],
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)


class TestGetDatasetsLF(AnalysisLabelfreeSamples):
    url = '/analysis/dsets/'

    def test_new_ok(self):
        # self.oldds has max security so not all servers
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/', data={'dsids': f'{self.oldds.pk}', 'anid': 0})
        self.assertEqual(resp.status_code, 200)
        mztype = f'mzML (pwiz {self.pwiz.version_description})'
        checkjson = {
                'dsets': {f'{self.oldds.pk}': {
                    'id': self.oldds.pk,
                    'proj': self.oldds.runname.experiment.project.name,
                    'exp': self.oldds.runname.experiment.name,
                    'run': self.oldds.runname.name,
                    'dtype': self.oldds.datatype.name,
                    'secclass': self.oldds.securityclass.value,
                    'prefrac': False,
                    'hr': False,
                    'setname': '',
                    'locked': self.oldds.locked,
                    'storage': f'{self.oldp.name} - {self.oldexp.name} - {self.dtype.name} - {self.oldrun.name}',
                    'fields': {'fake': '', '__regex': ''},
                    'instruments': [self.prod.name],
                    'instrument_types': [self.prod.shortname],
                    'qtype': {'name': self.oldds.quantdataset.quanttype.name,
                        'short': self.oldds.quantdataset.quanttype.shortname,
                        'is_isobaric': False},
                    'nrstoredfiles': [1, self.ft.name],
                    'channels': False,
                    'ft_files': {self.ft.name: [{'ft_name': self.ft.name, 'id': self.oldsf.pk, 'name': self.oldsf.filename, 'fr': '', 'fields': {'__sample': self.oldqsf.projsample.sample, 'fake': ''}, 'dsetsample': self.oldqsf.projsample.sample}],
                        },
                    'incomplete_files': [],
                    'picked_ftype': self.ft.name,
                    'allfilessamesample': True,
                    }},
                'field_order': self.inputdef.value[-1:],
                'error': False,
                'errmsg': [],
                # TODO update this when personal data hits (remove anaserver)
                'servers': [
                    {'id': self.anaserver.pk, 'name': self.anaserver.name},
                    {'id': self.remoteanaserver.pk, 'name': self.remoteanaserver.name}]
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)


    def test_with_saved_analysis(self):
        # self.oldds has max security so not all servers
        resp = self.cl.get(f'{self.url}{self.nfwf.pk}/', data={'dsids': f'{self.oldds.pk}', 'anid': self.analf.pk})
        self.assertEqual(resp.status_code, 200)
        mztype = f'mzML (pwiz {self.pwiz.version_description})'
        checkjson = {
                'dsets': {f'{self.oldds.pk}': {
                    'id': self.oldds.pk,
                    'proj': self.oldds.runname.experiment.project.name,
                    'exp': self.oldds.runname.experiment.name,
                    'run': self.oldds.runname.name,
                    'dtype': self.oldds.datatype.name,
                    'secclass': self.oldds.securityclass.value,
                    'prefrac': False,
                    'hr': False,
                    'setname': '',
                    'locked': self.oldds.locked,
                    'storage': f'{self.oldp.name} - {self.oldexp.name} - {self.dtype.name} - {self.oldrun.name}',
                    'fields': {'fake': '', '__regex': ''},
                    'instruments': [self.prod.name],
                    'instrument_types': [self.prod.shortname],
                    'qtype': {'name': self.oldds.quantdataset.quanttype.name,
                        'short': self.oldds.quantdataset.quanttype.shortname,
                        'is_isobaric': False},
                    'nrstoredfiles': [1, self.ft.name],
                    'channels': False,
                    'ft_files': {self.ft.name: [{'ft_name': self.ft.name, 'id': self.oldsf.pk, 'name': self.oldsf.filename, 'fr': '', 'fields': {'__sample': self.afs2.value, 'fake': ''}, 'dsetsample': self.oldqsf.projsample.sample}],
                        },
                    'incomplete_files': [],
                    'picked_ftype': self.ft.name,
                    'allfilessamesample': False,
                    }},
                'field_order': self.inputdef.value[-1:],
                'error': False,
                'errmsg': [],
                # TODO when we enable personal/sensitive data, this test
                # will only return ONE server, uncomment the below line
                #'servers': [{'id': self.remoteanaserver.pk, 'name': self.remoteanaserver.name}],
                'servers': [
                    {'id': self.anaserver.pk, 'name': self.anaserver.name},
                    {'id': self.remoteanaserver.pk, 'name': self.remoteanaserver.name}]
                }
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)


class TestGetWorkflowVersionDetails(AnalysisPageTest):
    url = '/analysis/workflow/'

    def test_bad_req(self):
        resp = self.cl.post(self.url)
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'Something is wrong, contact admin')
        maxpk = am.NextflowWorkflowRepo.objects.aggregate(Max('pk'))['pk__max']
        resp = self.cl.get(self.url, data={'wfvid': maxpk + 10, 'dsids': f'{self.ds.pk}'})
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()['error'], 'Could not find workflow')

    def test_ok(self):
        # Add some usr file to make it show up in the dropdown, and make sure the other
        # userfile is not (wrong filetype)
        usrfraw_ft = rm.RawFile.objects.create(name='userfile_right_ft', producer=self.prod, 
                source_md5='usrf_rightft_md5', size=100, claimed=False, date=timezone.now(),
                usetype=rm.UploadFileType.USERFILE)
        sfusr_ft = rm.StoredFile.objects.create(rawfile=usrfraw_ft, md5=usrfraw_ft.source_md5,
                filetype=self.ft2, filename=usrfraw_ft.name, checked=True)
        rm.StoredFileLoc.objects.create(sfile=sfusr_ft, servershare=self.sstmp, path='',
                purged=False, active=True)
        utoken_ft = rm.UploadToken.objects.create(user=self.user, token='token_ft', expired=False,
                producer=self.prod, filetype=sfusr_ft.filetype, expires=timezone.now() + timedelta(1),
                uploadtype=rm.UploadFileType.USERFILE)
        userfile_ft = rm.UserFile.objects.create(sfile=sfusr_ft, description='This is a userfile',
                upload=utoken_ft)
        resp = self.cl.get(self.url, data={'wfvid': self.nfwf.pk, 'dsids': f'{self.ds.pk}'})
        self.assertEqual(resp.status_code, 200)
        allcomponents = {x.value: x for x in am.PsetComponent.ComponentChoices}
        checkjson = {'wf': {
            'components': {allcomponents[x.component].name: x.value 
                for x in  self.nfwf.paramset.psetcomponent_set.all()},
            'flags': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetparam_set.filter(param__ptype=am.Param.PTypes.FLAG)],
            'numparams': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetparam_set.filter(param__ptype=am.Param.PTypes.NUMBER)],
            'selectparams': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'opts': {f'{po.pk}': po.name for po in x.param.paramoption_set.all()},
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetparam_set.filter(param__ptype=am.Param.PTypes.SELECT)],
            'textparams': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetparam_set.filter(param__ptype=am.Param.PTypes.TEXT)],
            'multicheck': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'opts': {f'{po.pk}': po.name for po in x.param.paramoption_set.all()},
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetparam_set.filter(param__ptype=am.Param.PTypes.MULTI)],
            'fileparams': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'ftype': x.param.filetype_id, 'allow_resultfile': x.allow_resultfiles,
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetfileparam_set.all()],
            'multifileparams': [{'nf': x.param.nfparam, 'name': x.param.name, 'id': x.param.pk,
                'ftype': x.param.filetype_id, 'allow_resultfile': x.allow_resultfiles,
                'help': x.param.help or False}
                for x in self.nfwf.paramset.psetmultifileparam_set.all()],
            'libfiles': {f'{ft}': [{'id': x.sfile.id, 'desc': x.description,
                'name': x.sfile.filename} for x in [self.lf, userfile_ft]
                if x.sfile.filetype_id == ft]
                for ft in [self.lf.sfile.filetype_id, sfusr_ft.filetype_id]},
            'prev_resultfiles': [{'ana': f'{self.wftype.name}_{self.ana.name}',
                'date': datetime.strftime(self.ana.date, '%Y-%m-%d'), 'id': self.resultfn.sfile_id,
                'fn': self.resultfn.sfile.filename}],
            }}
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)


class TestStoreAnalysis(AnalysisPageTest):
    url = '/analysis/store/'

    def setUp(self):
        super().setUp()
        self.nfwf = am.NextflowWfVersionParamset.objects.create(update='nf workflow',
                commit='master', filename='nf.py', nfworkflow=self.nfw,
                paramset=self.pset, nfversion='', active=True)
        self.wftype = am.UserWorkflow.WFTypeChoices.STD
        self.wf = am.UserWorkflow.objects.create(name='testwf', wftype=self.wftype, public=True)
        self.wf.nfwfversionparamsets.add(self.nfwf)

    def test_new_analysis_and_run_and_purge(self):
        quant = self.ds.quantdataset.quanttype
        params = {'flags': {self.param1.pk: True}, 'inputparams': {self.param3.pk: 42}, 
                'multicheck': {self.param2.pk: [self.popt1.pk]}}
        postdata = {'dsids': [f'{self.ds.pk}'],
            'upload_external': False,
            'analysis_id': False,
            'infiles': {self.f3sfmz.pk: 1},
            'picked_ftypes': {self.ds.pk: f'mzML (pwiz {self.f3sfmz.mzmlfile.pwiz.version_description})'},
            'nfwfvid': self.nfwf.pk,
            'dssetnames': {self.ds.pk: 'setA'},
            'components': {'ISOQUANT_SAMPLETABLE': {'hello': 'yes'},
                'INPUTDEF': 'a',
                'ISOQUANT': {'setA': {'chemistry': quant.shortname,
                    'denoms': {quant.quanttypechannel_set.first().channel.name: True,
                        quant.quanttypechannel_set.last().channel.name: False},
                    'channels': {x.channel.name: [f'{x}_sample', x.channel.id] for x in quant.quanttypechannel_set.all()},
                    'report_intensity': False,
                    'sweep': False,
                    'remove': {quant.quanttypechannel_set.first().channel.name: True,
                        quant.quanttypechannel_set.last().channel.name: False},
                    }},
                },
            'analysisname': 'Test new analysis',
            # FIXME add some fields
            'fnfields': {self.f3sfmz.pk: {'fake': 'yes'}},
            'dsetfields': {f'{self.ds.pk}': {'__regex': 'fr_find', 'fake': 'hello'}},
            'params': params,
            'singlefiles': {self.pfn2.pk: self.sflib.pk},
            'multifiles': {self.pfn1.pk: [self.sfusr.pk]},
            # FIXME use self.ana here
            'base_analysis': {'isComplement': False,
                'dsets_identical': False,
                'selected': False,
                'typedname': '',
                'fetched': {},
                'resultfiles': [],
                },
            'wfid': self.wf.pk,
            'analysisserver_id': self.anaserver.pk,
            }
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        timestamp = datetime.strftime(datetime.now(), '%Y%m%d_')
        self.assertEqual(resp.status_code, 200)
        ana = am.Analysis.objects.last()
        self.assertEqual(ana.analysissampletable.samples, {'hello': 'yes'})
        regexes = {x.dataset_id: x.value for x in am.AnalysisDatasetSetValue.objects.filter(
            analysis=ana, field='__regex')}
        fakevals = {x.dataset_id: x.value for x in am.AnalysisDatasetSetValue.objects.filter(
            analysis=ana, field='fake')}
        for adsif in am.AnalysisDSInputFile.objects.filter(analysisset__analysis=ana):
            self.assertEqual(adsif.dsanalysis.dataset_id, self.ds.pk)
            self.assertEqual(adsif.analysisset.setname, postdata['dssetnames'][self.ds.pk])
            self.assertEqual(regexes[adsif.dsanalysis.dataset_id], postdata['dsetfields'][f'{self.ds.pk}']['__regex'])
            self.assertEqual(fakevals[adsif.dsanalysis.dataset_id], postdata['dsetfields'][f'{self.ds.pk}']['fake'])

        PT = am.Param.PTypes
        for ap in ana.analysisparam_set.all():
            pt = {PT.MULTI: 'multicheck', PT.TEXT: 'inputparams', PT.NUMBER: 'inputparams',
                    PT.FLAG: 'flags'}[ap.param.ptype]
            self.assertEqual(ap.value, params[pt][ap.param_id])
        self.assertEqual(ana.name, postdata['analysisname'])
        self.assertEqual(ana.storage_dir, ana.get_public_output_dir())
        checkjson = {'errmsg': False, 'multierror': [], 'analysis_id': ana.pk, 'token': False}
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)
        self.cl.post('/analysis/start/', content_type='application/json',
                data={'analysis_id': resp.json()['analysis_id']})
        self.run_job() # rsync extra files
        self.run_job() # run the analysis
        self.run_job() # rsync the results
        reports = rm.StoredFileLoc.objects.filter(sfile__filename='report.html', purged=False)
        self.assertEqual(reports.count(), 3) # analysisruns, web, analysis storage
        nfrunsfl = reports.filter(servershare=self.ssanaruns, path=ana.get_run_base_dir()).get()
        nfrunfn = os.path.join(self.nfrunshare.path, nfrunsfl.path, 'report.html')
        self.assertTrue(os.path.exists(nfrunfn))

        anasfl = reports.filter(servershare=self.ssana, path=ana.storage_dir).get()
        anadir = os.path.join(self.anashare.path, anasfl.path)
        anafn = os.path.join(anadir, 'report.html')
        self.assertTrue(os.path.exists(anafn))

        websfl = reports.filter(servershare=self.ssweb, path=ana.get_run_base_dir()).get()
        webdir = os.path.join(self.webshare.path, websfl.path)
        webfn = os.path.join(webdir, 'report.html')
        self.assertTrue(os.path.exists(webfn))

        self.run_job() # Queue backup (no task for it)
        j = jm.Job.objects.last()
        self.assertEqual(j.kwargs, {'sfloc_id': anasfl.pk, 'isdir': False})

        # Now purge
        j.state = jj.Jobstates.DONE
        j.save()
        self.user.is_staff = True
        self.user.save()
        resp = self.cl.post('/analysis/delete/', content_type='application/json', data={'item_id': ana.pk})
        resp = self.cl.post('/analysis/purge/', content_type='application/json', data={'item_id': ana.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(os.path.exists(webfn))
        self.assertTrue(os.path.exists(anafn))
        self.assertTrue(os.path.exists(nfrunfn))
        # Two purge jobs and two delete dir jobs, this test is getting slow
        self.run_job()
        self.run_job()
        self.run_job()
        self.run_job()
        self.assertFalse(os.path.exists(anafn))
        self.assertFalse(os.path.exists(nfrunfn))


class TestStoreExistingIsoAnalysis(AnalysisPageTest):
    url = '/analysis/store/'

    def test_existing_analysis(self):
        quant = self.ds.quantdataset.quanttype
        params = {'flags': {self.param1.pk: True},
                # str(self.popt4.pk) is because it is a key in a select param (value is its name)
                # and in JSON keys are not ints
                'inputparams': {self.param3.pk: 42, self.param4.pk: str(self.popt4.pk)},
                # str(self.popt1.pk) is because it is multiparam which is passed as string
                # {param}___{value} in the frontend
                'multicheck': {self.param2.pk: [str(self.popt1.pk)]}}
        postdata = {'dsids': [f'{self.ds.pk}'],
            'upload_external': False,
            'analysis_id': self.ana.pk,
            'infiles': {self.f3sfmz.pk: 1},
            'picked_ftypes': {self.ds.pk: f'mzML (pwiz {self.f3sfmz.mzmlfile.pwiz.version_description})'},
            'nfwfvid': self.nfwf.pk,
            'dssetnames': {self.ds.pk: 'setA'},
            'components': {'ISOQUANT_SAMPLETABLE': [[self.qch.name, 'setA', 'samplename', 'groupname']],
                'INPUTDEF': 'a',
                'ISOQUANT': {'setA': {'chemistry': quant.shortname,
                    'denoms': {x.channel.name: [f'{x}_sample', x.channel.id] for x in quant.quanttypechannel_set.all()},
                    'report_intensity': False,
                    'sweep': False,
                    }},
                },
            'analysisname': 'Test existing analysis',
            'fnfields': {},
            'dsetfields': {self.ds.pk: {'__regex': 'fr_find'}},
            'params': params,
            'singlefiles': {self.pfn2.pk: self.sflib.pk},
            'multifiles': {self.pfn1.pk: [self.sfusr.pk]},
            'base_analysis': {'isComplement': False,
                'dsets_identical': False,
                'selected': False,
                'typedname': '',
                'fetched': {},
                'resultfiles': [],
                },
            'wfid': self.wf.pk,
            'analysisserver_id': self.anaserver.pk,
            }
        prenow = datetime.now()
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        now = datetime.now()
        timeformat = '%Y%m%d_%H.%M'
        if prenow.second == 59 and (prenow + (now - prenow) / 2).second == 59:
            timestamp = datetime.strftime(prenow, timeformat)
        else:
            timestamp = datetime.strftime(now, timeformat)
        self.assertEqual(resp.status_code, 200)
        self.ana.refresh_from_db()
        self.assertEqual(self.ana.analysissampletable.samples, 
                [[self.qch.name, 'setA', 'samplename', 'groupname']])
        regexes = {x.dataset_id: x.value for x in am.AnalysisDatasetSetValue.objects.filter(
            analysis=self.ana, field='__regex')}
        for adsif in am.AnalysisDSInputFile.objects.filter(analysisset__analysis=self.ana):
            self.assertEqual(adsif.dsanalysis.dataset_id, self.ds.pk)
            self.assertEqual(adsif.analysisset.setname, postdata['dssetnames'][self.ds.pk])
            self.assertEqual(regexes[adsif.dsanalysis.dataset_id], postdata['dsetfields'][self.ds.pk]['__regex'])
        PT = am.Param.PTypes
        for ap in self.ana.analysisparam_set.all():
            pt = {PT.MULTI: 'multicheck', PT.TEXT: 'inputparams', PT.NUMBER: 'inputparams',
                    PT.FLAG: 'flags', PT.SELECT: 'inputparams'}[ap.param.ptype]
            if ap.param.ptype == PT.SELECT:
                self.assertEqual(ap.value, int(params[pt][ap.param_id]))
            elif ap.param.ptype ==  PT.MULTI:
                self.assertEqual(ap.value, [int(x) for x in params[pt][ap.param_id]])
            else:
                self.assertEqual(ap.value, params[pt][ap.param_id])
        self.assertEqual(self.ana.name, postdata['analysisname'])
        fullname = f'{self.ana.pk}_{self.wftype.name}_{self.ana.name}_{timestamp}'
        # This test flakes if executed right at midnight due to timestamp in assert string
        storedir =  f'{self.ana.user.username}/{fullname}'.replace(' ', '_')
        self.assertEqual(self.ana.storage_dir, storedir)
        self.anajob.refresh_from_db()
        c_ch = am.PsetComponent.ComponentChoices
        self.maxDiff = 50000
        job_check_kwargs = {'analysis_id': self.ana.pk,
          'filefields': {},
          'filesamples': {self.f3sfmz.pk: 'setA'},
          'infiles': {self.f3sfmz.pk: 1},
          'fserver_id': self.anaserver.pk,
          'dss_ids': [self.dss.pk],
          'sfloc_ids': [self.f3mzsss.pk],
          'inputs': {'components': {c_ch.INPUTDEF.name: self.inputdef.value,
              c_ch.PREFRAC.name: '.*fr([0-9]+).*mzML$', c_ch.ISOQUANT.name: {},
              c_ch.ISOQUANT_SAMPLETABLE.name: [[self.qch.name, 'setA', 'samplename', 'groupname']],
              },
              'multifiles': {self.pfn1.nfparam: [self.sfusr.pk]},
              'params': ['--isobaric', f'setA:{self.qt.shortname}:{self.qch.name}',
                  self.param2.nfparam, self.popt1.value, 
                  self.param1.nfparam, # flag so no value
                  self.param3.nfparam, '42',
                  self.param4.nfparam, self.popt4.value,
                  ],
              'singlefiles': {f'{self.pfn2.nfparam}': self.sflib.pk}},
              'platenames': {}, 'wfv_id': self.nfwf.pk}

        self.assertJSONEqual(json.dumps(self.anajob.kwargs), json.dumps(job_check_kwargs))
        checkjson = {'errmsg': False, 'multierror': [], 'analysis_id': self.ana.pk, 'token': False}
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)
        self.assertFalse(hasattr(self.ana, 'analysisbaseanalysis'))

        

class TestStoreAnalysisLF(AnalysisLabelfreeSamples):
    url = '/analysis/store/'

    def test_bad_req(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        resp = self.cl.post(self.url)
        self.assertEqual(resp.status_code, 400)
        resp = self.cl.post(self.url, content_type='application/json', data={})
        self.assertEqual(resp.status_code, 400)

    def test_files_not_on_analysis_server(self):
        c_ch = am.PsetComponent.ComponentChoices
        am.PsetComponent.objects.get_or_create(pset=self.pset,
                component=am.PsetComponent.ComponentChoices.COMPLEMENT_ANALYSIS)
        params = {'flags': {}, 'inputparams': {self.param3.pk: 42}, 
                'multicheck': {self.param2.pk: [self.popt2.pk]}}
        postdata = {'dsids': [f'{self.oldds.pk}'],
            'upload_external': False,
            'analysis_id': self.analf.pk,
            'infiles': {self.oldsf.pk: 23},
            'picked_ftypes': {self.oldds.pk: self.ft.name},
            'nfwfvid': self.nfwf.pk,
            'dssetnames': {},
            'components': {'ISOQUANT_SAMPLETABLE': False,
                'INPUTDEF': 'hej',
                'ISOQUANT': {},
                },
            'analysisname': 'Test existing analysis LF',
            'dsetfields': {f'{self.oldds.pk}': {'__regex': ''}},
            'fnfields': {self.oldsf.pk: {'__sample': 'testsample'}},
            'params': params,
            'singlefiles': {self.pfn2.pk: self.sflib.pk},
            'multifiles': {self.pfn1.pk: [self.sfusr.pk]},
            'base_analysis': {'isComplement': True,
                'runFromPSM': False,
                'dsets_identical': False,
                'selected': self.ana.pk,
                'typedname': '',
                'fetched': {},
                'resultfiles': [],
                },
            'wfid': self.wf.pk,
            'analysisserver_id': self.anaserver.pk,
            }
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        timestamp = datetime.strftime(datetime.now(), '%Y%m%d_')
        self.assertEqual(resp.status_code, 400)
        self.assertIn(f'Dataset {self.oldds.pk} does not have its files available to the '
                'selected analysis server', resp.json()['multierror'])

    def test_existing_analysis(self):
        c_ch = am.PsetComponent.ComponentChoices
        am.PsetComponent.objects.get_or_create(pset=self.pset,
                component=am.PsetComponent.ComponentChoices.COMPLEMENT_ANALYSIS)
        params = {'flags': {}, 'inputparams': {self.param3.pk: 42}, 
                'multicheck': {self.param2.pk: [self.popt2.pk]}}

        self.oldsss = rm.StoredFileLoc.objects.create(sfile=self.oldsf, servershare=self.ssnewstore,
                path=self.oldstorloc, active=True, purged=False)
        self.olddss = dm.DatasetServer.objects.create(dataset=self.oldds, storageshare=self.ssnewstore,
                storage_loc_ui=self.oldstorloc, storage_loc=self.oldstorloc, startdate=timezone.now())
        postdata = {'dsids': [f'{self.oldds.pk}'],
            'upload_external': False,
            'analysis_id': self.analf.pk,
            # Fake fraction nr
            'infiles': {self.oldsf.pk: 23},
            'picked_ftypes': {self.oldds.pk: self.ft.name},
            'nfwfvid': self.nfwf.pk,
            'dssetnames': {},
            'components': {'ISOQUANT_SAMPLETABLE': False,
                'INPUTDEF': 'hej',
                'ISOQUANT': {},
                },
            'analysisname': 'Test existing analysis LF',
            'dsetfields': {f'{self.oldds.pk}': {'__regex': ''}},
            'fnfields': {self.oldsf.pk: {'__sample': 'testsample'}},
            'params': params,
            'singlefiles': {self.pfn2.pk: self.sflib.pk},
            'multifiles': {self.pfn1.pk: [self.sfusr.pk]},
            'base_analysis': {'isComplement': True,
                'runFromPSM': False,
                'dsets_identical': False,
                'selected': self.ana.pk,
                'typedname': '',
                'fetched': {},
                'resultfiles': [],
                },
            'wfid': self.wf.pk,
            'analysisserver_id': self.anaserver.pk,
            }
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        timestamp = datetime.strftime(datetime.now(), '%Y%m%d_')
        self.assertEqual(resp.status_code, 200)
        self.analf.refresh_from_db()
        self.assertFalse(hasattr(self.analf, 'analysissampletable'))
        self.assertEqual(am.AnalysisDSInputFile.objects.filter(analysisset__analysis=self.analf).count(), 0)
        for afs in self.analf.analysisfilevalue_set.all():
            self.assertEqual(postdata['fnfields'][afs.sfile_id]['__sample'], afs.value)
        PT = am.Param.PTypes
        for ap in self.analf.analysisparam_set.all():
            pt = {PT.MULTI: 'multicheck', PT.TEXT: 'inputparams', PT.NUMBER: 'inputparams',
                    PT.FLAG: 'flags'}[ap.param.ptype]
            self.assertEqual(ap.value, params[pt][ap.param_id])
        self.assertEqual(self.analf.analysisparam_set.filter(param=self.param1).count(), 0)
        self.assertEqual(self.analf.name, postdata['analysisname'])
        self.assertEqual(self.analf.analysisfileparam_set.filter(param=self.pfn2, sfile=self.sflib).count(), 1)
        self.assertEqual(self.analf.analysisfileparam_set.filter(param=self.pfn1, sfile=self.sfusr).count(), 1)
        self.assertEqual(self.analf.analysisfileparam_set.count(), 2)
        fullname = f'{self.analf.pk}_{self.wftype.name}_{self.analf.name}_{timestamp}'
        # This test flakes if executed right at midnight due to timestamp in assert string
        self.assertEqual(self.analf.storage_dir[:-5], f'{self.analf.user.username}/{fullname}'.replace(' ', '_'))
        checkjson = {'errmsg': False, 'multierror': [], 'analysis_id': self.analf.pk, 'token': False}
        self.assertJSONEqual(resp.content.decode('utf-8'), checkjson)
        ba = am.AnalysisBaseanalysis.objects.get(analysis=self.analf)
        self.assertEqual(ba.base_analysis_id, self.ana.pk)
        self.assertTrue(ba.is_complement)
        self.assertFalse(ba.rerun_from_psms)
        self.assertJSONEqual(json.dumps(ba.shadow_isoquants), json.dumps({self.anaset.setname: {
            **self.isoqvals,
            'chemistry': self.ds.quantdataset.quanttype.shortname,
            'channels': {self.qch.name: [self.projsam1.sample, self.qch.pk]},
            'samplegroups': {self.samples.samples[0][0]: self.samples.samples[0][3]},
            }}))
        self.assertJSONEqual(json.dumps(ba.shadow_dssetnames), json.dumps({
            self.ds.pk: {'setname': self.ads1.setname.setname, 'fields': {'__regex': self.ads1.value}}}))

    def test_failing(self):
        # no sample annotations
        # no stored files at all
        # raw but not sf stored
        fn = 'noqt_fn'
        path = 'noqt_stor'
        raw = rm.RawFile.objects.create(name=fn, producer=self.prod, source_md5='noqt_fakemd5',
                usetype=rm.UploadFileType.RAWFILE, size=100, date=timezone.now(), claimed=True)
        newrun = dm.RunName.objects.create(experiment=self.ds.runname.experiment, name='noqt_ds')
        newds = dm.Dataset.objects.create(runname=newrun, datatype=self.ds.datatype, 
                date=timezone.now(), locked=True, securityclass=1)
        dm.DatasetServer.objects.create(dataset=newds, storage_loc=path,
                storageshare=self.dss.storageshare, startdate=timezone.now())
        dsr = dm.DatasetRawFile.objects.create(dataset=newds, rawfile=raw)
        dm.DatasetOwner.objects.create(dataset=newds, user=self.user)
        params = {'flags': {}, 'inputparams': {self.param3.pk: 42}, 
                'multicheck': {self.param2.pk: [self.popt2.pk]}}
        picked_ft = self.ft.name
        postdata = {'dsids': [f'{newds.pk}'],
            'analysis_id': 0,
            'upload_external': False,
            'infiles': {},
            'picked_ftypes': {newds.pk: picked_ft},
            'nfwfvid': self.nfwf.pk,
            'dssetnames': {},
            'components': {'ISOQUANT_SAMPLETABLE': False,
                'INPUTDEF': 'hej',
                'ISOQUANT': {},
                },
            'analysisname': 'Test existing analysis LF',
            'fnfields': {},
            'dsetfields': {f'{newds.pk}': {'__regex': ''}},
            'params': params,
            'singlefiles': {self.pfn2.pk: self.sflib.pk},
            'multifiles': {self.pfn1.pk: [self.sfusr.pk]},
            'base_analysis': {'isComplement': True,
                'runFromPSM': False,
                'dsets_identical': False,
                'selected': self.ana.pk,
                'typedname': '',
                'fetched': {},
                'resultfiles': [],
                },
            'wfid': self.wf.pk,
            'analysisserver_id': self.anaserver.pk,
            }
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 400)
        dsname = f'{self.ds.runname.experiment.project.name} / {self.ds.runname.experiment.name} / {newrun.name}'
        err = resp.json()['multierror']
        self.assertIn(f'File(s) or channels in dataset {dsname} do not have sample annotations, '
                'please edit the dataset first', err)
        self.assertIn(f'No stored files exist for dataset {dsname}', err)
        self.assertIn(f'Dataset {dsname} contains registered files that dont have a storage '
                'entry yet. Maybe the transferring hasnt been finished, or they are deleted.', err)

        # picked files fewer than raw files
        sf = rm.StoredFile.objects.create(rawfile=raw, filename=raw.name, md5=raw.source_md5, 
                filetype=self.ft, checked=True)
        rm.StoredFileLoc.objects.create(sfile=sf, servershare=self.ssnewstore, path=path,
                purged=False, active=True)
        raw2 = rm.RawFile.objects.create(name='second_rawfn', producer=self.prod,
                source_md5='fewer_fakemd5', size=100, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        dsr = dm.DatasetRawFile.objects.create(dataset=newds, rawfile=raw2)
        sf2 = rm.StoredFile.objects.create(rawfile=raw2, filename=raw2.name, md5=raw2.source_md5, 
                filetype=self.ft, checked=True)
        rm.StoredFileLoc.objects.create(sfile=sf2, servershare=self.ssnewstore, path=path,
                purged=False, active=True)
        sfmz = rm.StoredFile.objects.create(rawfile=raw2, filename=f'{fn}_mzml', md5='raw2mzml',
                filetype=self.ft, checked=True)
        rm.StoredFileLoc.objects.create(sfile=sfmz, servershare=self.ssnewstore, path=path,
                purged=False, active=True)
        mzml = am.MzmlFile.objects.create(sfile=sfmz, pwiz=self.pwiz)
        picked_ft = f'mzML (pwiz {mzml.pwiz.version_description})'
        postdata['picked_ftypes'] = {newds.pk: picked_ft}
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 400)
        dsname = f'{self.ds.runname.experiment.project.name} / {self.ds.runname.experiment.name} / {newrun.name}'
        err = resp.json()['multierror']
        self.assertIn(f'Files of type {picked_ft} are fewer than '
                f'raw files, please fix - for dataset {dsname}', err)

        # multi type dset
        sf2.filetype = self.ft2
        sf2.save()
        sfmz.delete()
        picked_ft = self.ft.name
        postdata['picked_ftypes'] = {newds.pk: picked_ft}
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 400)
        dsname = f'{self.ds.runname.experiment.project.name} / {self.ds.runname.experiment.name} / {newrun.name}'
        err = resp.json()['multierror']
        self.assertIn(f'Files of multiple datatypes exist in dataset {dsname}', err)

        # already running
        postdata['analysis_id'] = self.analf.pk
        self.anajoblf.state = jj.Jobstates.PROCESSING
        self.anajoblf.save()
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.json(), {'multierror': [], 'errmsg': 'This analysis has a running or queued job, '
            'it cannot be edited, please stop the job first'})
        self.assertEqual(resp.status_code, 403)

        # Already done
        self.anajoblf.state = jj.Jobstates.DONE
        self.anajoblf.save()
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {'multierror': [], 'errmsg': 'This analysis has already finished running, '
            'it cannot be edited'})

        # No permission
        newuser = User.objects.create(username='noaccess', email='noemail')
        self.ana.user=newuser
        self.ana.save()
        postdata['analysis_id'] = self.ana.pk
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {'multierror': [], 'errmsg': 'You do not have permission to edit this analysis'})
        
        # Locked dataset
        newds.locked = False
        newds.save()
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {'multierror': [], 'errmsg': 'Only locked datasets can be analyzed'})


class TestDeleteAnalysis(BaseTest):
    url = '/analysis/delete/'

    def setUp(self):
        super().setUp()
        self.ana = am.Analysis.objects.create(user=self.user, name='test', storage_dir='testdir')

    def test_fail_request(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # wrong dict keys
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)

        # No analysis
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.ana.pk + 1000})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Analysis does not exist', resp.json()['error'])

        # Not allowed wrong user
        user = User.objects.create(username='wrong', email='wrong2')
        self.user.is_staff = False
        self.user.save()
        self.ana.user = user
        self.ana.save()
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.ana.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('User is not authorized to delete', resp.json()['error'])
        
        # analysis already deleted
        self.ana.deleted = True
        self.ana.save()
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.ana.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Analysis is already deleted', resp.json()['error'])
        
    def test_delete(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.ana.pk})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(self.ana.deleted)
        self.ana.refresh_from_db()
        self.assertTrue(self.ana.deleted)
        # FIXME test job revoked


class TestPurgeAnalysis(BaseIntegrationTest):
    url = '/analysis/purge/'

    def setUp(self):
        super().setUp()
        self.ana = am.Analysis.objects.create(user=self.user, name='test', storage_dir='testdir', deleted=True)
        self.user.is_staff = True
        self.user.save()

    def test_fail_request(self):
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # wrong dict keys
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)

        # No analysis
        resp = self.cl.post(self.url, content_type='application/json',
                data={'item_id': self.ana.pk + 1000})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Analysis does not exist', resp.json()['error'])

        # Not allowed wrong user
        self.user.is_staff = False
        self.user.save()
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.ana.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Only admin is authorized to purge analysis', resp.json()['error'])
        
        # analysis not deleted
        self.user.is_staff = True
        self.user.save()
        self.ana.deleted = False
        self.ana.save()
        resp = self.cl.post(self.url, content_type='application/json', data={'item_id': self.ana.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Analysis is not deleted', resp.json()['error'])
