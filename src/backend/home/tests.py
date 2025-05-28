import os
from shutil import copytree
from django.utils import timezone

from kantele import settings
from kantele.tests import BaseTest, BaseIntegrationTest
from analysis import models as am
from rawstatus import models as rm
from datasets import models as dm
from jobs import models as jm
from jobs.jobs import Jobstates


class MzmlTests(BaseIntegrationTest):
    def setUp(self):
        super().setUp()
        self.ft = rm.StoredFileType.objects.create(name='Thermo raw', filetype='raw')
        self.prodqe = rm.Producer.objects.create(name='qe_prod', client_id='qe_abcdefg',
                shortname='qep1')
        self.prodtims = rm.Producer.objects.create(name='tims_prod', client_id='hijklm',
                shortname='p2')
        self.tims = rm.MSInstrumentType.objects.create(name='timstof')
        self.qe = rm.MSInstrumentType.objects.create(name='qe')
        instqe = rm.MSInstrument.objects.create(producer=self.prodqe,
                instrumenttype=self.qe, filetype=self.ft)
        insttims = rm.MSInstrument.objects.create(producer=self.prodtims,
                instrumenttype=self.tims, filetype=self.ft)
        self.refinewf = am.NextflowWfVersionParamset.objects.create(update='refine wf',
                commit='master', filename='refine.py', nfworkflow=self.nfw, paramset=self.pset, 
                nfversion='', active=True)
        wf = am.UserWorkflow.objects.create(name='refine', wftype=am.UserWorkflow.WFTypeChoices.SPEC,
                public=False)
        wf.nfwfversionparamsets.add(self.refinewf)


class TestCreateMzmls(MzmlTests):
    url = '/createmzml/'

    def test_fail_requests(self):
        # GET
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # wrong  keys
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)
        # dset does not exist
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': 10000})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('does not exist or is deleted', resp.json()['error'])
        # dset with diff raw files
        timsraw = rm.RawFile.objects.create(name='file2', producer=self.prodtims,
                source_md5='timsmd4', size=100, date=timezone.now(), claimed=True)
        timsdsr = dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=timsraw)
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('contains data from multiple instrument types', resp.json()['error'])
        timsdsr.delete()
    
    def test_existing_mzmls(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('already has existing mzML files of that proteowizard', resp.json()['error'])

    def test_other_pwiz_rsyncpull(self):
        '''Test if old pwiz existing files get purge job BEFORE launching new job, files are
        pulled from analysis to storage after w rsync'''
        oldpw = am.Proteowizard.objects.create(version_description='older', container_version='',
                nf_version=self.nfwv)
        self.f3mzml.pwiz = oldpw
        self.f3mzml.save()

        # Second dss
        rawfn = os.path.join(self.f3path, self.f3raw.name)
        dss2path = 'testdss2'
        dss2_fpath = os.path.join(settings.SHAREMAP[self.ssoldstorage.name], dss2path)
        os.makedirs(dss2_fpath)
        rawfn_dss2 = os.path.join(dss2_fpath, self.f3raw.name)
        copytree(rawfn, rawfn_dss2)
        dss2 = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.ssoldstorage,
                storage_loc_ui=dss2path, storage_loc=dss2path, startdate=timezone.now())
        rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.ssoldstorage,
                path=dss2path, active=True, purged=False)
        rm.StoredFileLoc.objects.create(sfile=self.f3sfmz, servershare=self.ssoldstorage,
                path=dss2path, active=True, purged=False)

        self.storagecontroller.can_rsync, self.remoteserver.can_rsync = False, True
        self.storagecontroller.save(), self.remoteserver.save()
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        purgej = jm.Job.objects.filter(funcname='remove_dset_files_servershare').last()
        j = jm.Job.objects.filter(funcname='convert_dataset_mzml').last()
        self.assertEqual(j.state, Jobstates.PENDING)
        exp_kw  = {'options': [], 'filters': ['"peakPicking true 2"', '"precursorRefine"'], 
                'dss_id': self.dss.pk, 'pwiz_id': self.pwiz.pk,
                'sfloc_ids': [x['pk'] for x in rm.StoredFileLoc.objects.filter(
                    servershare=self.ssnewstore, # FIXME this gets picked randomly in home/views!
                    sfile__rawfile__datasetrawfile__dataset=self.ds, sfile__mzmlfile__isnull=True,
                    ).values('pk')]}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}.mzML'
        mzmlfn1 = os.path.join(self.f3path, mzmlfn)
        mzmlfn2 = os.path.join(dss2_fpath, mzmlfn)
        mzml_sf = rm.StoredFile.objects.get(rawfile=self.f3raw, mzmlfile__pwiz=self.pwiz.pk)
        oldmzml_q = rm.StoredFileLoc.objects.filter(pk=self.f3mzsss.pk, active=False)
        self.assertTrue(oldmzml_q.filter(purged=False).exists())
        mzml1_q = rm.StoredFileLoc.objects.filter(sfile=mzml_sf, path=self.dss.storage_loc, active=True)
        mzml2_q = rm.StoredFileLoc.objects.filter(sfile=mzml_sf, path=dss2.storage_loc, active=True)
        self.assertTrue(mzml1_q.filter(purged=True).exists())
        self.assertTrue(mzml2_q.filter(purged=True).exists())
        with open(mzmlfn1, 'w') as fp:
            pass
        with open(mzmlfn2, 'w') as fp:
            pass
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertTrue(os.path.exists(mzmlfn2))
        self.run_job() # delete old mzml
        self.assertFalse(os.path.exists(mzmlfn1))
        self.assertTrue(oldmzml_q.filter(purged=True).exists())
        self.run_job() # delete old mzml (second dss)
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job() # convert mzml
        self.assertTrue(mzml1_q.filter(purged=False).exists())
        self.assertTrue(mzml2_q.filter(purged=True).exists())
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job() # running rsync
        self.assertTrue(mzml1_q.filter(purged=False).exists())
        self.assertTrue(mzml2_q.filter(purged=False).exists())
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        self.assertTrue(os.path.exists(mzmlfn2))

    def test_create_mzml_qe_rsyncpush(self):
        '''Run conversion and then push files to other server w rsync job'''
        rawfn = os.path.join(self.f3path, self.f3raw.name)
        dss2path = 'testdss2'
        dss2_fpath = os.path.join(settings.SHAREMAP[self.ssoldstorage.name], dss2path)
        os.makedirs(dss2_fpath)
        rawfn_dss2 = os.path.join(dss2_fpath, self.f3raw.name)
        copytree(rawfn, rawfn_dss2)
        dss2 = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.ssoldstorage,
                storage_loc_ui=dss2path, storage_loc=dss2path, startdate=timezone.now())
        rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.ssoldstorage,
                path=dss2path, active=True, purged=False)
        self.f3sfmz.delete()
        postdata = {'pwiz_id': self.pwiz.pk, 'dsid': self.ds.pk}
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 200)
        j = jm.Job.objects.first()
        self.assertEqual(j.funcname, 'convert_dataset_mzml')
        exp_kw  = {'options': [], 'filters': ['"peakPicking true 2"', '"precursorRefine"'], 
                'dss_id': self.dss.pk, 'pwiz_id': self.pwiz.pk,
                'sfloc_ids': [x['pk'] for x in rm.StoredFileLoc.objects.filter(
                    servershare=self.ssnewstore, # FIXME this gets picked randomly in home/views!
                    sfile__rawfile__datasetrawfile__dataset=self.ds, sfile__mzmlfile__isnull=True,
                    ).values('pk')]}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}.mzML'
        mzmlfn1 = os.path.join(self.f3path, mzmlfn)
        mzmlfn2 = os.path.join(dss2_fpath, mzmlfn)
        self.assertFalse(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job()
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job() # running rsync
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        self.assertTrue(os.path.exists(mzmlfn2))
        refineurl = '/refinemzml/'
        resp = self.cl.post(refineurl, content_type='application/json', data={'dsid': self.ds.pk,
            'dbid': self.sflib.pk, 'wfid': self.refinewf.pk})
        self.assertEqual(resp.status_code, 200)
        jq = jm.Job.objects.filter(funcname='refine_mzmls')
        self.assertTrue(jq.exists())
        j = jq.get()
        exp_kw = {'dss_id': self.dss.pk, 'wfv_id': self.refinewf.pk,
                'dbfn_id': self.sflib.pk, 
                #'dstshare_id': self.ssmzml.pk,
                'qtype': self.ds.quantdataset.quanttype.shortname}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}_refined.mzML'
        mzmlfn1 = os.path.join(self.f3path, mzmlfn)
        mzmlfn2 = os.path.join(dss2_fpath, mzmlfn)
        self.assertFalse(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job() # refining
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job() # running rsync
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        self.assertTrue(os.path.exists(mzmlfn2))

    def test_create_mzml_tims_rsync_sameserver(self):
        '''Two datasetserver locations (one on ssnewstore, one on sstmp), so rsync does not use
        SSH'''
        self.f3raw.producer = self.prodtims
        self.f3raw.save()
        rawfn = os.path.join(self.f3path, self.f3raw.name)
        dss2path = 'testdss2'
        dss2_fpath = os.path.join(settings.SHAREMAP[self.sstmp.name], dss2path)
        os.makedirs(dss2_fpath)
        rawfn_dss2 = os.path.join(dss2_fpath, self.f3raw.name)
        copytree(rawfn, rawfn_dss2)
        dss2 = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.sstmp,
                storage_loc_ui=dss2path, storage_loc=dss2path, startdate=timezone.now())
        rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.sstmp,
                path=dss2path, active=True, purged=False)
        self.f3sfmz.delete()
        postdata = {'pwiz_id': self.pwiz.pk, 'dsid': self.ds.pk}
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 200)
        j = jm.Job.objects.first()
        self.assertEqual(j.funcname, 'convert_dataset_mzml')
        exp_kw  = {'options': ['combineIonMobilitySpectra'], 'filters': ['"peakPicking true 2"', '"precursorRefine"', '"scanSumming precursorTol=0.02 scanTimeTol=10 ionMobilityTol=0.1"'], 
                'dss_id': self.dss.pk, 'pwiz_id': self.pwiz.pk,
                'sfloc_ids': [x['pk'] for x in rm.StoredFileLoc.objects.filter(
                    servershare=self.ssnewstore, # FIXME this gets picked randomly in home/views!
                    sfile__rawfile__datasetrawfile__dataset=self.ds, sfile__mzmlfile__isnull=True,
                    ).values('pk')]}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}.mzML'
        mzsfl_q_stor  = rm.StoredFileLoc.objects.filter(sfile__filename=mzmlfn,
                servershare=self.ssnewstore, path=self.dss.storage_loc, active=True)
        mzsfl_q_tmp = rm.StoredFileLoc.objects.filter(sfile__filename=mzmlfn, servershare=self.sstmp,
                path=dss2path, active=True)
        self.assertTrue(mzsfl_q_stor.filter(purged=True).exists())
        self.assertTrue(mzsfl_q_tmp.filter(purged=True).exists())
        mzmlfn1 = os.path.join(self.f3path, mzmlfn)
        mzmlfn2 = os.path.join(dss2_fpath, mzmlfn)
        self.assertFalse(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job()
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.assertTrue(mzsfl_q_stor.filter(purged=False).exists())
        self.assertTrue(mzsfl_q_tmp.filter(purged=True).exists())
        self.run_job() # running rsync
        self.assertTrue(mzsfl_q_tmp.filter(purged=False).exists())
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        t = j2.task_set.first()
        self.assertTrue(os.path.exists(mzmlfn2))


class TestRefineMzmls(MzmlTests):
    url = '/refinemzml/'

    def setUp(self):
        super().setUp()
        dbft = rm.StoredFileType.objects.create(name='database', filetype='tst', is_rawdata=False)
        dbraw = rm.RawFile.objects.create(name='db.fa', producer=self.prod, source_md5='db.famd5',
                size=100, date=timezone.now(), claimed=True)
        self.dbsf = rm.StoredFile.objects.create(rawfile=dbraw, filename=dbraw.name,
                    md5=dbraw.source_md5, filetype=dbft, checked=True)
        rm.StoredFileLoc.objects.create(sfile=self.dbsf, servershare=self.ssnewstore, path=self.storloc,
                purged=False, active=True)
        self.refinewf = am.NextflowWfVersionParamset.objects.create(update='refine wf',
                commit='refine ci', filename='refine.nf', nfworkflow=self.nfw, paramset=self.pset, 
                nfversion='', active=True)
        wf = am.UserWorkflow.objects.create(name='refine', wftype=am.UserWorkflow.WFTypeChoices.SPEC,
                public=False)
        wf.nfwfversionparamsets.add(self.refinewf)
        
    def test_fail_requests(self):
        # GET
        resp = self.cl.get(self.url)
        self.assertEqual(resp.status_code, 405)
        # wrong  keys
        resp = self.cl.post(self.url, content_type='application/json', data={'hello': 'test'})
        self.assertEqual(resp.status_code, 400)
        # dset does not exist
        resp = self.cl.post(self.url, content_type='application/json', data={'dsid': 10000})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('does not exist or is deleted', resp.json()['error'])
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=self.timsraw)
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pw.pk,
            'dsid': self.ds.pk, 'dbid': self.sflib.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('contains data from multiple instrument types', resp.json()['error'])

    def test_existing_mzmls_no_db(self):
        # no mzMLs exist yet
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pw.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Need to create normal mzMLs', resp.json()['error'])

        # Not passed a db
        am.MzmlFile.objects.create(sfile=self.qesf, pwiz=self.pw)
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pw.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual('Must pass a database to refine with', resp.json()['error'])

        # refined exists already
        refinedsf = rm.StoredFile.objects.create(rawfile=self.qeraw, filename=f'{self.qeraw.name}_refined',
                md5='refined_md5', checked=True, filetype=self.ft)
        rm.StoredFileLoc.objects.create(sfile=refinedsf, servershare=self.ds.storageshare, path=self.storloc,
                purged=False, active=True)
        am.MzmlFile.objects.create(sfile=refinedsf, pwiz=self.pw, refined=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 403)

    def do_refine(self, ds):
        resp = self.cl.post(self.url, content_type='application/json', data={'dsid': ds.pk,
            'dbid': self.dbsf.pk, 'wfid': self.refinewf.pk})
        self.assertEqual(resp.status_code, 200)
        j = jm.Job.objects.last()
        self.assertEqual(j.funcname, 'refine_mzmls')
        exp_kw = {'dset_id': self.ds.pk, 'wfv_id': self.refinewf.pk,
                'dbfn_id': self.dbsf.pk, 'dstshare_id': self.ssmzml.pk,
                'qtype': self.ds.quantdataset.quanttype.shortname}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)

    def test_refine_mzml(self):
        '''Do a refine, test its correctness, then make sure there is no dataset servershare
        move on the current dataset, unlike the test_with_filemove (old data to new share)'''
        am.MzmlFile.objects.create(sfile=self.qesf, pwiz=self.pw)
        self.do_refine(self.ds)
#        self.assertEqual(jm.Job.objects.filter(funcname='move_dset_servershare',
#            kwargs__dset_id=self.ds.pk).count(), 0)

#    def test_with_filemove(self):
#        # Create new dataset on old storage proj that can be mock-"moved"
#        moverun = dm.RunName.objects.create(name='test_with_filemove_loc', experiment=self.oldexp)
#        ds = self.ds
#        ds.storage_loc = moverun.name
#        ds.pk = None
#        ds.storageshare = self.ssoldstorage
#        ds.runname = moverun
#        ds.save()
#        dm.QuantDataset.objects.get_or_create(dataset=ds, quanttype=self.qt)
#        # Add raw files (pk=None, save -> copy original object)
#        qeraw = self.qeraw
#        qeraw.pk, qeraw.source_md5 = None, 'refine_test_with_filemove'
#        qeraw.save()
#        dm.DatasetRawFile.objects.create(rawfile=qeraw, dataset=ds)
#        qesf = self.qesf
#        qesf.pk, qesf.md5 = None, qeraw.source_md5
#        qesf.rawfile, qesf.filename = qeraw, qeraw.name
#        qesf.save()
#        rm.StoredFileLoc.objects.create(sfile=qesf, path=ds.storage_loc, servershare=ds.storageshare)
#        am.MzmlFile.objects.create(sfile=qesf, pwiz=self.pw)
#        self.do_refine(ds)
#        self.assertEqual(jm.Job.objects.filter(funcname='move_dset_servershare',
#            kwargs__dset_id=ds.pk).count(), 1)
