import os
from datetime import datetime
from shutil import copytree, copy
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
        self.prodtims = rm.Producer.objects.create(name='tims_prod', client_id='hijklm',
                shortname='p2')
        self.tims = rm.MSInstrumentType.objects.create(name='timstof')
        insttims = rm.MSInstrument.objects.create(producer=self.prodtims,
                instrumenttype=self.tims, filetype=self.ft)
        self.refinewf = am.NextflowWfVersionParamset.objects.create(update='refine wf',
                commit='master', filename='refine.py', nfworkflow=self.nfw, paramset=self.pset, 
                nfversion='', active=True)
        self.wf = am.UserWorkflow.objects.create(name='refine', wftype=am.UserWorkflow.WFTypeChoices.SPEC,
                public=False)
        self.wf.nfwfversionparamsets.add(self.refinewf)


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
        timsraw = rm.RawFile.objects.create(name='file2', producer=self.prodtims, source_md5='timsmd4', 
                size=100, date=timezone.now(), claimed=True, usetype=rm.UploadFileType.RAWFILE)
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

        # Create second raw file for dset
        f3raw2 = rm.RawFile.objects.create(name='f3raw2', producer=self.prod,
                source_md5='f3_fakemd5_2', size=100, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=f3raw2)
        f3sf2 = rm.StoredFile.objects.create(rawfile=f3raw2, filename=f3raw2.name,
                md5=f3raw2.source_md5, filetype=self.ft, checked=True)
        f3sss2 = rm.StoredFileLoc.objects.create(sfile=f3sf2, servershare=self.ssnewstore,
                path=self.storloc, active=True, purged=False)

        j_q = jm.Job.objects.filter(funcname='convert_dataset_mzml')
        self.assertFalse(j_q.exists())
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        j = j_q.get()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(j.state, Jobstates.PENDING)
        with open(os.path.join(self.f3path, f3raw2.name), 'w') as fp:
            pass
        exp_kw  = {'options': [], 'filters': ['precursorRefine'], 
                'dss_id': self.dss.pk, 'pwiz_id': self.pwiz.pk, 'sfloc_ids': [f3sss2.pk]}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(f3raw2.name)[0]}.mzML'
        mzmlfn1 = os.path.join(self.f3path, mzmlfn)
        mzml_sf = rm.StoredFile.objects.get(rawfile=f3raw2, mzmlfile__pwiz=self.pwiz.pk)
        mzml_q = rm.StoredFileLoc.objects.filter(sfile=mzml_sf, servershare=self.ssnewstore,
                active=True)
        self.assertTrue(mzml_q.filter(purged=True).exists())
        md5sum = f'{mzml_sf.filename} md5 file'
        self.assertNotEqual(mzml_sf.md5, md5sum)
        j = jm.Job.objects.first()
        self.run_job() # convert mzml
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertTrue(mzml_q.filter(purged=False).exists())
        mzml_sf.refresh_from_db()
        self.assertEqual(mzml_sf.md5, md5sum)
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')

    def test_other_pwiz_rsyncpull(self):
        '''Test if old pwiz existing files get purge job BEFORE launching new job, files are
        pulled from analysis to storage after w rsync'''

        # Remove non-remote analysis server, so results are copied:
        # 1. to rsyncable area (here: open ssnewstorage)
        # 2. to remote
        self.anaserver.delete()
        oldpw = am.Proteowizard.objects.create(version_description='older', nf_version=self.nfwv,
                params={'mzmltool': 'msconvert'})
        self.f3mzml.pwiz = oldpw
        self.f3mzml.save()

        # Second dss
        rawfn = os.path.join(self.f3path, self.f3raw.name)
        dss2path = 'testdss2'
        dss2_fpath = os.path.join(self.oldstorctrl.path, dss2path)
        os.makedirs(dss2_fpath)
        rawfn_dss2 = os.path.join(dss2_fpath, self.f3raw.name)
        copytree(rawfn, rawfn_dss2)
        dss2 = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.analocalstor,
                storage_loc_ui=dss2path, storage_loc=dss2path, startdate=timezone.now())
        rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.analocalstor,
                path=dss2path, active=True, purged=False)
        rm.StoredFileLoc.objects.create(sfile=self.f3sfmz, servershare=self.analocalstor,
                path=dss2path, active=True, purged=False)

        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 200)
        purgej = jm.Job.objects.filter(funcname='remove_dset_files_servershare').last()
        j = jm.Job.objects.filter(funcname='convert_dataset_mzml').last()
        self.assertEqual(j.state, Jobstates.PENDING)
        exp_kw  = {'options': [], 'filters': ['precursorRefine'], 
                'dss_id': dss2.pk, 'pwiz_id': self.pwiz.pk,
                'sfloc_ids': [x['pk'] for x in rm.StoredFileLoc.objects.filter(
                    servershare=self.analocalstor, # FIXME this gets picked randomly in home/views!
                    sfile__rawfile__datasetrawfile__dataset=self.ds, sfile__mzmlfile__isnull=True,
                    ).values('pk')]}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}.mzML'
        mzmlfn1 = os.path.join(dss2_fpath, mzmlfn)
        mzmlfn2 = os.path.join(self.f3path, mzmlfn)
        mzml_sf = rm.StoredFile.objects.get(rawfile=self.f3raw, mzmlfile__pwiz=self.pwiz.pk)
        md5sum = f'{mzml_sf.filename} md5 file'
        self.assertNotEqual(mzml_sf.md5, md5sum)
        oldmzml_q = rm.StoredFileLoc.objects.filter(pk=self.f3mzsss.pk, active=False)
        self.assertTrue(oldmzml_q.filter(purged=False).exists())
        mzml1_q = rm.StoredFileLoc.objects.filter(sfile=mzml_sf, servershare=self.analocalstor,
                active=True)
        mzml2_q = rm.StoredFileLoc.objects.filter(sfile=mzml_sf, servershare=self.ssnewstore,
                active=True)
        self.assertTrue(mzml1_q.filter(purged=True).exists())
        self.assertTrue(mzml2_q.filter(purged=True).exists())
        # Create old mzml files to delete
        with open(mzmlfn1, 'w') as fp:
            pass
        with open(mzmlfn2, 'w') as fp:
            pass
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertTrue(os.path.exists(mzmlfn2))
        j = jm.Job.objects.first()
        self.assertEqual(j.funcname, 'remove_dset_files_servershare')
        self.run_job() # delete old mzml
        self.assertFalse(os.path.exists(mzmlfn2))
        self.assertTrue(oldmzml_q.filter(purged=True).exists())
        self.run_job() # delete old mzml (second dss)
        self.assertFalse(os.path.exists(mzmlfn1))
        self.run_job() # convert mzml
        self.assertTrue(mzml1_q.filter(purged=False).exists())
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertFalse(mzml2_q.filter(purged=False).exists())
        mzml_sf.refresh_from_db()
        self.assertEqual(mzml_sf.md5, md5sum)
        self.run_job() # running rsync to newstorage
        self.assertTrue(mzml1_q.filter(purged=False).exists())
        self.assertTrue(mzml2_q.filter(purged=False).exists())
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        self.assertTrue(os.path.exists(mzmlfn2))

    def test_create_mzml_qe_rsyncpush_and_refine(self):
        '''Run conversion and then push files to other server w rsync job,
        newstorage is here not attached to storagecontroller,
        so need to push files first to inbox
        '''
        self.anaprofile.delete()
        self.newstorctrl.delete()

        rawfn = os.path.join(self.f3path, self.f3raw.name)
        dss2path = 'testdss2'
        dss2_fpath = os.path.join(self.oldstorctrl.path, dss2path)
        os.makedirs(dss2_fpath)
        rawfn_dss2 = os.path.join(dss2_fpath, self.f3raw.name)
        copytree(rawfn, rawfn_dss2)
        dss2 = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.analocalstor,
                storage_loc_ui=dss2path, storage_loc=dss2path, startdate=timezone.now())
        rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.analocalstor,
                path=dss2path, active=True, purged=False)

        self.f3sfmz.delete()
        postdata = {'pwiz_id': self.pwiz.pk, 'dsid': self.ds.pk}
        resp = self.cl.post(self.url, content_type='application/json', data=postdata)
        self.assertEqual(resp.status_code, 200)
        j = jm.Job.objects.first()
        self.assertEqual(j.funcname, 'convert_dataset_mzml')
        exp_kw  = {'options': [], 'filters': ['precursorRefine'], 
                'dss_id': dss2.pk, 'pwiz_id': self.pwiz.pk,
                'sfloc_ids': [x['pk'] for x in rm.StoredFileLoc.objects.filter(
                    servershare=self.analocalstor, # FIXME this gets picked randomly in home/views!
                    sfile__rawfile__datasetrawfile__dataset=self.ds, sfile__mzmlfile__isnull=True,
                    ).values('pk')]}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}.mzML'
        runpath = f'{self.ds.pk}_convert_mzml_{j.kwargs["timestamp"]}' 
        f3sfmz = rm.StoredFile.objects.get(rawfile=self.f3raw, mzmlfile__isnull=False)
        mzmlfn1 = os.path.join(self.rootdir, 'oldstorage', dss2path, mzmlfn)
        m1q = rm.StoredFileLoc.objects.filter(sfile=f3sfmz, servershare=self.analocalstor,
                path=dss2path, active=True, purged=False)
        mzmlfn2 = os.path.join(self.inboxctrl.path, 'datasets', f'{self.ds.pk}', mzmlfn)
        m2q = rm.StoredFileLoc.objects.filter(sfile=f3sfmz, servershare=self.ssinbox,
                path=f'datasets/{self.ds.pk}', active=True, purged=False)
        mzmlfn3 = os.path.join(self.f3path, mzmlfn)
        m3q = rm.StoredFileLoc.objects.filter(sfile=f3sfmz, servershare=self.ssnewstore,
                path=self.storloc, active=True, purged=False)
        self.assertFalse(m1q.filter(purged=True).exists())
        self.run_job() # run convert
        self.assertTrue(m1q.exists())
        self.assertFalse(m2q.exists())
        self.assertTrue(os.path.exists(mzmlfn1))
        self.run_job() # running rsync to accessible primary dset on open storage
        self.assertTrue(os.path.exists(mzmlfn2))
        self.assertTrue(m2q.exists())
        self.assertFalse(m3q.exists())
        self.assertFalse(os.path.exists(mzmlfn3))
        self.run_job() # rsync to second dset from primary controller
        self.assertTrue(os.path.exists(mzmlfn3))
        self.assertTrue(m3q.exists())
        self.run_job() # set to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')

        # Now do refine on produced file:
        anasflib = rm.StoredFileLoc.objects.create(sfile=self.sflib, servershare=self.analocalstor,
                path='libfiles', active=True, purged=False)
        copy(os.path.join(self.newstorctrl.path, self.sflibloc.path, self.sflib.filename),
                os.path.join(self.oldstorctrl.path, anasflib.path, self.sflib.filename))
        refineurl = '/refinemzml/'
        resp = self.cl.post(refineurl, content_type='application/json', data={'dsid': self.ds.pk,
            'dbid': self.sflib.pk, 'wfid': self.refinewf.pk})
        self.assertEqual(resp.status_code, 200)
        jq = jm.Job.objects.filter(funcname='refine_mzmls')
        self.assertTrue(jq.exists())
        j = jq.get()
        exp_kw = {'dss_id': dss2.pk, 'wfv_id': self.refinewf.pk,
                'dbfn_id': self.sflib.pk, 'qtype': self.ds.quantdataset.quanttype.shortname}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}_refined.mzML'
        ref_ana = am.Analysis.objects.last()
        timestamp = datetime.strftime(ref_ana.date, '%Y%m%d_%H.%M')
        mzmlfn1 = os.path.join(dss2_fpath, mzmlfn)
        mzmlfn2 = os.path.join(self.inboxctrl.path, 'datasets', str(self.ds.pk),  mzmlfn)
        mzmlfn3 = os.path.join(self.f3path, mzmlfn)
        self.assertFalse(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.run_job() # refining
        #self.assertTrue(os.path.exists(mzmlfn0))
        self.assertTrue(os.path.exists(mzmlfn1))
        self.run_job() # rsync to inbox
        self.assertTrue(os.path.exists(mzmlfn2))
        self.run_job() # running rsync to first dss
        self.assertTrue(os.path.exists(mzmlfn3))
        self.run_job() # setting to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        self.assertTrue(os.path.exists(mzmlfn2))

    def test_create_mzml_tims_rsync_sameserver(self):
        '''Two datasetserver locations (one on ssnewstore, one on sstmp),
        so rsync does not use SSH. As ssnewstore is mounted on analysis
        server, the analysis server can directly run local non-ssh rsync'''
        self.f3raw.producer = self.prodtims
        self.f3raw.save()
        rawfn = os.path.join(self.f3path, self.f3raw.name)
        dss2path = 'testdss2'
        dss2_fpath = os.path.join(self.tmpctrl.path, dss2path)
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
        exp_kw  = {'options': ['combineIonMobilitySpectra'], 'filters': ['precursorRefine', '"scanSumming precursorTol=0.02 scanTimeTol=10 ionMobilityTol=0.1"'], 
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
        self.run_job() # convert mzml
        self.assertTrue(os.path.exists(mzmlfn1))
        self.assertFalse(os.path.exists(mzmlfn2))
        self.assertTrue(mzsfl_q_stor.filter(purged=False).exists())
        self.assertTrue(mzsfl_q_tmp.filter(purged=True).exists())
        self.run_job() # running rsync to sstmp, not mounted
        self.assertTrue(os.path.exists(mzmlfn2))
        self.assertTrue(mzsfl_q_tmp.filter(purged=False).exists())
        self.run_job() # set to done
        j2 = jm.Job.objects.last()
        self.assertEqual(j2.state, 'done')
        t = j2.task_set.first()


class TestRefineMzmls(MzmlTests):
    url = '/refinemzml/'

    def setUp(self):
        super().setUp()
        self.refinewf = am.NextflowWfVersionParamset.objects.create(update='refine wf',
                commit='master', filename='refine.py', nfworkflow=self.nfw, paramset=self.pset, 
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
        timsraw = rm.RawFile.objects.create(name='file2', producer=self.prodtims, source_md5='timsmd4',
                size=100, date=timezone.now(), claimed=True, usetype=rm.UploadFileType.RAWFILE)
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=timsraw)
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk, 'dbid': self.sflib.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('contains data from multiple instrument types', resp.json()['error'])

    def test_existing_mzmls_no_db(self):
        # Not passed a db
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual('Must pass a database to refine with', resp.json()['error'])

        # no mzMLs exist yet
        self.f3mzml.delete()
        resp = self.cl.post(self.url, content_type='application/json', data={'pwiz_id': self.pwiz.pk,
            'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 403)
        self.assertIn('Need to create normal mzMLs', resp.json()['error'])

        # refined exists already
        refinedsf = rm.StoredFile.objects.create(rawfile=self.f3raw, filename=f'{self.f3raw.name}_refined',
                md5='refined_md5', checked=True, filetype=self.ft)
        rm.StoredFileLoc.objects.create(sfile=refinedsf, servershare=self.dss.storageshare, path=self.storloc,
                purged=False, active=True)
        am.MzmlFile.objects.create(sfile=refinedsf, pwiz=self.pwiz, refined=True)
        resp = self.cl.post(self.url, content_type='application/json', data={'dsid': self.ds.pk})
        self.assertEqual(resp.status_code, 403)

    def test_refine_mzml_move_dbfile(self):
        '''Do a refine, test its correctness, then make sure there is no dataset servershare
        move on the current dataset, unlike the test_with_filemove (old data to new share)'''
        dbraw = rm.RawFile.objects.create(name='db2.fa', producer=self.prod, source_md5='db2md5',
                size=100, claimed=True, date=timezone.now(), usetype=rm.UploadFileType.LIBRARY)
        dbsf = rm.StoredFile.objects.create(rawfile=dbraw, md5=dbraw.source_md5,
                filetype=self.lft, checked=True, filename=dbraw.name)
        rm.StoredFileLoc.objects.create(sfile=dbsf, servershare=self.analocalstor, path='libfiles',
                active=True, purged=False)
        # Create second file for dset
        f3raw2 = rm.RawFile.objects.create(name='f3raw2', producer=self.prod,
                source_md5='f3_fakemd5_2', size=100, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.RAWFILE)
        dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=f3raw2)
        f3sf2 = rm.StoredFile.objects.create(rawfile=f3raw2, filename=f3raw2.name,
                md5=f3raw2.source_md5, filetype=self.ft, checked=True)
        f3sss2 = rm.StoredFileLoc.objects.create(sfile=f3sf2, servershare=self.ssnewstore,
                path=self.storloc, active=True, purged=False)
        f3sfmz2 = rm.StoredFile.objects.create(rawfile=f3raw2,
                filename=f'{os.path.splitext(f3raw2.name)[0]}.mzML',
                md5='md5_for_f3sf_mzml2', filetype=self.ft, checked=True)
        f3mzsss2 = rm.StoredFileLoc.objects.create(sfile=f3sfmz2, servershare=self.ssnewstore,
                path=self.storloc, active=True, purged=False)
        f3mzml2 = am.MzmlFile.objects.create(sfile=f3sfmz2, pwiz=self.pwiz)

        refined1 = rm.StoredFile.objects.filter(mzmlfile__refined=True, rawfile=self.f3raw, deleted=False)
        refined2 = rm.StoredFile.objects.filter(mzmlfile__refined=True, rawfile=f3raw2, deleted=False)
        self.assertFalse(refined1.exists())
        self.assertFalse(refined2.exists())
        resp = self.cl.post(self.url, content_type='application/json', data={'dsid': self.ds.pk,
            'dbid': dbsf.pk, 'wfid': self.refinewf.pk})
        self.assertTrue(refined1.exists())
        self.assertTrue(refined2.exists())
        self.assertEqual(resp.status_code, 200)
        jdb = jm.Job.objects.first()
        self.assertEqual(jdb.funcname, 'rsync_otherfiles_to_servershare')
        j = jm.Job.objects.get(funcname='refine_mzmls')
        exp_kw = {'dss_id': self.dss.pk, 'wfv_id': self.refinewf.pk, 'dbfn_id': dbsf.pk, 
                'qtype': self.ds.quantdataset.quanttype.shortname}
        for k, val in exp_kw.items():
            self.assertEqual(j.kwargs[k], val)
        self.run_job() # rsync db
        mzmlfn = f'{os.path.splitext(self.f3raw.name)[0]}_refined.mzML'
        mzmlfn2 = f'{os.path.splitext(f3raw2.name)[0]}_refined.mzML'
        ana = am.Analysis.objects.last()
        mzmlfn1_path = os.path.join(self.f3path, mzmlfn)
        mzmlfn2_path = os.path.join(self.f3path, mzmlfn2)
        self.assertFalse(os.path.exists(mzmlfn1_path))
        self.assertFalse(os.path.exists(mzmlfn2_path))
        self.run_job() # refining
        self.assertTrue(os.path.exists(mzmlfn1_path))
        self.assertTrue(os.path.exists(mzmlfn2_path))
        self.run_job() # set to done
        j.refresh_from_db()
        self.assertEqual(j.state, Jobstates.DONE)

        # Delete one of the refined mzMLs and re-run
        refined2.delete()
        os.unlink(mzmlfn2_path)
        resp = self.cl.post(self.url, content_type='application/json', data={'dsid': self.ds.pk,
            'dbid': dbsf.pk, 'wfid': self.refinewf.pk})
        self.assertEqual(resp.status_code, 200)
        jdb = jm.Job.objects.first()
        self.assertEqual(jdb.funcname, 'rsync_otherfiles_to_servershare')
        j2 = jm.Job.objects.filter(funcname='refine_mzmls').last()
        self.assertGreater(j2.pk, j.pk)
        exp_kw = {'dss_id': self.dss.pk, 'wfv_id': self.refinewf.pk, 'dbfn_id': dbsf.pk, 
                'qtype': self.ds.quantdataset.quanttype.shortname}
        for k, val in exp_kw.items():
            self.assertEqual(j2.kwargs[k], val)
        ana = am.Analysis.objects.last()
        # re-define path bc new run dir
        mzmlfn2_nf = os.path.join(self.rootdir, self.nfrunshare.path, ana.get_run_base_dir(), mzmlfn2)
        self.assertFalse(os.path.exists(mzmlfn2_nf))
        self.assertTrue(os.path.exists(mzmlfn1_path))
        self.assertFalse(os.path.exists(mzmlfn2_path))
        self.run_job() # refining
        self.assertTrue(os.path.exists(mzmlfn1_path))
        self.assertTrue(os.path.exists(mzmlfn2_path))
        self.run_job() # set to done
        j2.refresh_from_db()
        self.assertEqual(j2.state, Jobstates.DONE)

