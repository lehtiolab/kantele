# Integration tests, including storage files etc
import os
import shutil
from time import sleep
from datetime import timedelta

from django.contrib.auth.models import User
from django.test import TestCase, LiveServerTestCase, Client
from django.utils import timezone
from django.core.management import call_command
from django.test import tag

from kantele import settings
from datasets import models as dm
from rawstatus import models as rm
from jobs import models as jm
from analysis import models as am


# FIXME
# This is mainly fixtures and its getting out of hand
# We need to separate them better

# Django flushes DB between tests, so we dont need get_or_create, but theres a lot non shared


class BaseTest(TestCase):
    '''Normal django tests inherit here'''

    def post_json(self, data):
        return self.cl.post(self.url, content_type='application/json', data=data)

    def setUp(self):
        # Clean directory containing storage servers
        self.rootdir = '/storage'
        for dirname in os.listdir(self.rootdir):
            if os.path.isdir(os.path.join(self.rootdir, dirname)):
                shutil.rmtree(os.path.join(self.rootdir, dirname))
        shutil.copytree('/fixtures', self.rootdir, dirs_exist_ok=True)
        self.cl = Client()
        username='testuser'
        email = 'test@test.com'
        password='12345'
        self.user = User(username=username, email=email)
        self.user.set_password(password)
        self.user.save() 
        dm.Operator.objects.create(user=self.user) # need operator for QC jobs, analysis check-in
        self.cl.login(username=username, password=password)
        # storage backend
        self.storagecontroller = rm.FileServer.objects.create(name='storage1', uri='s1.test',
                fqdn='storage_ssh_1', can_rsync_remote=True, rsyncusername='kantele',
                rsynckeyfile='/kantelessh/rsync_key', can_backup=True)
        self.anaserver = rm.FileServer.objects.create(name='analysis1', uri='ana.test',
                fqdn='analysis_ssh_1', rsyncusername='kantele', rsynckeyfile='/kantelessh/rsync_key')
        self.anaprofile = rm.AnalysisServerProfile.objects.create(server=self.anaserver, queue_name='analysis1')

        self.ssinbox = rm.ServerShare.objects.create(name='inbox', max_security=2,
                function=rm.ShareFunction.INBOX, maxdays_data=1)
        self.sslib = rm.ServerShare.objects.create(name='libshare', max_security=1,
                function=rm.ShareFunction.LIBRARY)
        self.inboxctrl = rm.FileserverShare.objects.create(server=self.storagecontroller,
                share=self.ssinbox, path=os.path.join(self.rootdir, 'inbox'))
        self.libctrl = rm.FileserverShare.objects.create(server=self.storagecontroller,
                share=self.sslib, path=os.path.join(self.rootdir, 'libshare'))

        self.sstmp = rm.ServerShare.objects.create(name='tmpshare', max_security=1,
                function=rm.ShareFunction.RAWDATA)
        self.tmpctrl = rm.FileserverShare.objects.create(server=self.storagecontroller,
                share=self.sstmp, path=os.path.join(self.rootdir, 'tmp'))
        self.ssnewstore = rm.ServerShare.objects.create(name='ssnewstore', max_security=1,
                function=rm.ShareFunction.RAWDATA)
        self.newstorctrl = rm.FileserverShare.objects.create(server=self.storagecontroller,
                share=self.ssnewstore, path=os.path.join(self.rootdir, 'newstorage'))
        self.ana_newstor = rm.FileserverShare.objects.create(server=self.anaserver,
                share=self.ssnewstore, path=os.path.join(self.rootdir, 'newstorage'))
        self.ssana = rm.ServerShare.objects.create(name='analysisfakename', max_security=1,
                function=rm.ShareFunction.ANALYSIS_DELIVERY)
        self.anashare = rm.FileserverShare.objects.create(server=self.storagecontroller, share=self.ssana,
                path=os.path.join(self.rootdir, 'analysis'))
        # web share without a controller
        self.ssweb = rm.ServerShare.objects.create(name='reportswebshare', max_security=1,
                function=rm.ShareFunction.REPORTS, maxdays_data=1)

        self.ssanaruns = rm.ServerShare.objects.create(name='analysisruns', max_security=1,
                function=rm.ShareFunction.ANALYSISRESULTS, maxdays_data=1)
        self.nfrunshare = rm.FileserverShare.objects.create(server=self.anaserver,
                share=self.ssanaruns, path=os.path.join(self.rootdir, 'nf_run_output'))

        self.remoteanaserver = rm.FileServer.objects.create(name='analysis2', uri='s0.test',
                fqdn='analysis_ssh_2', can_rsync_remote=False, rsyncusername='kantele',
                rsynckeyfile='/kantelessh/rsync_key')
        self.anaprofile2 = rm.AnalysisServerProfile.objects.create(server=self.remoteanaserver, queue_name='analysis2')
        self.analocalstor = rm.ServerShare.objects.create(name='analocalstor', active=True,
                max_security=max(rm.DataSecurityClass), function=rm.ShareFunction.RAWDATA, maxdays_data=1)
        self.ssanaruns2 = rm.ServerShare.objects.create(name='analysisruns2', max_security=1,
                function=rm.ShareFunction.ANALYSISRESULTS, maxdays_data=1)
        self.nfrunshare2 = rm.FileserverShare.objects.create(server=self.remoteanaserver,
                share=self.ssanaruns2, path=os.path.join(self.rootdir, 'nf_run_output2'))
        self.oldstorctrl = rm.FileserverShare.objects.create(server=self.remoteanaserver,
                share=self.analocalstor, path=os.path.join(self.rootdir, 'oldstorage'))

        # Species / sampletype fill
        self.spec1, _ = dm.Species.objects.get_or_create(linnean='species1', popname='Spec1')
        self.spec2, _ = dm.Species.objects.get_or_create(linnean='species2', popname='Spec2')
        self.samtype1, _ = dm.SampleMaterialType.objects.get_or_create(name='sampletype1')
        self.samtype2, _ = dm.SampleMaterialType.objects.get_or_create(name='sampletype2')

        # Datasets/projects prep
        self.species = dm.Species.objects.create(linnean='Homo sapiens', popname='Human')
        self.dtype = dm.Datatype.objects.create(name='dtype1')
        self.dtcompdef = dm.DatatypeComponent.objects.create(datatype=self.dtype, component=dm.DatasetUIComponent.DEFINITION)
        self.dtcompfiles = dm.DatatypeComponent.objects.create(datatype=self.dtype, component=dm.DatasetUIComponent.FILES)
        self.dtcompsamples = dm.DatatypeComponent.objects.create(datatype=self.dtype, component=dm.DatasetUIComponent.SAMPLES)
        qdt, _ = dm.Datatype.objects.get_or_create(name='Quantitative proteomics')
        self.ptype = dm.ProjectTypeName.objects.create(name='testpt')
        self.pi = dm.PrincipalInvestigator.objects.create(name='testpi')

        # File prep, producers etc
        self.ft = rm.StoredFileType.objects.create(name='testft_bruker', filetype='tst',
                is_rawdata=True, is_folder=True, stablefiles=['analysis.tdf'])
        self.prod = rm.Producer.objects.create(name='prod1', client_id='abcdefg', shortname='p1', internal=True)
        self.adminprod = rm.Producer.objects.create(name='adminprod', client_id=settings.STORAGECLIENT_APIKEY, shortname=settings.PRODUCER_ADMIN_NAME, internal=True)
        self.msit = rm.MSInstrumentType.objects.create(name='test')
        rm.MSInstrument.objects.create(producer=self.prod, instrumenttype=self.msit, filetype=self.ft)
        self.qt, _ = dm.QuantType.objects.get_or_create(name='testqt', shortname='testqtplex')
        self.qch, _ = dm.QuantChannel.objects.get_or_create(name='126')
        self.qtch, _ = dm.QuantTypeChannel.objects.get_or_create(quanttype=self.qt, channel=self.qch) 
        self.lfqt, _ = dm.QuantType.objects.get_or_create(name='labelfree', shortname='lf')

        # Project/dset on new storage
        self.p1 = dm.Project.objects.create(name='p1', pi=self.pi, ptype=self.ptype)
        self.projsam1 = dm.ProjectSample.objects.create(sample='sample1', project=self.p1)
        dm.SampleMaterial.objects.create(sample=self.projsam1, sampletype=self.samtype1)
        dm.SampleSpecies.objects.create(sample=self.projsam1, species=self.spec1)
        self.exp1 = dm.Experiment.objects.create(name='e1', project=self.p1)
        self.run1 = dm.RunName.objects.create(name='run1', experiment=self.exp1)
        self.storloc = os.path.join(self.p1.name, self.exp1.name, self.dtype.name, self.run1.name)
        self.ds = dm.Dataset.objects.create(date=self.p1.registered, runname=self.run1,
                datatype=self.dtype, securityclass=min(rm.DataSecurityClass))
        self.dss = dm.DatasetServer.objects.create(dataset=self.ds, storageshare=self.ssnewstore,
                storage_loc_ui=self.storloc, storage_loc=self.storloc, startdate=timezone.now())
        dm.DatasetComponentState.objects.create(dataset=self.ds, state=dm.DCStates.OK, dtcomp=self.dtcompfiles)
        dm.DatasetComponentState.objects.create(dataset=self.ds, state=dm.DCStates.OK, dtcomp=self.dtcompsamples)
        self.contact, _ = dm.ExternalDatasetContact.objects.get_or_create(dataset=self.ds,
                defaults={'email': 'contactname'})
        dm.DatasetOwner.objects.get_or_create(dataset=self.ds, user=self.user)
        self.f3path = os.path.join(self.newstorctrl.path, self.storloc)
        fn3 = 'raw3.raw' # directory to pretend its bruker file with analysis.tdf
        f3size = sum(os.path.getsize(os.path.join(wpath, subfile))
                for wpath, subdirs, files in os.walk(os.path.join(self.f3path, fn3))
                    for subfile in files if subfile)
        # Important, the md5 here is fake, since the raw fn3 is also used in actual transfer
        # of a new file in tests
        self.f3raw = rm.RawFile.objects.create(name=fn3, producer=self.prod,
                source_md5='f3_fakemd5',
                size=f3size, date=timezone.now(), claimed=True, usetype=rm.UploadFileType.RAWFILE)
        self.f3dsr = dm.DatasetRawFile.objects.create(dataset=self.ds, rawfile=self.f3raw)
        self.f3sf = rm.StoredFile.objects.create(rawfile=self.f3raw, filename=fn3,
                md5=self.f3raw.source_md5, filetype=self.ft, checked=True)
        self.f3sssinbox = rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.ssinbox,
                path=settings.TMPPATH, active=False, purged=False)
        self.f3sss = rm.StoredFileLoc.objects.create(sfile=self.f3sf, servershare=self.ssnewstore,
                path=self.storloc, active=True, purged=False)
        self.qcs = dm.QuantChannelSample.objects.create(dataset=self.ds, channel=self.qtch,
                projsample=self.projsam1)
        dm.QuantDataset.objects.create(dataset=self.ds, quanttype=self.qt)
        dm.DatasetSample.objects.create(dataset=self.ds, projsample=self.projsam1)

        # Pwiz/mzml
        self.pset = am.ParameterSet.objects.create(name='pset_base')
        self.nfw = am.NextflowWorkflowRepo.objects.create(
                description='repo_base desc', repo='/storage/nfrepo')
        self.nfwv = am.NextflowWfVersionParamset.objects.create(update='pwiz wfv base',
                commit='master', filename='pwiz.py', nfworkflow=self.nfw,
                paramset=self.pset, nfversion='', active=True)
        self.pwiz = am.Proteowizard.objects.create(version_description='pwversion desc1',
                params={'mzmltool': 'msconvert'}, nf_version=self.nfwv)
        self.f3sfmz = rm.StoredFile.objects.create(rawfile=self.f3raw, filename=f'{os.path.splitext(fn3)[0]}.mzML',
                md5='md5_for_f3sf_mzml', filetype=self.ft, checked=True)
        self.f3mzsss = rm.StoredFileLoc.objects.create(sfile=self.f3sfmz, servershare=self.ssnewstore,
                path=self.storloc, active=True, purged=False)
        self.f3mzml = am.MzmlFile.objects.create(sfile=self.f3sfmz, pwiz=self.pwiz)

        # Project/dataset/files on old storage
        oldfn = 'raw1'
        self.oldp = dm.Project.objects.create(name='oldp', pi=self.pi, ptype=self.ptype)
        self.projsam2 = dm.ProjectSample.objects.create(sample='sample2', project=self.oldp)
        dm.SampleMaterial.objects.create(sample=self.projsam2, sampletype=self.samtype2)
        dm.SampleSpecies.objects.create(sample=self.projsam2, species=self.spec2)
        self.oldexp = dm.Experiment.objects.create(name='olde', project=self.oldp)
        self.oldrun = dm.RunName.objects.create(name='run1', experiment=self.oldexp)
        self.oldstorloc = os.path.join(self.oldp.name, self.oldexp.name, self.oldrun.name)
        self.oldds = dm.Dataset.objects.create(date=self.oldp.registered, runname=self.oldrun,
                datatype=self.dtype, securityclass=max(rm.DataSecurityClass)) 
        self.olddss = dm.DatasetServer.objects.create(dataset=self.oldds, storageshare=self.analocalstor,
                storage_loc_ui=self.oldstorloc, storage_loc=self.oldstorloc, startdate=timezone.now())
        dm.QuantDataset.objects.get_or_create(dataset=self.oldds, quanttype=self.lfqt)
        dm.DatasetComponentState.objects.create(dataset=self.oldds, dtcomp=self.dtcompfiles, state=dm.DCStates.OK)
        dm.DatasetComponentState.objects.create(dataset=self.oldds, dtcomp=self.dtcompsamples, state=dm.DCStates.OK)
        self.contact, _ = dm.ExternalDatasetContact.objects.get_or_create(dataset=self.oldds,
                email='contactname')
        dm.DatasetOwner.objects.get_or_create(dataset=self.oldds, user=self.user)
        self.oldfpath = os.path.join(self.oldstorctrl.path, self.oldstorloc)
        oldsize = os.path.getsize(os.path.join(self.oldfpath, oldfn))
        self.oldraw = rm.RawFile.objects.create(name=oldfn, producer=self.prod,
                source_md5='old_to_new_fakemd5', size=oldsize, date=timezone.now(), claimed=True, usetype=rm.UploadFileType.RAWFILE)
        self.olddsr = dm.DatasetRawFile.objects.create(dataset=self.oldds, rawfile=self.oldraw)
        self.oldsf = rm.StoredFile.objects.create(rawfile=self.oldraw, filename=oldfn,
                    md5=self.oldraw.source_md5, filetype=self.ft, checked=True)
        self.oldsss = rm.StoredFileLoc.objects.create(sfile=self.oldsf, servershare=self.analocalstor,
                path=self.oldstorloc, active=True, purged=False)
        self.oldqsf = dm.QuantSampleFile.objects.create(rawfile=self.olddsr, projsample=self.projsam2)

        # Tmp rawfile inbox
        tmpfn = 'raw2'
        tmpfpathfn = os.path.join(self.inboxctrl.path, tmpfn)
        tmpsize = os.path.getsize(tmpfpathfn)
        self.tmpraw = rm.RawFile.objects.create(name=tmpfn, producer=self.prod,
                source_md5='tmpraw_fakemd5', size=tmpsize, date=timezone.now(), claimed=False, usetype=rm.UploadFileType.RAWFILE)
        self.tmpsf = rm.StoredFile.objects.create(rawfile=self.tmpraw, md5=self.tmpraw.source_md5,
                filename=tmpfn, checked=True, filetype=self.ft)
        self.tmpsss = rm.StoredFileLoc.objects.create(sfile=self.tmpsf, servershare=self.ssinbox,
                path='', active=True, purged=False)

        # Library files, for use as input, so claimed and ready
        self.lft = rm.StoredFileType.objects.create(name=settings.DBFA_FT_NAME, filetype='fasta',
                is_rawdata=False, user_uploadable=True)
        self.libraw = rm.RawFile.objects.create(name='db.fa', producer=self.prod,
                source_md5='libfilemd5', size=100, claimed=True, date=timezone.now(), usetype=rm.UploadFileType.LIBRARY)
        self.sflib = rm.StoredFile.objects.create(rawfile=self.libraw, md5=self.libraw.source_md5,
                filetype=self.lft, checked=True, filename=self.libraw.name)
        self.sflibloc = rm.StoredFileLoc.objects.create(sfile=self.sflib,
                servershare=self.ssnewstore, path='libfiles', active=True, purged=False)
        self.lf = am.LibraryFile.objects.create(sfile=self.sflib, description='This is a libfile')

#        # User files for input
        self.uft = rm.StoredFileType.objects.create(name='ufileft', filetype='tst', is_rawdata=False)

        # Analysis files
        anaft = rm.StoredFileType.objects.create(name=settings.ANALYSIS_FT_NAME, filetype='ana',
                is_rawdata=False)
        self.anaprod = rm.Producer.objects.create(name='analysisprod', client_id=settings.ANALYSISCLIENT_APIKEY, shortname=settings.PRODUCER_ANALYSIS_NAME)
        self.ana_raw = rm.RawFile.objects.create(name='ana_file', producer=self.anaprod,
                source_md5='kjlmnop1234', size=100, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.ANALYSIS)
        self.anasfile = rm.StoredFile.objects.create(rawfile=self.ana_raw, filetype=anaft,

                filename=self.ana_raw.name, md5=self.ana_raw.source_md5)
        rm.StoredFileLoc.objects.create(sfile=self.anasfile, servershare=self.sstmp, path='',
                active=True, purged=False)
        self.ana_raw2 = rm.RawFile.objects.create(name='ana_file2', producer=self.anaprod,
                source_md5='anarawabc1234', size=100, date=timezone.now(), claimed=True,
                usetype=rm.UploadFileType.ANALYSIS)
        self.anasfile2 = rm.StoredFile.objects.create(rawfile=self.ana_raw2,
                filetype_id=self.ft.id, filename=self.ana_raw2.name, filetype=anaft,
                    md5=self.ana_raw2.source_md5)
        rm.StoredFileLoc.objects.create(sfile=self.anasfile2, servershare=self.sstmp, path='',
                active=True, purged=False)


class ProcessJobTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.job = self.jobclass(1)

    def check(self, expected_tasks):
        for t in self.job.run_tasks:
            self.assertIn(t, expected_tasks)


@tag('slow')
class BaseIntegrationTest(LiveServerTestCase):
    # use a live server so that jobrunner can interface with it (otherwise only dummy
    # test client can do that)
    port = 80
    host = '0.0.0.0'
    jobrun_timeout = 2

    def setUp(self):
        BaseTest.setUp(self)

    def post_json(self, data):
        return self.cl.post(self.url, content_type='application/json', data=data)

    def run_job(self):
        '''Call run jobs, then sleep to make tasks do their work'''
        call_command('runjobs')
        sleep(self.jobrun_timeout)
