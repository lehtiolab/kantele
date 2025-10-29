from datetime import datetime

from kantele.tests import BaseTest
from kantele import settings
from analysis import models as am
from jobs import models as jm
from jobs import jobs as jj
from dashboard import models as dm
from datasets import models as dam


class QCBase(BaseTest):
    pepseq = 'IAMAPEPTIDE'
    pepcharge = 2
    tprt = 15.0
    tpscore = 5e-06
    tpfwhm=0.09
    tpms1 = 70000.0
    ddaplots = {
        "nrpsms": 67637,  
        "nrscans": 129968,
        "nrpeptides": 38194,
        "nr_unique_peptides": 36158,
        "nrproteins": 5023,
        "missed_cleavages": {
            "0": 64603,
            "1": 2965,
            "2": 69
            },
        "injtime": False,
        "matchedpeaks": {
            "q1": 10.0,
            "q2": 14.0,
            "q3": 17.0
            }, 
        "score": {
            "q1": 0.29441416,
            "q2": 0.45151624,
            "q3": 0.5780469
            },
        "rt": {
            "q1": 15.705895,
            "q2": 24.795988,
            "q3": 32.635075
            },
        "p_error": {
            "q1": 1.1175711,
            "q2": 2.3226476,
            "q3": 3.9190276
            },
        "fwhm": {
            "q1": 0.07856697541417468,
            "q2": 0.11463563697349244,
            "q3": 0.19013413110220512
            },
        "ionmob": {
            "q1": 0.89153504,
            "q2": 0.99197274,
            "q3": 1.1201969
            },
        "ms1": {
            "q1": 82378.75,
            "q2": 192032.5,
            "q3": 460915.0
            }
        }

    diaplots = {
        "nrpsms": 42386,
        "nrscans": 24745,
        "nrpeptides": 39086,
        "nr_unique_peptides": 37123,
        "nrproteins": 5592,
        "missed_cleavages": {
            "0": 41661,
            "1": 725,
            "2": 7,
            },
        "peaks_fwhm": 3.012,
        "score": {
            "q1": 1.31007038817188e-06,
            "q2": 4.847267973673297e-06,
            "q3": 2.0830304492847063e-05,
            },
        "rt": {
            "q1": 13.748827934265137,
            "q2": 21.873414993286133,
            "q3": 29.986976623535156,
            },
        "fwhm": {
            "q1": 0.07407437451183796,
            "q2": 0.08781002461910248,
            "q3": 0.10342045687139034,
            },
        "ionmob": {
            "q1": 0.89760422706604,
            "q2": 0.9681817889213562,
            "q3": 1.0567045211791992,
            },
        "ms1": {
            "q1": 24022.95,
            "q2": 50979.8,
            "q3": 122692.25,
            },
        "trackedpeptides": {
            f'{pepseq}_{pepcharge}': {
                "rt": tprt,
                "score": tpscore,
                "fwhm": tpfwhm,
                "ms1": tpms1,
                },
            },
        }


    def setUp(self):
        super().setUp()
        self.diaana = am.Analysis.objects.create(user=self.user, name='dia ana qc', base_rundir='testdirqc')
        self.diaqc = dm.QCRun.objects.create(rawfile=self.tmpraw, analysis=self.diaana, is_ok=False,
                message='', runtype=dam.AcquisistionMode.DIA)

        wfv = am.NextflowWfVersionParamset.objects.create(update='qc wfv base',
                commit='qc ci base', filename='qc.nf', nfworkflow=self.nfw,
                paramset=self.pset, nfversion='', active=True)
        self.trackpep = dm.TrackedPeptide.objects.create(sequence=self.pepseq, charge=self.pepcharge)
        pepset = dm.TrackedPeptideSet.objects.create(name='qcpeps', acqmode=dam.AcquisistionMode.DIA,
                active=True, frozen=True)
        dm.PeptideInSet.objects.create(peptideset=pepset, peptide=self.trackpep)
        qcjob = jm.Job.objects.create(funcname='qcjob', state=jj.Jobstates.PROCESSING,
                kwargs={'trackpeptides': [[self.trackpep.pk, self.trackpep.sequence, 
                    self.trackpep.charge]]}, timestamp=datetime.now())
        uwf = am.UserWorkflow.objects.create(name='qc', wftype=am.UserWorkflow.WFTypeChoices.QC,
                public=False)
        uwf.nfwfversionparamsets.add(wfv)
        am.NextflowSearch.objects.create(nfwfversionparamset=wfv, workflow=uwf, analysis=self.diaana,
            token='token1234', job=qcjob)


class QCSave(QCBase):
    url = '/jobs/set/longqc/'

    def test_bad_requests(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'client_id': 'wrong'})
        self.assertEqual(resp.status_code, 403)
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'qcrun_id': self.diaqc.pk}
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('Missing parameter', resp.json()['msg'])

    def test_save_qcrun_dia(self):
        self.diaana = am.Analysis.objects.create(user=self.user, name='testana_qc', base_rundir='testdirqc')
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'qcrun_id': self.diaqc.pk,
                'analysis_id': self.diaana.pk, 'state': 'ok', 'msg': 'test ok',
                'plots': self.diaplots}
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 200)
        bp = self.diaqc.boxplotdata_set.get(datatype=dm.QuartileDataTypes.SCORE)
        self.assertEqual(bp.q2, post['plots']['score']['q2'])
        lp = self.diaqc.lineplotdata_set.get(datatype=dm.LineDataTypes.NRPSMS)
        self.assertEqual(lp.value, post['plots']['nrpsms'])
        self.assertFalse(self.diaqc.boxplotdata_set.filter(datatype=dm.QuartileDataTypes.MASSERROR).exists())

    def test_save_dda_qc(self):
        self.diaqc.runtype = dam.AcquisistionMode.DDA
        self.diaqc.save()
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'qcrun_id': self.diaqc.pk,
                'analysis_id': self.diaana.pk, 'state': 'ok', 'msg': 'test ok',
                'plots': self.ddaplots}
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 200)
        bp = self.diaqc.boxplotdata_set.get(datatype=dm.QuartileDataTypes.SCORE)
        self.assertEqual(bp.q2, post['plots']['score']['q2'])
        lp = self.diaqc.lineplotdata_set.get(datatype=dm.LineDataTypes.NRPSMS)
        self.assertEqual(lp.value, post['plots']['nrpsms'])
        self.assertFalse(self.diaqc.lineplotdata_set.filter(datatype=dm.LineDataTypes.PEAKS_FWHM).exists())


class QCShow(QCBase):
    url = '/dash/longqc/'

    def test_show(self):
        # First upload data
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'qcrun_id': self.diaqc.pk,
                'analysis_id': self.diaana.pk, 'state': 'ok', 'msg': 'test ok', 'plots': self.diaplots}
        resp = self.cl.post('/jobs/set/longqc/', content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 200)

        resp = self.cl.get(f'{self.url}{self.prod.pk}/DIA/0/{2}/')
        self.assertEqual(resp.status_code, 200)
        js = resp.json()
        rawdate = datetime.strftime(self.tmpraw.date, '%Y-%m-%d %H:%M')
        linemap = {dm.LineDataTypes.NRPEPTIDES: 'nrpeptides', dm.LineDataTypes.NRPSMS: 'nrpsms',
                dm.LineDataTypes.NRPROTEINS: 'nrproteins', dm.LineDataTypes.NRSCANS: 'nrscans',
                dm.LineDataTypes.NRPEPTIDES_UNI: 'nr_unique_peptides',
                dm.LineDataTypes.PEAKS_FWHM: 'peaks_fwhm',
                }
        boxmap = {dm.QuartileDataTypes.FWHM: 'fwhm', dm.QuartileDataTypes.MASSERROR: 'p_error',
                dm.QuartileDataTypes.PEPMS1AREA: 'ms1',
                dm.QuartileDataTypes.SCORE: 'score', dm.QuartileDataTypes.IONMOB: 'ionmob',
                dm.QuartileDataTypes.IONINJ: 'injtime', dm.QuartileDataTypes.RT: 'rt',
                dm.QuartileDataTypes.MATCHED_PEAKS: 'matchedpeaks',
                }
        for pn, points in js['data'].items():
            if not len(points):
                continue
            elif 'value' in points[0]:
                keys = []
                if pn == 'ident':
                    # one point for each line (pep, prot, unipep)
                    keys = [dm.LineDataTypes.NRPEPTIDES, dm.LineDataTypes.NRPEPTIDES,
                            dm.LineDataTypes.NRPEPTIDES_UNI]
                    self.assertEqual(len(points), 3)
                    for p in points:
                        self.assertEqual(p['value'], self.diaplots[linemap[p['key']]])
                elif pn == 'psms':
                    self.assertEqual(len(points), 2)
                    keys = [dm.LineDataTypes.NRPSMS, dm.LineDataTypes.NRSCANS]
                    for p in points:
                        self.assertEqual(p['value'], self.diaplots[linemap[p['key']]])
                elif pn == 'PEAKS_FWHM':
                    self.assertEqual(len(points), 1)
                    keys = [dm.LineDataTypes.PEAKS_FWHM]
                    self.assertEqual(points[0]['value'], self.diaplots[linemap[points[0]['key']]])
                elif pn == 'miscleav':
                    self.assertEqual(len(points), 2)
                    keys = [dm.LineDataTypes.MISCLEAV1, dm.LineDataTypes.MISCLEAV2]
                    for p in points:
                        nr = {dm.LineDataTypes.MISCLEAV1: '1', dm.LineDataTypes.MISCLEAV2: '2'}[p['key']]
                        self.assertEqual(p['value'], self.diaplots['missed_cleavages'][nr])
                elif pn == 'mcratio':
                    # TODO test for missed cleavage plot
                    continue
                else:
                    self.assertEqual(len(points), 1)
                    keys = [dm.LineDataTypes[pn]]
                    self.assertEqual(points[0]['value'], self.diaplots[pn])
                self.assertIn(points[0]['key'], keys)
                self.assertEqual(points[0]['date'], rawdate)

            elif 'q2' in points[0]:
                self.assertEqual(len(points), 1)
                self.assertEqual(points[0]['q2'], self.diaplots[boxmap[dm.QuartileDataTypes[pn]]]['q2'])
        # Since it's DIA, we can check trackedpeptides
        base_val = {'pepid': self.trackpep.pk, 'run': self.diaqc.pk,
                'date': datetime.strftime(self.tmpraw.date, '%Y-%m-%d %H:%M')}
        exp_tp = {'RT': [{'value': self.tprt, **base_val}],
        'FWHM': [{'value': self.tpfwhm, **base_val}],
        'SCORE': [{'value': self.tpscore, **base_val}],
        'PEPMS1AREA': [{'value': self.tpms1, **base_val}],
        'peps': {f'{self.trackpep.pk}': self.trackpep.sequence},
        }
        self.assertEqual(js['extradata'], exp_tp)
