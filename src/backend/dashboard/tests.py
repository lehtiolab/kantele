from datetime import datetime

from kantele.tests import BaseTest
from kantele import settings
from analysis import models as am
from dashboard import models as dm
from datasets import models as dam


class QCBase(BaseTest):
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
        "ioninj": False,
        "matchedpeaks": {
            "q1": 10.0,
            "q2": 14.0,
            "q3": 17.0
            }, 
        "scores": {
            "q1": 0.29441416,
            "q2": 0.45151624,
            "q3": 0.5780469
            },
        "retention_times": {
            "q1": 15.705895,
            "q2": 24.795988,
            "q3": 32.635075
            },
        "precursor_errors": {
            "q1": 1.1175711,
            "q2": 2.3226476,
            "q3": 3.9190276
            },
        "fwhms": {
            "q1": 0.07856697541417468,
            "q2": 0.11463563697349244,
            "q3": 0.19013413110220512
            },
        "ionmobilities": {
            "q1": 0.89153504,
            "q2": 0.99197274,
            "q3": 1.1201969
            },
        "peptide_areas": {
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
            "2": 7
            },
        "peaks_fwhm": 3.012,
        "scores": {
            "q1": 1.31007038817188e-06,
            "q2": 4.847267973673297e-06,
            "q3": 2.0830304492847063e-05
            },
        "retention_times": {
            "q1": 13.748827934265137,
            "q2": 21.873414993286133,
            "q3": 29.986976623535156
            },
        "fwhms": {
            "q1": 0.07407437451183796,
            "q2": 0.08781002461910248,
            "q3": 0.10342045687139034
            },
        "ionmobilities": {
            "q1": 0.89760422706604,
            "q2": 0.9681817889213562,
            "q3": 1.0567045211791992
            },
        "peptide_areas": {
            "q1": 24022.95,
            "q2": 50979.8,
            "q3": 122692.25
            }
        }


class QCSave(QCBase):
    url = '/jobs/set/longqc/'

    def test_bad_requests(self):
        resp = self.cl.post(self.url, content_type='application/json', data={'client_id': 'wrong'})
        self.assertEqual(resp.status_code, 403)
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'rf_id': self.tmpraw.pk}
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('Missing parameter', resp.json()['msg'])
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'rf_id': self.tmpraw.pk, 'plots': {},
                'analysis_id': 1, 'acqtype': 'ADD', 'state': 'not ok', 'msg': 'test fail'} 
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 400)
        self.assertIn('Incorrect acquisition type', resp.json()['msg'])

    def test_save_qcrun_dia(self):
        # Create analysis
        ana = am.Analysis.objects.create(user=self.user, name='testana_qc', storage_dir='testdirqc')
        ##
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'rf_id': self.tmpraw.pk,
                'analysis_id': ana.pk, 'acqtype': 'DIA', 'state': 'ok', 'msg': 'test ok',
                'plots': self.diaplots}
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 200)
        qcd_q = dm.QCData.objects.filter(analysis=ana, rawfile=self.tmpraw,
                runtype=dam.AcquisistionMode.DIA)
        self.assertTrue(qcd_q.exists())
        qcd = qcd_q.get()
        bp = qcd.boxplotdata_set.get(datatype=dm.QuartileDataTypes.SCORE)
        self.assertEqual(bp.q2, post['plots']['scores']['q2'])
        lp = qcd.lineplotdata_set.get(datatype=dm.LineDataTypes.NRPSMS)
        self.assertEqual(lp.value, post['plots']['nrpsms'])
        self.assertFalse(qcd.boxplotdata_set.filter(datatype=dm.QuartileDataTypes.MASSERROR).exists())

    def test_save_dda_qc(self):
        # Create analysis
        ana = am.Analysis.objects.create(user=self.user, name='testana_qc', storage_dir='testdirqc')
        ##
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'rf_id': self.tmpraw.pk,
                'analysis_id': ana.pk, 'acqtype': 'DDA', 'state': 'ok', 'msg': 'test ok',
                'plots': self.ddaplots}
        resp = self.cl.post(self.url, content_type='application/json', data=post)
        self.assertEqual(resp.status_code, 200)
        qcd_q = dm.QCData.objects.filter(analysis=ana, rawfile=self.tmpraw,
                runtype=dam.AcquisistionMode.DDA)
        self.assertTrue(qcd_q.exists())
        qcd = qcd_q.get()
        bp = qcd.boxplotdata_set.get(datatype=dm.QuartileDataTypes.SCORE)
        self.assertEqual(bp.q2, post['plots']['scores']['q2'])
        lp = qcd.lineplotdata_set.get(datatype=dm.LineDataTypes.NRPSMS)
        self.assertEqual(lp.value, post['plots']['nrpsms'])
        self.assertFalse(qcd.lineplotdata_set.filter(datatype=dm.LineDataTypes.PEAKS_FWHM).exists())


class QCShow(QCBase):
    url = '/dash/longqc/'

    def test_show(self):
        ana = am.Analysis.objects.create(user=self.user, name='testana_qc', storage_dir='testdirqc')
        post = {'client_id': settings.ANALYSISCLIENT_APIKEY, 'rf_id': self.tmpraw.pk,
                'analysis_id': ana.pk, 'acqtype': 'DIA', 'state': 'ok', 'msg': 'test ok',
                'plots': self.diaplots}
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
        boxmap = {dm.QuartileDataTypes.FWHM: 'fwhms', dm.QuartileDataTypes.MASSERROR: 'perror',
                dm.QuartileDataTypes.PEPMS1AREA: 'peptide_areas',
                dm.QuartileDataTypes.SCORE: 'scores', dm.QuartileDataTypes.IONMOB: 'ionmobilities',
                dm.QuartileDataTypes.IONINJ: 'ioninj', dm.QuartileDataTypes.RT: 'retention_times',
                dm.QuartileDataTypes.MATCHED_PEAKS: 'matchedpeaks',
                }
        for pn, points in js['data'].items():
            if not len(points):
                continue
            elif 'value' in points[0]:
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
                    pass 
                else:
                    self.assertEqual(len(points), 1)
                    keys = [dm.LineDataTypes[pn]]
                    self.assertEqual(points[0]['value'], self.diaplots[pn])
                self.assertIn(points[0]['key'], keys)
                self.assertEqual(points[0]['date'], rawdate)

            elif 'q2' in points[0]:
                self.assertEqual(len(points), 1)
                self.assertEqual(points[0]['q2'], self.diaplots[boxmap[dm.QuartileDataTypes[pn]]]['q2'])
