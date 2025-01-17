from django.db import models

from rawstatus import models as rm
from analysis import models as am
from datasets import models as dm


class QCData(models.Model):
    rawfile = models.ForeignKey(rm.RawFile, on_delete=models.CASCADE)
    analysis = models.ForeignKey(am.Analysis, on_delete=models.CASCADE)
    is_ok = models.BooleanField(default=True)
    message = models.TextField()
    runtype = models.IntegerField(choices=dm.AcquisistionMode.choices)


class LineDataTypes(models.IntegerChoices):
    NRPROTEINS = 1, 'Nr of proteins'
    NRPEPTIDES = 2, 'Nr of peptides'
    NRPEPTIDES_UNI = 3, 'Nr of peptides mapping uniquely'
    NRPSMS = 4, 'Nr of PSMs'
    NRSCANS = 5, 'Nr of scans'
    MISCLEAV1 = 6, 'Nr of PSMs with 1 missed cleavage'
    MISCLEAV2 = 7, 'Nr of PSMs with 2 missed cleavage'
    PEAKS_FWHM = 8, 'Amount scans FWHM'


class QuartileDataTypes(models.IntegerChoices):
    FWHM = 1, 'FWHM'
    PEPMS1AREA = 2, 'Peptide MS1 area'
    MASSERROR = 3, 'Mass error'
    RT = 4, 'Retention time'
    SCORE = 5, 'Score'
    IONMOB = 6, 'Ion mobility'
    IONINJ = 7, 'Ion injection time(ms)'
    MATCHED_PEAKS = 8, 'Amount MS2 peaks matched'


class LineplotData(models.Model):
    qcrun = models.ForeignKey(QCData, on_delete=models.CASCADE)
    value = models.FloatField()
    datatype = models.IntegerField(choices=LineDataTypes.choices)


class BoxplotData(models.Model):
    datatype = models.IntegerField(choices=QuartileDataTypes.choices)
    qcrun = models.ForeignKey(QCData, on_delete=models.CASCADE)
    q1 = models.FloatField()
    q2 = models.FloatField()
    q3 = models.FloatField()
