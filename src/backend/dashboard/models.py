from django.db import models

from rawstatus import models as rm
from analysis import models as am
from datasets import models as dm


class QCRun(models.Model):
    rawfile = models.OneToOneField(rm.RawFile, on_delete=models.CASCADE)
    analysis = models.OneToOneField(am.Analysis, on_delete=models.CASCADE)
    is_ok = models.BooleanField(default=True)
    message = models.TextField()
    runtype = models.IntegerField(choices=dm.AcquisistionMode.choices)


class TrackedPeptide(models.Model):
    sequence = models.TextField(unique=True)
    charge = models.IntegerField()


class TrackedPeptideSet(models.Model):
    date = models.DateTimeField(auto_now=True)
    name = models.TextField(unique=True) # e.g. v1-synthetic-peptides
    acqmode = models.IntegerField(choices=dm.AcquisistionMode.choices)
    active = models.BooleanField(default=False)
    frozen = models.BooleanField(default=False)
    

class PeptideInSet(models.Model):
    peptideset = models.ForeignKey(TrackedPeptideSet, on_delete=models.CASCADE)
    peptide = models.ForeignKey(TrackedPeptide, on_delete=models.CASCADE)


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
    qcrun = models.ForeignKey(QCRun, on_delete=models.CASCADE)
    value = models.FloatField()
    datatype = models.IntegerField(choices=LineDataTypes.choices)


class BoxplotData(models.Model):
    datatype = models.IntegerField(choices=QuartileDataTypes.choices)
    qcrun = models.ForeignKey(QCRun, on_delete=models.CASCADE)
    q1 = models.FloatField()
    q2 = models.FloatField()
    q3 = models.FloatField()


class PeptideTrackData(models.Model):
    datatype = models.IntegerField(choices=QuartileDataTypes.choices)
    qcrun = models.ForeignKey(QCRun, on_delete=models.CASCADE)
    value = models.FloatField()
    # Peptide deletion is not allowed if there are data points!
    peptide = models.ForeignKey(TrackedPeptide, on_delete=models.PROTECT)
