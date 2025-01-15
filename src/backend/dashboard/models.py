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


class LineplotData(models.Model):
    qcrun = models.ForeignKey(QCData, on_delete=models.CASCADE)
    value = models.FloatField()
    shortname = models.CharField(max_length=20)


class BoxplotData(models.Model):
    shortname = models.CharField(max_length=15)
    qcrun = models.ForeignKey(QCData, on_delete=models.CASCADE)
    upper = models.FloatField()
    lower = models.FloatField()
    q1 = models.FloatField()
    q2 = models.FloatField()
    q3 = models.FloatField()
