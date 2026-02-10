from django.db import models
from django.contrib.auth.models import User

from kantele import settings
from analysis import models as am
from datasets import models as dm


class DsetMsgTypes(models.IntegerChoices):
    DELETE_SOON = 1
    DELETED = 2
    FILES_ARRIVED = 3 # FIXME test this too


class AnalysisMsgTypes(models.IntegerChoices):
    DELETE_SOON = 1
    DELETED = 2
    COMPLETED = 3


# Create your models here.
class UserMessage(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    txt = models.TextField()
    date = models.DateTimeField(auto_now_add=True)
    shown = models.BooleanField(default=False)
    deleted = models.BooleanField(default=False)

    @classmethod
    def create_message(cls, user_id, *, msgtype, dset_id=False, analysis_id=False):
        usermsg = cls(user_id=user_id)
        if dset_id and not DatasetMessage.objects.filter(dset_id=dset_id, msgtype=msgtype).exists():
            usermsg.save()
            dsmsg = DatasetMessage.objects.create(dset_id=dset_id, msgtype=msgtype, msg=usermsg)
            usermsg.txt = dsmsg.get_msg()
            usermsg.save()

        elif analysis_id and not AnalysisMessage.objects.filter(analysis_id=analysis_id,
                msgtype=msgtype).exists():
            usermsg.save()
            anamsg = AnalysisMessage.objects.create(analysis_id=analysis_id, msgtype=msgtype,
                    msg=usermsg)
            usermsg.txt = anamsg.get_msg()
            usermsg.save()



class DatasetMessage(models.Model):
    msgtype = models.IntegerField(choices=DsetMsgTypes.choices)
    dset = models.ForeignKey(dm.Dataset, on_delete=models.CASCADE)
    msg = models.ForeignKey(UserMessage, on_delete=models.CASCADE)

    def get_msg(self):
        proj = dm.Project.objects.filter(experiment__runname__dataset=self.dset).values('name').get()
        msgmap = {
                DsetMsgTypes.DELETE_SOON: f'Your dataset {self.dset_id} from project '
                                f'{proj["name"]} will expire in less than '
                                f'{settings.DATASET_EXPIRY_DAYS_MESSAGE} days',
                DsetMsgTypes.DELETED: f'Your dataset {self.dset_id} from project '
                                f'{proj["name"]} has expired and has been removed from storage',
                DsetMsgTypes.FILES_ARRIVED: f'Your dataset {self.dset_id} from project '
                                f'{proj["name"]} has new files that can be accepted',
                }
        return msgmap[self.msgtype]


class AnalysisMessage(models.Model):
    msgtype = models.IntegerField(choices=AnalysisMsgTypes.choices)
    analysis = models.ForeignKey(am.Analysis, on_delete=models.CASCADE)
    msg = models.ForeignKey(UserMessage, on_delete=models.CASCADE)

    def get_msg(self):
        msgmap = {
                AnalysisMsgTypes.DELETE_SOON: f'Your analysis {self.analysis.id} / '
                                f'{self.analysis.name} will expire in less than '
                                f'{settings.ANALYSIS_EXPIRY_DAYS_MESSAGE} days',
                AnalysisMsgTypes.DELETED: f'Your analysis {self.analysis.id} / '
                                f'{self.analysis.name} has expired and has been removed from storage',
                AnalysisMsgTypes.COMPLETED: f'Your analysis {self.analysis.id} / '
                                f'{self.analysis.name} has finished running',
                }
        return msgmap[self.msgtype]



