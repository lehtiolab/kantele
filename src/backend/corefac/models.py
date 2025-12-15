from django.db import models

from datasets import models as dm


class PrepOptionProtocol(models.Model):
    doi = models.TextField(unique=True)
    registered = models.DateTimeField(auto_now_add=True)
    version = models.TextField()
    paramopt = models.ForeignKey(dm.SampleprepParameterOption, on_delete=models.CASCADE)
    active = models.BooleanField(default=True)


class SamplePipeline(models.Model):
    name = models.TextField(unique=True)


class PipelineVersion(models.Model):
    pipeline = models.ForeignKey(SamplePipeline, on_delete=models.CASCADE)
    version = models.TextField()
    timestamp = models.DateTimeField()
    # Once pipeline locked, it can not be saved anymore, except for delete/inactivation/activation
    locked = models.BooleanField(default=False)
    active = models.BooleanField(default=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['pipeline', 'version'], name='uni_pipelineversion')]


class PipelineEnzyme(models.Model):
    pipelineversion = models.ForeignKey(PipelineVersion, on_delete=models.CASCADE)
    enzyme = models.ForeignKey(dm.Enzyme, on_delete=models.PROTECT)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['enzyme_id', 'pipelineversion_id'], name='uni_pipelineenzyme')]


class PipelineStep(models.Model):
    pipelineversion = models.ForeignKey(PipelineVersion, on_delete=models.CASCADE)
    index = models.IntegerField()
    step = models.ForeignKey(PrepOptionProtocol, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['index', 'pipelineversion_id'], name='uni_pipelinestep')]


class DatasetPipeline(models.Model):
    '''A prep pipeline can be reused in multiple datasets if needed - result of a sample
    prep can theoretically be used in e.g. DIA / DDA runs, etc'''
    pipelineversion = models.ForeignKey(PipelineVersion, on_delete=models.CASCADE)
    dataset = models.OneToOneField(dm.Dataset, on_delete=models.CASCADE)


class TrackingStages(models.IntegerChoices):
    SAMPLESREADY = 1, 'Samples arrived'
    PREPSTARTED = 2, 'Prep started'
    PREPFINISHED = 3, 'Prep finished'
    MSQUEUED = 4, 'Queued on MS'


class DatasetPrepTracking(models.Model):
    dspipe = models.ForeignKey(DatasetPipeline, on_delete=models.CASCADE)
    stage = models.IntegerField(choices=TrackingStages.choices)
    timestamp = models.DateTimeField()

    class Meta:
        constraints = [models.UniqueConstraint(fields=['dspipe', 'stage'], name='uni_dspipe')]

class DatasetPrepTrackingNodate(models.Model):
    # For stages between PREPSTARTED and PREPFINISHED
    dspipe = models.ForeignKey(DatasetPipeline, on_delete=models.CASCADE)
    stage = models.ForeignKey(PipelineStep, on_delete=models.CASCADE)
    finished = models.BooleanField()

    class Meta:
        constraints = [models.UniqueConstraint(fields=['dspipe', 'stage'], name='uni_dspipenotrack')]


#class ILabToken(models.Model):
#    token = models.TextField()
#    # expires for warning
#    expires = models.DateTimeField()
#
#
#class ProjectIlab(models.Model):
#    project = models.ForeignKey(dm.Project, on_delete=models.CASCADE)
#    ilab_id = models.TextField()
#
#
#class Location(models.Model):
#    # freezers and such
#    name = models.TextField()
#
#
#class SampleLocation(models.Model):
#    # Create new on each location change
#    location = models.ForeignKey(Location, on_delete=models.CASCADE)
#    sample = models.ForeignKey(dm.ProjectSample, on_delete=models.CASCADE)
#    timestamp = models.DateTimeField(auto_now_add=True)
