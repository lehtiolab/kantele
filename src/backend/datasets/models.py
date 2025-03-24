from django.db import models
from django.contrib.auth.models import User
from rawstatus.models import RawFile, ServerShare
from jobs.models import Job


class PrincipalInvestigator(models.Model):
    name = models.TextField(max_length=100)

    def __str__(self):
        return self.name


class ProjectTypeName(models.Model):
    # Local/research, corefac, consortiumABC, etc
    # In future, this can be used for filtering in project reporting, as
    # well as assigning if data is allowed on specific server
    name = models.TextField()


class Project(models.Model):
    name = models.TextField(unique=True)
    pi = models.ForeignKey(PrincipalInvestigator, on_delete=models.CASCADE)
    active = models.BooleanField(default=True)
    registered = models.DateTimeField(auto_now_add=True)
    ptype = models.ForeignKey(ProjectTypeName, on_delete=models.CASCADE)
    # external ref for things like iLab API or something, can be blank
    externalref = models.TextField()


class ProjLogLevels(models.IntegerChoices):
    OPEN = 1, 'Opened'
    CLOSE = 2, 'Closed'
    INFO = 3, 'Info'
    # Security logs are for "user allowed to do X", "user X accessed sensitive data"
    SECURITY = 4, 'Security'
    # Critical will reach admin
    CRITICAL = 5, 'Critical'


class ProjectLog(models.Model):
    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    date = models.DateTimeField(auto_now_add=True)
    level = models.IntegerField(choices=ProjLogLevels.choices)
    message = models.TextField()


class UserPtype(models.Model):
    ptype = models.ForeignKey(ProjectTypeName, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)


class Experiment(models.Model):
    name = models.TextField()
    project = models.ForeignKey(Project, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['name', 'project'], name='uni_expproj')]


class RunName(models.Model):
    name = models.TextField(max_length=100)
    experiment = models.ForeignKey(Experiment, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['name', 'experiment'], name='uni_runexp')]


class Species(models.Model):
    linnean = models.TextField(unique=True)
    popname = models.TextField()


class Datatype(models.Model):
    name = models.TextField(max_length=100)
    public = models.BooleanField(default=True)

    def __str__(self):
        return self.name


class DatasetUIComponent(models.IntegerChoices):
    FILES = 1, 'Files'
    SAMPLES = 2, 'Samples'
    ACQUISITION = 3, 'MS Acquisition'
    DEFINITION = 4, 'Definition'
    SAMPLEPREP = 5, 'MS Sampleprep'
    LCSAMPLES = 6, 'LC samples'
    POOLEDLCSAMPLES = 7, 'Pooled LC samples'


class DatatypeComponent(models.Model):
    datatype = models.ForeignKey(Datatype, on_delete=models.CASCADE)
    component = models.IntegerField(choices=DatasetUIComponent.choices)

    def __str__(self):
        return f'{self.datatype.name} has component {DatasetUIComponent(self.component).label}'


class DatasetSecurityClass(models.IntegerChoices):
    # Go from lowest to highest classification
    NOSECURITY = 1, 'Not classified'
    # FIXME when ready, also have personal data dsets
    # PERSONAL = 2, 'Personal data'


class Dataset(models.Model):
    date = models.DateTimeField('date created')
    runname = models.OneToOneField(RunName, on_delete=models.CASCADE)
    #experiment = models.ForeignKey(Experiment, on_delete=models.CASCADE)
    #runname = models.TextField()
    datatype = models.ForeignKey(Datatype, on_delete=models.CASCADE)
    securityclass = models.IntegerField(choices=DatasetSecurityClass.choices)
    # NB! storage_loc/share should only ever be updated in jobs' post-run (after moves)
    # because it is source of truth for where to/from move files
    storage_loc = models.TextField(max_length=200, unique=True)
    storageshare = models.ForeignKey(ServerShare, on_delete=models.CASCADE)
    deleted = models.BooleanField(default=False) # for UI only, indicate deleted from active storage
    purged = models.BooleanField(default=False) # for UI only, indicate permanent deleted from cold storage too
    locked = models.BooleanField(default=False)


class DatasetOwner(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)


class DCStates(models.IntegerChoices):
    OK = 1, 'OK'
    NEW = 2, 'New' # 
    INCOMPLETE = 3, 'Incomplete'
    ERROR = 4, 'Error'


class DatasetComponentState(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    dtcomp = models.ForeignKey(DatatypeComponent, on_delete=models.CASCADE)
    state = models.IntegerField(choices=DCStates.choices)
    # timestamp (when is saved/updated)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['dataset', 'dtcomp'], name='uni_dscomp')]


class DatasetRawFile(models.Model):
    # FIXME Restrict to single filetype per dataset somehow
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    rawfile = models.OneToOneField(RawFile, on_delete=models.CASCADE)


class DatasetSpecies(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    species = models.ForeignKey(Species, on_delete=models.CASCADE)


class ParamType(models.Model):
    typename = models.TextField()

    def __str__(self):
        return self.typename


class Labcategories(models.IntegerChoices):
    # FIXME should these be folded into the Datatype categories?
    # Even if they are only two?
    ACQUISITION = 1, 'MS Acquisition'
    SAMPLEPREP = 2, 'MS Sample prep'


class SampleprepParameter(models.Model):
    title = models.TextField(unique=True)
    active = models.BooleanField(default=True)

    def __str__(self):
        return self.title


class SampleprepParameterOption(models.Model):
    param = models.ForeignKey(SampleprepParameter, on_delete=models.CASCADE)
    value = models.TextField()
    active = models.BooleanField(default=True)

    def __str__(self):
        return self.value

    class Meta:
        constraints = [models.UniqueConstraint(fields=['param_id', 'value'], name='uni_sprepopt')]


class SampleprepParameterValue(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    value = models.ForeignKey(SampleprepParameterOption, on_delete=models.CASCADE)


class FieldParameter(models.Model):
    # adminable
    title = models.TextField()
    placeholder = models.TextField()
    paramtype = models.ForeignKey(ParamType, on_delete=models.CASCADE)
    category = models.IntegerField(choices=Labcategories.choices)
    active = models.BooleanField(default=True)

    def __str__(self):
        return self.title


class FieldParameterValue(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    param = models.ForeignKey(FieldParameter, on_delete=models.CASCADE)
    value = models.TextField()


class CheckboxParameter(models.Model):
    # adminable
    title = models.TextField()
    category = models.IntegerField(choices=Labcategories.choices)
    active = models.BooleanField(default=True)

    def __str__(self):
        return self.title


class CheckboxParameterOption(models.Model):
    param = models.ForeignKey(CheckboxParameter, on_delete=models.CASCADE)
    value = models.TextField()

    def __str__(self):
        return self.value


class CheckboxParameterValue(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    value = models.ForeignKey(CheckboxParameterOption, on_delete=models.CASCADE)


class Enzyme(models.Model):
    name = models.TextField()

    def __str__(self):
        return self.name


class EnzymeDataset(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    enzyme = models.ForeignKey(Enzyme, on_delete=models.CASCADE)


class QuantType(models.Model):
    name = models.TextField(unique=True)
    shortname = models.TextField()

    def __str__(self):
        return self.name


class QuantChannel(models.Model):
    name = models.TextField()

    def __str__(self):
        return self.name


class QuantTypeChannel(models.Model):
    quanttype = models.ForeignKey(QuantType, on_delete=models.CASCADE)
    channel = models.ForeignKey(QuantChannel, on_delete=models.CASCADE)

    def __str__(self):
        return '{} - {}'.format(self.quanttype.name, self.channel.name)


class QuantDataset(models.Model):
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    quanttype = models.ForeignKey(QuantType, on_delete=models.CASCADE)


class SampleMaterialType(models.Model):
    '''Different kind of samples, e.g. tissue, cell culture, plasma'''
    name = models.TextField()


class ProjectSample(models.Model):
    sample = models.TextField()
    project = models.ForeignKey(Project, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['sample', 'project'], name='uni_sampleproj')]


class SampleMaterial(models.Model):
    sample = models.ForeignKey(ProjectSample, on_delete=models.CASCADE)
    sampletype = models.ForeignKey(SampleMaterialType, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['sample', 'sampletype'], name='uni_sampletype')]


class SampleSpecies(models.Model):
    sample = models.ForeignKey(ProjectSample, on_delete=models.CASCADE)
    species = models.ForeignKey(Species, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['sample', 'species'], name='uni_samplespecies')]


class DatasetSample(models.Model):
    '''Reporting model for keeping track of samples in datasets'''
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    projsample = models.ForeignKey(ProjectSample, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['projsample', 'dataset'], name='uni_samds')]


class QuantSampleFile(models.Model):
    rawfile = models.OneToOneField(DatasetRawFile, on_delete=models.CASCADE)
    projsample = models.ForeignKey(ProjectSample, on_delete=models.CASCADE)


class QuantChannelSample(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    channel = models.ForeignKey(QuantTypeChannel, on_delete=models.CASCADE)
    projsample = models.ForeignKey(ProjectSample, on_delete=models.CASCADE)


class QuantFileChannel(models.Model):
    '''In non-pooled labelchecks the mapping is single-channel files'''
    channel = models.ForeignKey(QuantTypeChannel, on_delete=models.CASCADE)
    dsrawfile = models.OneToOneField(DatasetRawFile, on_delete=models.CASCADE)
    

class HiriefRange(models.Model):
    # adminable
    start = models.DecimalField(max_digits=5, decimal_places=2)
    end = models.DecimalField(max_digits=5, decimal_places=2)

    def __str__(self):
        return '{} - {}'.format(self.start, self.end)

    def get_path(self):
        return '{}_{}'.format(self.start, self.end)


class ExternalDatasetContact(models.Model):
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    email = models.TextField()


class Operator(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    def __str__(self):
        return '{} {}'.format(self.user.first_name, self.user.last_name)


class OperatorDataset(models.Model):
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    operator = models.ForeignKey(Operator, on_delete=models.CASCADE)


class AcquisistionMode(models.IntegerChoices):
    DDA = 1, 'DDA'
    DIA = 2, 'DIA'


class AcquisistionModeDataset(models.Model):
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    acqmode = models.IntegerField(choices=AcquisistionMode.choices)


class ReversePhaseDataset(models.Model):
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    length = models.TextField()


class Prefractionation(models.Model):
    name = models.TextField()

    def __str__(self):
        return self.name


class PrefractionationDataset(models.Model):
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    prefractionation = models.ForeignKey(Prefractionation, on_delete=models.CASCADE)


class HiriefDataset(models.Model):
    pfdataset = models.OneToOneField(PrefractionationDataset, on_delete=models.CASCADE)
    hirief = models.ForeignKey(HiriefRange, on_delete=models.CASCADE)


class PrefractionationLength(models.Model):
    pfdataset = models.OneToOneField(PrefractionationDataset, on_delete=models.CASCADE)
    length = models.TextField()


class PrefractionationFractionAmount(models.Model):
    pfdataset = models.OneToOneField(PrefractionationDataset, on_delete=models.CASCADE)
    fractions = models.IntegerField()


class DatasetJob(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
