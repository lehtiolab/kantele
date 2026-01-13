import os
import re
from datetime import datetime

from django.db import models
from django.contrib.auth.models import User

from rawstatus import models as filemodels
from datasets import models as dsmodels
from jobs import models as jmodels


class LibraryFile(models.Model):
    description = models.TextField()
    sfile = models.OneToOneField(filemodels.StoredFile, on_delete=models.CASCADE)

    def __str__(self):
        return self.description


class FileParam(models.Model):
    name = models.TextField()
    nfparam = models.TextField()
    filetype = models.ForeignKey(filemodels.StoredFileType, on_delete=models.CASCADE)
    help = models.TextField()

    def __str__(self):
        return self.name


class Param(models.Model):
    class PTypes(models.IntegerChoices):
        FLAG = 1, 'Flag'
        MULTI = 2, 'Multiple choice checkbox'
        TEXT = 3, 'Text'
        NUMBER = 4, 'Numbers, integers, floats'
        SELECT = 5, 'Single choice select'

    name = models.TextField()
    nfparam = models.TextField()
    ptype = models.IntegerField(choices=PTypes.choices)
    visible = models.BooleanField(default=True)
    help = models.TextField()

    def __str__(self):
        return self.name


class ParamOption(models.Model):
    param = models.ForeignKey(Param, on_delete=models.CASCADE)
    name = models.TextField()
    value = models.TextField()

    def __str__(self):
        return '{} - {}'.format(self.param.name, self.name)


class ParameterSet(models.Model):
    name = models.TextField()

    def __str__(self):
        return self.name


class NextflowWorkflowRepo(models.Model):
    description = models.TextField(help_text='Description of workflow')
    repo = models.TextField()
    
    def __str__(self):
        return self.description


class NextflowWfVersionParamset(models.Model):
    update = models.TextField(help_text='Description of workflow update')
    # NB commit cannot be unique, in case of multiple paramsets
    commit = models.CharField(max_length=50)
    filename = models.TextField()
    nfworkflow = models.ForeignKey(NextflowWorkflowRepo, on_delete=models.CASCADE)
    paramset = models.ForeignKey(ParameterSet, on_delete=models.CASCADE)
    date = models.DateTimeField(auto_now=True)
    nfversion = models.TextField()
    active = models.BooleanField(default=False)
    
    def __str__(self):
        return '{} - {}'.format(self.nfworkflow.description, self.update)


class NfConfigFile(models.Model):
    serverprofile = models.ForeignKey(filemodels.AnalysisServerProfile, on_delete=models.CASCADE)
    nfpipe = models.ForeignKey(NextflowWfVersionParamset, on_delete=models.CASCADE)
    nfconfig = models.ForeignKey(LibraryFile, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['serverprofile', 'nfpipe', 'nfconfig'], name='uni_nfconfigfile')]



class UserWorkflow(models.Model):

    class WFTypeChoices(models.IntegerChoices):
        STD = 1, 'Quantitative proteomics'
        QC = 2, 'Instrument quality control'
        VARDB = 3, 'Other proteomics, special DB'
        DBGEN = 4, 'Proteogenomics DB generation'
        PISEP = 5, 'pI-separated identification'
        SPEC = 6, 'Special internal'
        LC = 7, 'Labelcheck'
        USER = 8, 'User-run workflow'

    name = models.TextField()
    wftype = models.IntegerField(choices=WFTypeChoices.choices)
    nfwfversionparamsets = models.ManyToManyField(NextflowWfVersionParamset)
    public = models.BooleanField()

    def __str__(self):
        return self.name


# FIXME 
# - Field names used are stored in experiment JSON
# - gene quant, other extra fields 
# - msstitch for extra fields (ExplainedIonCurrentRatio, NumMatchedMainIons)
class OutputFieldName(models.Model):
    '''To be user-friendly, store field names here using a description,
    so that it is easier to use the correct name'''
    description = models.TextField(help_text='Short description')
    fieldname = models.TextField()

    def __str__(self):
        return f'{self.description}: {self.fieldname}'


class WfOutput(models.Model):
    description = models.TextField() # E.g MSGF from dda 3.0
    # Sometimes fields will not be there at all! In that case just ignore
    psmfile = models.TextField() # e.g psmtable.txt, name of file
    pepfile = models.TextField()
    genefile = models.TextField()
    fasta_arg = models.TextField() # e.g. --tdb (cannot use FK bc we dont know if its multifile or not)
    peppeptidefield = models.ForeignKey(OutputFieldName, related_name='peppep', on_delete=models.CASCADE)
    peppepfield = models.ForeignKey(OutputFieldName, related_name='pepposterior', on_delete=models.CASCADE)
    psmprotfield = models.ForeignKey(OutputFieldName, related_name='prot', on_delete=models.CASCADE)
    psmms1field = models.ForeignKey(OutputFieldName, related_name='psmms1', on_delete=models.CASCADE)
    pepms1field = models.ForeignKey(OutputFieldName, related_name='pepms1', on_delete=models.CASCADE)
    pepfdrfield = models.ForeignKey(OutputFieldName, related_name='pepfdr', on_delete=models.CASCADE)
    psmfdrfield = models.ForeignKey(OutputFieldName, related_name='psmfdr', on_delete=models.CASCADE)
    psmpepfield = models.ForeignKey(OutputFieldName, related_name='psmposterior', on_delete=models.CASCADE)
    psmmzfield = models.ForeignKey(OutputFieldName, related_name='psmmz', on_delete=models.CASCADE)
    psmchargefield = models.ForeignKey(OutputFieldName, related_name='charge', on_delete=models.CASCADE)
    psmfnfield = models.ForeignKey(OutputFieldName, related_name='fn', on_delete=models.CASCADE)
    scanfield = models.ForeignKey(OutputFieldName, related_name='psmscan', on_delete=models.CASCADE)
    rtfield = models.ForeignKey(OutputFieldName, related_name='psmrt', on_delete=models.CASCADE)
    psmscorefield = models.ForeignKey(OutputFieldName, related_name='score', on_delete=models.CASCADE)
    psmsetname = models.ForeignKey(OutputFieldName, related_name='psmset', on_delete=models.CASCADE)
    psmpeptide = models.ForeignKey(OutputFieldName, related_name='psmpep', on_delete=models.CASCADE)
    genetablegenefield = models.ForeignKey(OutputFieldName, related_name='genegene', on_delete=models.CASCADE)

    def get_fasta_files(self, **jobkw):
        '''Fasta files need inspection of job parameters as there is no "proper" DB
        column for them in the analysis'''
        try:
            sfids = jobkw['multifiles'][self.fasta_arg]
        except KeyError:
            try:
                sfids = [jobkw['singlefiles'][self.fasta_arg]]
            except KeyError:
                return (1, False, 'Cannot find fasta files for this analysis with job arg '
                        f'{self.fasta_arg}, contact admin.')
        fa_files = filemodels.StoredFile.objects.filter(pk__in=sfids).values(
                'pk', 'servershare__name', 'path', 'filename')
        if fa_files.count() < len(sfids):
            return (1, False, 'Cannot find fasta files for this analysis with db ids '
                        f'{",".join([str(x) for x in sfids])}, contact admin.')
        return (0, fa_files, '')

    def get_psm_outfile(self, analysis):
        '''Returns the PSM file to be used, returns error in case of not found,
        or when the analysis is a "rerun from PSM table"
        '''
        b_ana_mgr = AnalysisBaseanalysis.objects.filter(analysis=analysis, rerun_from_psms=True)
        if b_ana_mgr.count():
            return (1, False, f'Cannot load results which have been generated from an '
            'existing PSM table (rerun from PSMs).')
        else:
            psmfile = analysis.analysisresultfile_set.filter(sfile__filename=self.psmfile)
        if psmfile.count() == 1:
            return (0, psmfile.values('sfile__servershare__name', 'sfile__path', 'sfile__filename'), '')
        elif psmfile.count() > 1:
            return (1, False, f'Multiple PSM files ({self.psmfile}) found for this analysis? Contact admin.')
        else:
            return (1, False, f'Cannot find output PSM file ({self.psmfile}) for this analysis.')

    def get_peptide_outfile(self, analysis):
        pepfile = analysis.analysisresultfile_set.filter(sfile__filename=self.pepfile)
        if pepfile.count() == 1:
            return (0, pepfile.values('sfile__servershare__name', 'sfile__path', 'sfile__filename'), '')
        elif pepfile.count() > 1:
            return (1, False, f'Multiple peptide files ({self.pepfile}) found for this analysis? Contact admin.')
        else:
            return (1, False, f'Cannot find output peptide file ({self.pepfile}) for this analysis.')

    def get_gene_outfile(self, analysis):
        '''Gene file is not always output'''
        genefile = analysis.analysisresultfile_set.filter(sfile__filename=self.genefile)
        if genefile.count() > 1:
            return (1, False, f'Multiple gene files ({self.genefile}) found for this analysis? Contact admin.')
        elif genefile.count():
            return (0, genefile.values('sfile__servershare__name', 'sfile__path', 'sfile__filename'), '')
        else:
            return (0, False, 'No gene file available in this analysis')
        


class PipelineVersionOutput(models.Model):
    '''Mapping a pipeline (version) to an output field definition. Multiple
    output definitions can be used for a single pipeline version, so that
    a pipeline can output e.g. different search engine PSMs'''
    nfwfversion = models.ForeignKey(NextflowWfVersionParamset, on_delete=models.CASCADE)
    output = models.ForeignKey(WfOutput, on_delete=models.CASCADE)


class PsetComponent(models.Model):
    '''Special components for a parameter set. Components are such elements for a workflow
    that require special, non generalized code written for it. They can be in multiple workflows
    but are not as generalized as e.g. parameters'''

    class ComponentChoices(models.IntegerChoices):
        ISOQUANT = 1, 'Isobaric quant summarizing with denominators, median sweep, or intensity'
        INPUTDEF = 2, 'Input file definition of specific type, value eg [path, instrument, sample]'
        ISOQUANT_SAMPLETABLE = 3, 'Sampletable for isobaric quant'
        LABELCHECK_ISO = 4, 'Labelcheck isoquant'
        COMPLEMENT_ANALYSIS = 5, 'MS search complementing earlier run or rerun from PSMs'
        PREFRAC = 6, 'Prefractionated MS data'
        HIRIEF_STRIP_TOLERANCE = 7, 'HiRIEF strip tolerance'
        REMOVE_CHANNEL = 8, 'Remove channel from experiment set'
        NO_DATASETS = 9, 'No datasets in analysis (e.g. static/external input file)'
        # DSET_NAMES ?

    pset = models.ForeignKey(ParameterSet, on_delete=models.CASCADE)
    component = models.IntegerField(choices=ComponentChoices.choices)
    value = models.JSONField(default=dict) 
    # JSON in value: if needed eg mzmldef: [path, instrument, set, plate, fr]
    # else {}
    # FIXME future also setnames, sampletables, fractions, etc which is not a param
    # to be included in parameterset
    # prefrac '.*fr([0-9]+).*mzML$'

    def __str__(self):
        return '{} - {}'.format(self.pset.name, self.ComponentChoices(self.component).label)


class PsetFileParam(models.Model):
    param = models.ForeignKey(FileParam, on_delete=models.CASCADE)
    allow_resultfiles = models.BooleanField(default=False)
    pset = models.ForeignKey(ParameterSet, on_delete=models.CASCADE)

    def __str__(self):
        return '{} -- {}'.format(self.pset.name, self.param.name)


class PsetMultiFileParam(models.Model):
    param = models.ForeignKey(FileParam, on_delete=models.CASCADE)
    allow_resultfiles = models.BooleanField(default=False)
    pset = models.ForeignKey(ParameterSet, on_delete=models.CASCADE)

    def __str__(self):
        return '{} -- {}'.format(self.pset.name, self.param.name)


class PsetParam(models.Model):
    param = models.ForeignKey(Param, on_delete=models.CASCADE)
    pset = models.ForeignKey(ParameterSet, on_delete=models.CASCADE)

    def __str__(self):
        return '{} -- {}'.format(self.pset.name, self.param.name)


class Analysis(models.Model):
    date = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    name = models.TextField()
    log = models.JSONField(default=list)
    deleted = models.BooleanField(default=False)
    # FIXME purged lost its meaning now? w sfl in multi location
    purged = models.BooleanField(default=False)
    base_rundir = models.TextField()
    editable = models.BooleanField(default=True)
    securityclass = models.IntegerField(choices=filemodels.DataSecurityClass.choices)

    def get_fullname(self, wftype=False):
        if wftype:
            pass
        elif hasattr(self, 'nextflowsearch'):
            wftype = self.nextflowsearch.workflow.wftype
        else:
            wftype = UserWorkflow.WFTypeChoices.USER
        shortname = UserWorkflow.WFTypeChoices(wftype).name
        return f'{shortname}_{self.name}'

    def get_run_base_dir(self, wftype=False):
        '''Used for setting self.base_rundir'''
        cleanname = re.sub('[^a-zA-Z0-9\.\-_]', '_', self.get_fullname(wftype))
        return (f'{self.pk}_{cleanname}_{datetime.strftime(self.date, "%Y%m%d_%H.%M")}')

    def get_public_output_dir(self):
        return os.path.join(self.user.username, self.get_run_base_dir())


# Can this be generalized to deleted log for also files?
class AnalysisDeleted(models.Model):
    analysis = models.OneToOneField(Analysis, on_delete=models.CASCADE)
    date = models.DateTimeField(auto_now_add=True)


class AnalysisError(models.Model):
    message = models.TextField()
    analysis = models.OneToOneField(Analysis, on_delete=models.CASCADE)


class ExternalAnalysis(models.Model):
    analysis = models.OneToOneField(Analysis, on_delete=models.CASCADE)
    description = models.TextField()
    # Deleting tokens happens, and we shouldnt lose the external analysis for it
    last_token = models.OneToOneField(filemodels.UploadToken, on_delete=models.PROTECT)


class NextflowSearch(models.Model):
    nfwfversionparamset = models.ForeignKey(NextflowWfVersionParamset, on_delete=models.CASCADE)
    workflow = models.ForeignKey(UserWorkflow, on_delete=models.CASCADE)
    analysis = models.OneToOneField(Analysis, on_delete=models.CASCADE)
    # token is for authentication of NF with-weblog
    token = models.TextField()
    job = models.OneToOneField(jmodels.Job, on_delete=models.CASCADE)


class AnalysisResultFile(models.Model):
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    sfile = models.ForeignKey(filemodels.StoredFile, on_delete=models.CASCADE)


class Proteowizard(models.Model):
    # When updating a version, NF pipeline will get updated, not passing container
    # into that here. So then new Proteowizard and new NextflowWfVersionParamset
    # Filters and options for pwiz are currently defined in view
    version_description = models.TextField()
    params = models.JSONField(default=dict, blank=True,
            help_text='E.g. {"centroid": "1,2", "mzmltool": "msconvert"}')
    date_added = models.DateTimeField(auto_now_add=True)
    nf_version = models.ForeignKey(NextflowWfVersionParamset, on_delete=models.CASCADE)
    active = models.BooleanField(default=True)


class MzmlFile(models.Model):
    sfile = models.OneToOneField(filemodels.StoredFile, on_delete=models.CASCADE)
    pwiz = models.ForeignKey(Proteowizard, on_delete=models.CASCADE)
    refined = models.BooleanField(default=False)


class EnsemblFasta(models.Model):
    libfile = models.OneToOneField(LibraryFile, on_delete=models.CASCADE)
    version = models.IntegerField()
    organism = models.TextField()


class UniProtFasta(models.Model):
    class UniprotClass(models.IntegerChoices):
        SWISS = 0, 'Swiss_canonical'
        SWISS_ISOFORMS = 1, 'Swiss_canonical_isoforms'
        REFERENCE = 2, 'Reference_proteome'
        REFERENCE_ISOFORMS = 3, 'Reference_proteome_isoforms'
    url_addons = {'SWISS': ' AND (reviewed:true)',
            'SWISS_ISOFORMS': ' AND (reviewed:true)&includeIsoform=true',
            'REFERENCE': '',
            'REFERENCE_ISOFORMS': '&includeIsoform=true',
            }

    libfile = models.OneToOneField(LibraryFile, on_delete=models.CASCADE)
    version = models.TextField()
    organism = models.TextField()
    #organismcode = models.IntegerField()
    dbtype = models.IntegerField(choices=UniprotClass.choices)


class AnalysisParam(models.Model):
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    param = models.ForeignKey(Param, on_delete=models.CASCADE)
    value = models.JSONField() # can be option_id value, list of those, bool, or text/number input

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'param'], name='uni_anaparam')]


class AnalysisSampletable(models.Model):
    analysis = models.OneToOneField(Analysis, on_delete=models.CASCADE)
    samples = models.JSONField()
    # FIXME could we instead do: four columns channel - set - sample - group ?
    # Doesnt give a big improvement, if wf sampletable format will change, this DB table will change
    # But then we can keep representation correct instead of leaving the JSON intact
    # I don't see the JSON benefit except somewhat easier because it's passed around as JSON a lot
    # Added benefit: clearer DB representation, stricter


class AnalysisSetname(models.Model):
    '''All set or sample names in an analysis that are per dataset'''
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    setname = models.TextField()

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'setname'], name='uni_anasets')]


class DatasetAnalysis(models.Model):
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    dataset = models.ForeignKey(dsmodels.Dataset, on_delete=models.CASCADE)
    # cannot put setname here because of searches without dset/setname
    # model used in reporting, and also for finding datasets for base analysis etc
    # and in mstulos for coupling dset/analysis, and for external analyses ("just dataset", 
    # no input files etc are stored)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'dataset'], name='uni_dsa_anadsets')]


class AnalysisDatasetSetValue(models.Model):
    '''Dataset mapping to setnames (multiple dataset can have the same setname)'''
    # Note that datasets can be deleted, or have their file contents changed
    # That means this is not to be trusted for future bookkeeping of what was in the analysis
    # For that, you should combine it with using the below AnalysisDSInputFile model
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    dataset = models.ForeignKey(dsmodels.Dataset, on_delete=models.CASCADE)
    setname = models.ForeignKey(AnalysisSetname, on_delete=models.CASCADE, null=True)
    field = models.TextField()
    value = models.TextField()

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'dataset', 'field'], name='uni_anadsetsfields')]


class AnalysisDSInputFile(models.Model):
    '''Input files for set-based analysis (isobaric and prefraction-datasets)'''
    dsanalysis = models.ForeignKey(DatasetAnalysis, on_delete=models.CASCADE)
    sfile = models.ForeignKey(filemodels.StoredFile, on_delete=models.CASCADE)
    analysisset = models.ForeignKey(AnalysisSetname, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysisset', 'sfile'], name='uni_anaset_infile')]


class AnalysisFileValue(models.Model):
    '''If one sample per file is used in labelfree analyses, the samples are stored
    here'''
    # this assumes at least one entry of this model per file/analysis
    # (for non-set data), so samplename is a field. This is the only mapping of
    # file/analysis we have currently for non-set data. If there's ever need
    # of mapping files WITHOUT field/value for an analysis, we can break out
    # to an extra model, alternatively null the fields

    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    field = models.TextField()
    value = models.TextField()
    sfile = models.ForeignKey(filemodels.StoredFile, on_delete=models.CASCADE)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'sfile', 'field'], name='uni_anassamplefile')]


class AnalysisIsoquant(models.Model):
    # FIXME can we not split the JSON field into denoms -JSON, 2x boolean, default=False?
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    setname = models.ForeignKey(AnalysisSetname, on_delete=models.CASCADE)
    #{denoms: [ch_id, ch_id], sweep: false, report_intensity: false, remove: [ch_id, ch_id]}
    value = models.JSONField() 

    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'setname'], name='uni_anaiso')]

class AnalysisFileParam(models.Model):
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    param = models.ForeignKey(FileParam, on_delete=models.CASCADE)
    sfile = models.ForeignKey(filemodels.StoredFile, on_delete=models.CASCADE)

    # Multifiles makes this useless? Can still put in more files than needed with a race, just not
    # duplicates FIXME
    class Meta:
        constraints = [models.UniqueConstraint(fields=['analysis', 'param', 'sfile'], name='uni_anafileparam')]


class AnalysisBaseanalysis(models.Model):
    analysis = models.OneToOneField(Analysis, on_delete=models.CASCADE, related_name='analysis')
    base_analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE, related_name='base_analysis')
    is_complement = models.BooleanField(default=False)
    rerun_from_psms = models.BooleanField(default=False)
    # we can theoretically get shadow_ from analysisisoquant, but what if we have a recursive base analysis
    # then we have to get the base analysis' base analysis isoquant. Instead accumulate here in JSON
    shadow_isoquants = models.JSONField() # {setname: {ch: [sample, chid]} ...}
    shadow_dssetnames = models.JSONField() # {dsid: {setname: abc, regex: .*fr01} ...}
