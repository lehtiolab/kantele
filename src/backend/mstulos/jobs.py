import re
import os

from django.db.models import Q

from kantele import settings
from jobs.jobs import BaseJob
from mstulos import models as m
from mstulos import tasks as mt
from analysis import models as am
from rawstatus import models as rm


# DDA pipeline 2.8 and up have OK format, before that: amount/count
# before 2.6 there is X__POOL
# before 2.3 # PSMs is not correct (identical)

class ProcessAnalysis(BaseJob): 
    refname = 'ingest_search_results'
    task = mt.summarize_result_peptable
    queue = settings.QUEUE_SEARCH_INBOX

    """
    kwargs = {
    'analysis_id': 1,
    'token': abcdd1234 ,
    """
    # FIXME this is not atomic view, if crash -> DB entries!
    # maybe instead of job, put some of this in view that created/saved analysis
   
    def process(self, **kwargs):
        analysis = am.Analysis.objects.select_related('nextflowsearch__nfwfversionparamset').get(
                pk=kwargs['analysis_id'])
        # First get isobaric types:
        plexq = Q(dataset__quantdataset__quanttype__shortname__contains='plex')
        plexq |= Q(dataset__quantdataset__quanttype__shortname='tmtpro')
        isob_types = []
        all_pepfile_arg, all_psmfile_arg, all_genefile_arg, all_fa_files = {}, {}, {}, {}
        for plextype in analysis.datasetanalysis_set.filter(plexq).distinct(
                'dataset__quantdataset__quanttype__shortname'):
            ptname = plextype.dataset.quantdataset.quanttype.shortname
            plextype_trf = {'tmtpro': 'tmt16plex'}.get(ptname, ptname)
            isob_types.append(plextype_trf)
        
        # Pass all WfOuput objects mapped for the used pipeline
        headers, fa_files, pepfile_arg, psmfile_arg = {}, {}, {}, {}
        for pipe_out in analysis.nextflowsearch.nfwfversionparamset.pipelineversionoutput_set.select_related('output').all():
            output = pipe_out.output
            # Output files headers according to their DB entries
            headers[output.pk] = {
                    'isobaric': isob_types,
                    'pep': {
                        'fdr': output.pepfdrfield.fieldname,
                        'posterior': output.peppepfield.fieldname,
                        'peptide': output.peppeptidefield.fieldname,
                        'ms1': output.pepms1field.fieldname,
                        },
                    'psm': {
                        'fdr': output.psmfdrfield.fieldname,
                        'posterior': output.psmpepfield.fieldname,
                        'fn': output.psmfnfield.fieldname,
                        'scan': output.scanfield.fieldname,
                        'setname': output.psmsetname.fieldname,
                        'peptide': output.psmpeptide.fieldname,
                        'mz': output.psmmzfield.fieldname,
                        'charge': output.psmchargefield.fieldname,
                        'score': output.psmscorefield.fieldname,
                        'ms1': output.psmms1field.fieldname,
                        'rt': output.rtfield.fieldname,
                        'protein': output.psmprotfield.fieldname,
                        },
                    'gene': {
                        'genename': output.genetablegenefield.fieldname,
                        },
                        }

            # Get fasta files
            fa_rc, fa_files, faerr = output.get_fasta_files(**analysis.nextflowsearch.job.kwargs['inputs'])
            psm_rc, psmfile, psmerr = output.get_psm_outfile(analysis)
            pep_rc, pepfile, peperr = output.get_peptide_outfile(analysis)
            gene_rc, genefile, geneerr = output.get_gene_outfile(analysis)
            
            if fa_rc or psm_rc or pep_rc or gene_rc:
                raise RuntimeError('\n'.join([faerr, psmerr, peperr, geneerr]).strip())

            psmfile = psmfile.get()
            pepfile = pepfile.get()
            genefile = genefile.get() if genefile else False

            all_fa_files[output.pk] = [(x['pk'], x['servershare__name'], os.path.join(x['path'], x['filename']))
                    for x in fa_files]
            all_pepfile_arg[output.pk] = (pepfile['sfile__servershare__name'],
                    os.path.join(pepfile['sfile__path'], pepfile['sfile__filename']))
            all_psmfile_arg[output.pk] = (psmfile['sfile__servershare__name'],
                    os.path.join(psmfile['sfile__path'], psmfile['sfile__filename']))
            if genefile:
                all_genefile_arg[output.pk] = (genefile['sfile__servershare__name'],
                        os.path.join(genefile['sfile__path'], genefile['sfile__filename']))

        self.run_tasks.append(((kwargs['token'], kwargs['organism_id'], all_pepfile_arg, all_psmfile_arg, all_genefile_arg, headers, all_fa_files), {}))
