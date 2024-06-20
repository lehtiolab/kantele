import re
import json
from base64 import b64decode
from uuid import uuid4

from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.contrib.postgres.aggregates import ArrayAgg
from django.views.decorators.http import require_POST
from django.db.models import Q, Count, F
from django.db.models.functions import Upper
from django.core.paginator import Paginator

from mstulos import models as m
from jobs import views as jv
from jobs.jobutil import create_job
from datasets import models as dm
from analysis import models as am


# FIXME:
# fix fixmes in job/task
# have shareable URLs for search including unrolls
# create plots for TMT
# gene/protein centric tables

def paginate(qset, pnr):
    pages = Paginator(qset, 100)
    pages.ELLIPSIS = '__'
    try:
        pnr = int(pnr)
    except ValueError:
        pnr = 1
    page = pages.get_page(pnr)
    page_context = {'last_res_nr': page.end_index(), 'total_res_nr': pages.count,
            'first_res_nr': page.start_index(), 'page_nr': pnr,
            'pagerange': [x for x in pages.get_elided_page_range(pnr, on_each_side=2, on_ends=1)]}
    return page, page_context


@login_required
@require_POST
def add_analysis(request, nfs_id):
    analysis = am.Analysis.objects.select_related('nextflowsearch__nfwfversionparamset__wfoutput').get(
            nextflowsearch__pk=nfs_id)
    # First do checks:
    organisms = set()
    for dsa in analysis.datasetanalysis_set.all():
        dsorganisms = set()
        for dss in dsa.dataset.datasetsample_set.all():
            dsorganisms.update(x.species_id for x in dss.projsample.samplespecies_set.all())
        # FIXME also check DIA/DDA here
        if not len(dsorganisms):
            return JsonResponse({'error': True, 'message': 'Must enter organism in dataset metadata in order to load results'})
        organisms.update(dsorganisms)
    if len(organisms) > 1:
        return JsonResponse({'error': True, 'message': 'Multiple organism-datasets are not possible to load in result service'})

    exp, _cr = m.Experiment.objects.get_or_create(analysis=analysis, defaults={'token': str(uuid4())})
    if not _cr and exp.upload_complete:
        return JsonResponse({'error': True, 'message': 'This analysis is already in the results database'})
#        # TODO currently only recent and isobaric data, as  a start
#        # figure out how we store one file/sample -> analysisfilesample
#        # and labelfree fractions?  -> analysisdset probbaly
#        if not hasattr(analysis, 'analysissampletable'):
#            raise RuntimeError('Cannot process analysis without sampletable as conditions currently')

    create_job('ingest_search_results', analysis_id=analysis.pk, token=exp.token,
            organism_id=organisms.pop())
    return JsonResponse({})


@require_POST
def init_store_experiment(request):
    # Delete all conditions before rerunning task, both because since it is not possible to only
    # get_or_create on name/exp, as there are duplicates in the DB e.g. multiple sets with
    # TMT channel 126 and:
    # It also obviates the need to do get_or_create on a lot of fields running the task
    # storing the data -> only create is faster since it skips the get query
    data = json.loads(request.body.decode('utf-8'))
    exp = m.Experiment.objects.get(token=data['token'])
    m.Condition.objects.filter(experiment_id=exp).delete()
    samplesets = {}
    # FIXME non-set searches (have analysisdsinputfile), also non-sampletable (same?)
    for asn in exp.analysis.analysissetname_set.all():
        c_setn = m.Condition.objects.create(name=asn.setname,
                cond_type=m.Condition.Condtype['SAMPLESET'], experiment=exp)
        sampleset = {'set_id': c_setn.pk, 'files': {}}
        regex_db = asn.analysisdatasetsetvalue_set.filter(field='__regex')
        regex = regex_db.get().value if regex_db.exists() else False
        for dsf in asn.analysisdsinputfile_set.all():
            c_fn = m.Condition.objects.create(name=dsf.sfile.filename,
                    cond_type=m.Condition.Condtype['FILE'], experiment=exp)
            sampleset['files'][dsf.sfile.filename] = c_fn.pk
            if regex:
                frnum = re.match(regex, dsf.sfile.filename).group(1)
                c_fr = m.Condition.objects.create(name=frnum,
                        cond_type=m.Condition.Condtype['FRACTION'], experiment=exp)
                c_fn.parent_conds.add(c_fr)
            c_fn.parent_conds.add(c_setn)
        clean_set = re.sub('[^a-zA-Z0-9_]', '_', asn.setname)
        samplesets[clean_set] = sampleset

    # Sample name and group name are repeated in sampletable so they use get_or_create
    # Can also do that because they cant be duplicate in experiment, like e.g.
    # fractions or channels over multiple sets
    # TODO exempt them from deletion above?
    samples = {'groups': {}, 'samples': {}, }
    for ch, setn, sample, sgroup in exp.analysis.analysissampletable.samples:
        clean_group = re.sub('[^a-zA-Z0-9_]', '_', sgroup)
        clean_sample = re.sub('[^a-zA-Z0-9_]', '_', sample)
        clean_set = re.sub('[^a-zA-Z0-9_]', '_', setn)
        if sgroup != '':
            gss = f'{clean_group}_{clean_sample}_{clean_set}___{ch}'
            c_group, _cr = m.Condition.objects.get_or_create(name=sgroup,
                    cond_type=m.Condition.Condtype['SAMPLEGROUP'], experiment=exp)
            samples['groups'][gss] = c_group.pk
        else:
            gss = f'{clean_sample}_{clean_set}___{ch}'
        c_sample, _cr = m.Condition.objects.get_or_create(name=sample,
                cond_type=m.Condition.Condtype['SAMPLE'], experiment=exp)
        #samples['samples'][gss] = c_sample.pk
        c_ch = m.Condition.objects.create(name=ch, cond_type=m.Condition.Condtype['CHANNEL'],
                experiment=exp)
        samples[gss] = c_ch.pk
        # Now add hierarchy:
        c_ch.parent_conds.add(samplesets[clean_set]['set_id'])
        c_ch.parent_conds.add(c_sample)
        # TODO how to treat non-grouped sample? currently this is X__POOL
        if sgroup != '':
            c_sample.parent_conds.add(c_group)
    return JsonResponse({'samplesets': samplesets, 'samples': samples})


# peptide centric:
@login_required
def frontpage(request):
    rawq = request.GET.get('q', False)
    featfields = ['peptides', 'proteins', 'genes', 'experiments']
    textfields = [f'{x}_text' for x in featfields] 
    exactfields = [f'{x}_text_exact' for x in featfields]
    idfields = [f'{x}_id' for x in featfields]
    if rawq:
        # fields/text/id must have right order as in client?
        # this because client doesnt send keys to have shorter b64 qstring
        getq = json.loads(b64decode(rawq))
        q = {'expand': {}}
        for field_ix, f in enumerate(idfields + textfields + exactfields):
            q[f] = getq[field_ix] 
        for feat, exp_ix in zip(featfields[1:], range(field_ix + 1, field_ix + 4)):
            q['expand'][feat] = getq[exp_ix]
        q['pep_excludes'] = getq[exp_ix + 1]
        q['datatypes'] = getq[exp_ix + 2]
    else:
        q = {f: [] for f in idfields}
        q.update({**{f: '' for f in textfields}, **{f: 1 for f in exactfields}})
        q['experiments_text_exact'] = 0
        q['pep_excludes'] = ''
        q['datatypes'] = {'dia': True, 'dda': True}
        q['expand'] = {'proteins': 0, 'genes': 0, 'experiments': 0}
    # first query filtering:
    qset = m.PeptideSeq.objects
    qset = qset.annotate(pepupp=Upper('seq'))
    if q['peptides_id']:
        qset = qset.filter(pk__in=[x[0] for x in q['peptides_id']])
    if q['peptides_text']:
        if q['peptides_text_exact']:
            qset = qset.filter(pepupp__in=q['peptides_text'].upper().split('\n'))
        else:
            peptq = Q()
            for pept in q['peptides_text'].upper().split('\n'):
                peptq |= Q(pepupp__contains=pept)
            qset = qset.filter(peptq)

    # Exclude sequences if any
    do_exclude, pepexq = False, Q()
    internal_aa = []
    pep_excludes = q['pep_excludes'].split('\n') if q['pep_excludes'] else []
    for pepex in pep_excludes:
        do_exclude = True
        if len(pepex) == 4 and pepex[:3] == 'int':
            internal_aa.append(pepex[-1].upper())
        else:
            pepexq |= Q(pepupp__contains=pepex)
    if internal_aa:
        pepexq |= Q(pepupp__regex=f'[A-Z]+[{"".join(internal_aa)}][A-Z]+')
    if do_exclude:
        qset = qset.exclude(pepexq)

    if q['experiments_id']:
        qset = qset.filter(peptideprotein__experiment__in=[x[0] for x in q['experiments_id']])
    if q['experiments_text']:
        qset = qset.annotate(eupp=Upper('peptideprotein__experiment__analysis__name'))
        exp_t_q = Q()
        for exp_t in q['experiments_text'].upper().split('\n'):
            exp_t_q |= Q(eupp__contains=exp_t)
        qset = qset.filter(exp_t_q)
        #(eupp__in=[x.upper() for x in q['experiments_text'].split('\n')])
    if q['proteins_id']:
        qset = qset.filter(peptideprotein__proteinfa__protein__in=[x[0] for x in q['proteins_id']])
    if q['proteins_text']:
        qset = qset.annotate(pupp=Upper('peptideprotein__proteinfa__protein__name'))
        if q['proteins_text_exact']:
            qset = qset.filter(pupp__in=[x.upper() for x in q['proteins_text'].split('\n')])
        else:
            ptxtq = Q()
            for ptxt in q['proteins_text'].upper().split('\n'):
                ptxtq |= Q(pupp__contains=ptxt)
            qset = qset.filter(ptxtq)

    if not all(q['datatypes'].values()):
        # FIXME datasets can migrate to have proper DB col for DIA/DDA, will get easier
        # lookups here
        dt_q = Q()
        dtypes_db = {x.value.upper(): x for x in dm.SelectParameterOption.objects.filter(param__title='Acquisition mode')}
        for dtype, keep_dt in q['datatypes'].items():
            if keep_dt:
                dt_q |= Q(peptideprotein__experiment__analysis__datasetsearch__dataset__selectparametervalue__value=dtypes_db[dtype.upper()])
        qset = qset.filter(dt_q)

    if q['genes_id']:
        qset = qset.filter(peptideprotein__proteinfa__proteingene__gene__in=[x[0] for x in q['genes_id']])
    if q['genes_text']:
        qset = qset.annotate(gupp=Upper('peptideprotein__proteinfa__proteingene__gene__name'))
        if q['genes_text_exact']:
            qset = qset.filter(gupp__in=[x.upper() for x in q['genes_text'].split('\n')])
        else:
            gtxtq = Q()
            for gtxt in q['genes_text'].upper().split('\n'):
                gtxtq |= Q(gupp__contains=gtxt)
            qset = qset.filter(gtxtq)
    
    fields = {'seq', 'id', 
            'peptideprotein__proteinfa__protein__name', 'peptideprotein__proteinfa__protein_id',
            'peptideprotein__proteinfa__proteingene__gene__name', 'peptideprotein__proteinfa__proteingene__gene_id', 'peptideprotein__experiment__analysis__name', 'peptideprotein__experiment_id'}
    agg_fields = {
            'proteins': ('peptideprotein__proteinfa__protein__name', 'peptideprotein__proteinfa__protein_id'),
            'genes': ('peptideprotein__proteinfa__proteingene__gene__name', 'peptideprotein__proteinfa__proteingene__gene_id'),
            'experiments': ('peptideprotein__experiment__analysis__name', 'peptideprotein__experiment_id'),
            }
    for aggr_col in featfields[1:]:
        if not q['expand'][aggr_col]:
            qset = qset.annotate(**{aggr_col: ArrayAgg(agg_fields[aggr_col][0])})
            idkey = f'{aggr_col}_id'
            qset = qset.annotate(**{idkey: ArrayAgg(agg_fields[aggr_col][1])})
            fields.update((aggr_col, idkey))
            fields.difference_update(agg_fields[aggr_col])
        
    qset = qset.values(*fields).order_by('pk')
    rows = []
    pnr = request.GET.get('page', 1)
    page, page_context = paginate(qset, pnr)
    for pep in page:
        agg_prots = pep.get('proteins', False)
        agg_genes = pep.get('genes', False)
        agg_exps = pep.get('experiments', False)
        prot = pep.get('peptideprotein__proteinfa__protein__name', False)
        pid = pep.get('peptideprotein__proteinfa__protein_id', False)
        gene = pep.get('peptideprotein__proteinfa__proteingene__gene__name', False)
        gid = pep.get('peptideprotein__proteinfa__proteingene__gene_id', False)
        exp = pep.get('peptideprotein__experiment__analysis__name', False)
        eid = pep.get('peptideprotein__experiment_id', False)
        row = {'id': pep['id'], 'seq': pep['seq']}
            # Have to set() the below in case there are duplicates:
            # not sure if those can be fished out WITHOUT keeping the
            # ID order and name order correlate, either in PG SQL or python
        if agg_prots:
            row['proteins'] = list(set(zip(pep['proteins_id'], agg_prots)))
        if agg_genes:
            row['genes'] = list(set(zip(pep['genes_id'], agg_genes)))
        if agg_exps:
            row['experiments'] = list(set(zip(pep['experiments_id'], agg_exps)))
        if not agg_prots:
            row['proteins'] = [(pid, prot)]
        if not agg_genes:
            row['genes'] = [(gid, gene)]
        if not agg_exps:
            row['experiments'] = [(eid, exp)]
        rows.append(row)
    context = {'tulos_data': rows, 'filters': q, **page_context,
            'total_exp': m.Experiment.objects.filter(upload_complete=True).count(), 'q': rawq or '',
            'total_pep': m.PeptideSeq.objects.count(),
            }
    return render(request, 'mstulos/front_pep.html', context=context)


#@login_required
def peptide_table(request):
    rawq = request.GET.get('q', False)
    if rawq:
        '''{ peptide_id: [exp_id, exp_id2, ...], ...}'''
        pepquery = json.loads(b64decode(rawq))
    else:
        pepquery = {}
    peptides = []
    filterq = Q()
    setorsample = 'SAMPLESET'
    for pepid, exps in pepquery.items():
        filterq |= Q(peptide__sequence_id=pepid, setorsample__experiment__in=exps)
    peptides = m.IdentifiedPeptide.objects.filter(filterq).values('peptide__encoded_pep', 'peptidefdr__fdr', 'amountpsmspeptide__value', 'setorsample__name', 'setorsample__experiment__analysis__name').order_by('peptide_id', 'setorsample__experiment_id', 'setorsample_id')
    pnr = request.GET.get('page', 1)
    page, page_context = paginate(peptides, pnr)
    context = {'peptides': page, **page_context}
    return render(request, 'mstulos/peptides.html', context=context)
    


#@login_required
def psm_table(request):
    '''Given a combination of peptide-sequence-ids and experiments they are in,
    produce a PSM table'''
    # TODO is it faster to loop over the peptides (all given peps x all given experiments) 
    # in python, or should we keep the SQL statement?
    rawq = request.GET.get('q', False)
    if rawq:
        '''{ peptide_id: [exp_id, exp_id2, ...], ...}'''
        pepquery = json.loads(b64decode(rawq))
    else:
        pepquery = {}
    all_exp_ids = {y for x in pepquery.values() for y in x}
    exp_files = {eid: m.Condition.objects.filter(cond_type=m.Condition.Condtype['FILE'],
        experiment=eid) for eid in all_exp_ids}

    filterq = Q()
    for pepid, exps in pepquery.items():
        pepexps = [exp_files[eid] for eid in exps]
        filterq |= Q(peptide__sequence_id=pepid, filecond__experiment__in=exps)
    sample_cond = 'filecond__parent_conds__name'
    sample_cond_id = 'filecond__parent_conds__id'
    qset = m.PSM.objects.filter(filterq).annotate(sample_or_set=F(sample_cond)).values('peptide__encoded_pep', 'filecond__name', 'scan', 'fdr', 'score', 'filecond__experiment__analysis__name', 'sample_or_set').order_by('peptide_id', 'filecond__experiment_id', sample_cond_id, 'filecond_id')
    pnr = request.GET.get('page', 1)
    page, page_context = paginate(qset, pnr)
    context = {'psms': page, **page_context}
    return render(request, 'mstulos/psms.html', context=context)


@require_POST
def upload_proteins(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        exp = m.Experiment.objects.get(token=data['token'], upload_complete=False)
    except m.Experiment.DoesNotExist:
        return JsonResponse({'error': 'Not allowed to access'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Bad request to mstulos uploads'}, status=400)
    stored_prots, stored_genes = {}, {}
    organism_genes = m.Gene.objects.filter(organism_id=data['organism_id'])
    # organism_proteins = m.Protein.objects.filter(peptideprotein__proteingene__gene__in=organism_genes)
    existing_genes = {x.name: x.pk for x in organism_genes}
    # Usually fasta duplicates dont work in analyses, but be defensive and include
    # the fa fn ID here at least - still assumes no duplicates in the single files
    # To defend against that wed have to include the sequence
    existing_prots = {f'{x.fafn_id}__{x.protein.name}': x.pk for x in 
            m.ProteinFasta.objects.filter(fafn_id__in=data['fa_ids'])}
    for fa_id, prot, gene, seq in data['protgenes']:
        if gene in existing_genes:
            store_gid = existing_genes[gene]
        elif gene:
            store_gid = m.Gene.objects.get_or_create(name=gene, organism_id=data['organism_id'])[0].pk
        fa_prot = f'{fa_id}__{prot}'
        if fa_prot not in existing_prots:
            dbprot, _ = m.Protein.objects.get_or_create(name=prot)
            protfa = m.ProteinFasta.objects.create(protein=store_prot, fafn_id=fa_id, sequence=seq)
            existing_prots[fa_prot] = protfa.pk
            if gene:
                m.ProteinGene.objects.get_or_create(proteinfa=protfa, gene_id=store_gid)
        stored_prots[prot] = existing_prots[fa_prot]
    return JsonResponse({'error': False, 'protein_ids': stored_prots})


def get_mods_from_seq(seq, mods=False, pos=0):
    '''Recursive, finds mods at positions in peptide'''
    if not mods:
        mods = {}
    if m := re.search('[+-][0-9]+\.[0-9]+', seq):
        pos = pos + m.start()
        mods[pos] = m.group()
        nextseq = seq[m.end():]
        mods, pos = get_mods_from_seq(nextseq, mods, pos)
    return mods, pos


def encode_mods(resultmolecule, barepep, mod_ids_table):
    # FIXME what if there is a negative mod (ie fixed plus another)?
    '''Turn any PSM sequence (from sage or MSGF) into a barepep with a map
    of where the mods and their IDs in our DB are'''
    resultmol_strip = re.sub('[\[\]]', '', resultmolecule)
    modpos, _ = get_mods_from_seq(resultmol_strip)
    encodings = [f'{pos}:{mod_ids_table[mod]}' for pos, mod in modpos.items()]
    return f'{barepep}[{",".join(encodings)}]'


@require_POST
def upload_peptides(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        exp = m.Experiment.objects.get(token=data['token'], upload_complete=False)
    except m.Experiment.DoesNotExist:
        return JsonResponse({'error': 'Not allowed to access'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Bad request to mstulos uploads'}, status=400)
    stored_peps = {}
    # Get mods in dict with both sage format (rounded to 4 dec) and MSGF (rounded to 3 dec)
    # e.g. {304.1234: 3, 304.123: 3, 79.1234: 1, 79.123: 1}
    # TODO maybe centralize table so its same in other views etc?
    mod_ids = {}
    [mod_ids.update({f'+{round(x.mass, 3)}': x.id, f'+{round(x.mass, 4)}': x.id})
        for x in m.Modification.objects.all()]
    for pep in data['peptides']:
        bareseq = re.sub('[^A-Z]', '', pep['pep'])
        pepseq, _cr = m.PeptideSeq.objects.get_or_create(seq=bareseq)
        encoded_pepmol = encode_mods(pep['pep'], bareseq, mod_ids)
        if _cr:
            mol = m.PeptideMolecule.objects.create(sequence=pepseq, encoded_pep=encoded_pepmol)
        else:
            mol, _cr = m.PeptideMolecule.objects.get_or_create(sequence=pepseq,
                    encoded_pep=encoded_pepmol)

        for prot_id in pep['prots']:
            m.PeptideProtein.objects.get_or_create(peptide=pepseq, proteinfa_id=prot_id,
                    experiment=exp)
        stored_peps[pep['pep']] = mol.pk
        idpeps = {}
        for cond_id, fdr in pep['qval']:
            idpep = m.IdentifiedPeptide.objects.create(peptide=mol, setorsample_id=cond_id)
            idpeps[cond_id] = idpep
            m.PeptideFDR.objects.create(fdr=fdr, idpep=idpep)
        for cond_id, nrpsms in pep['psmcount']:
            m.AmountPSMsPeptide.objects.create(value=nrpsms, idpep=idpeps[cond_id])
        for cond_id, ms1area in pep['ms1']:
            if ms1area != 'NA':
                m.PeptideMS1.objects.create(ms1=ms1area, idpep=idpeps[cond_id])
        for cond_id, quant in pep['isobaric']:
            if quant != 'NA':
                m.PeptideIsoQuant.objects.create(peptide=mol, value=quant, channel_id=cond_id)
    return JsonResponse({'error': False, 'pep_ids': stored_peps})


@require_POST
def upload_psms(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        exp = m.Experiment.objects.get(token=data['token'], upload_complete=False)
    except m.Experiment.DoesNotExist:
        return JsonResponse({'error': 'Not allowed to access'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Bad request to mstulos uploads'}, status=400)
    for psm in data['psms']:
        m.PSM.objects.create(peptide_id=psm['pep_id'], fdr=psm['qval'], scan=psm['scan'],
                filecond_id=psm['fncond'], score=psm['score'])
    return JsonResponse({'error': False})


@require_POST
def upload_done(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        exp = m.Experiment.objects.get(token=data['token'], upload_complete=False)
    except m.Experiment.DoesNotExist:
        return JsonResponse({'error': 'Not allowed to access'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Bad request to mstulos uploads'}, status=400)

    exp.upload_complete = True
    exp.save()
    jv.set_task_done(data['task_id'])
    return JsonResponse({'error': False})