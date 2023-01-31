import json
from base64 import b64decode

from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.contrib.postgres.aggregates import ArrayAgg
from django.views.decorators.http import require_POST
from django.db.models import Q, Count
from django.db.models.functions import Upper
from django.core.paginator import Paginator

from mstulos import models as m
from jobs import views as jv


# FIXME:
# fix fixmes in job/task
# have shareable URLs for search including unrolls
# create plots for TMT
# PSM tables
# PSM plots?
# gene/protein centric tables

# peptide centric:
@login_required
def frontpage(request):
    rawq = request.GET.get('q', False)
    qfields = ['peptides', 'proteins', 'genes', 'experiments']
    textfields = [f'{x}_text' for x in qfields]
    idfields = [f'{x}_id' for x in qfields]
    if rawq:
        # fields/text/id must have right order as in client?
        # this because client doesnt send keys to have shorter b64 qstring
        getq = json.loads(b64decode(rawq))
        q = {f: getq[i] for i, f in enumerate(idfields + textfields)}
        q['expand'] = {k: v for k,v in zip(qfields[1:], getq[8:])}
    else:
        q = {f: [] for f in idfields}
        q.update({f: '' for f in textfields})
        q['expand'] = {'proteins': 0, 'genes': 0, 'experiments': 0}
    # first query filtering:
    qset = m.PeptideSeq.objects
    if q['peptides_id']:
        qset = qset.filter(pk__in=q['peptides_id'])
    if q['peptides_text']:
        qset = qset.filter(seq__in=q['peptides_text'].split('\n'))
    if q['experiments_id']:
        qset = qset.filter(peptideprotein__experiment__in=q['experiments_id'])
    if q['experiments_text']:
        exp_t_q = Q()
        for exp_t in q['experiments_text'].split('\n'):
            exp_t_q |= Q(eupp__contains=exp_t.upper())

        qset = qset.annotate(eupp=Upper('peptideprotein__experiment__analysis__name')).filter(exp_t_q)
        #(eupp__in=[x.upper() for x in q['experiments_text'].split('\n')])
    if q['proteins_id']:
        qset = qset.filter(peptideprotein__protein__in=q['proteins_id'])
    if q['proteins_text']:
        qset = qset.annotate(pupp=Upper('peptideprotein__protein__name')).filter(pupp__in=[x.upper() for x in q['proteins_text'].split('\n')])
    if q['genes_id']:
        qset = qset.filter(peptideprotein__proteingene__gene__in=q['genes_id'])
    if q['genes_text']:
        qset = qset.annotate(gupp=Upper('peptideprotein__proteingene__gene__name')).filter(gupp__in=[x.upper() for x in q['genes_text'].split('\n')])
    
    fields = {'seq', 'id', 
            'peptideprotein__protein__name', 'peptideprotein__protein_id',
            'peptideprotein__proteingene__gene__name', 'peptideprotein__proteingene__gene_id', 'peptideprotein__experiment__analysis__name', 'peptideprotein__experiment_id'}
    agg_fields = {
            'proteins': ('peptideprotein__protein__name', 'peptideprotein__protein_id'),
            'genes': ('peptideprotein__proteingene__gene__name', 'peptideprotein__proteingene__gene_id'),
            'experiments': ('peptideprotein__experiment__analysis__name', 'peptideprotein__experiment_id'),
            }
    for aggr_col in qfields[1:]:
        if not q['expand'][aggr_col]:
            qset = qset.annotate(**{aggr_col: ArrayAgg(agg_fields[aggr_col][0])})
            idkey = f'{aggr_col}_id'
            qset = qset.annotate(**{idkey: ArrayAgg(agg_fields[aggr_col][1])})
            fields.update((aggr_col, idkey))
            fields.difference_update(agg_fields[aggr_col])
        
    qset = qset.values(*fields).order_by('pk')
    pages = Paginator(qset, 100)
    pnr = q.get('page', 1)
    page = pages.get_page(pnr)
    rows = []
    for pep in page:
        agg_prots = pep.get('proteins', False)
        agg_genes = pep.get('genes', False)
        agg_exps = pep.get('experiments', False)
        prot = pep.get('peptideprotein__protein__name', False)
        pid = pep.get('peptideprotein__protein_id', False)
        gene = pep.get('peptideprotein__proteingene__gene__name', False)
        gid = pep.get('peptideprotein__proteingene__gene_id', False)
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
    return render(request, 'mstulos/front.html', {'tulos_data': rows, 
        'filters': q, 'page_nr': pnr})


#@login_required
def peptide_table(request):
    rawq = request.GET.get('q', False)
    #qfields = ['peptides', 'proteins', 'genes', 'experiments']
    #textfields = [f'{x}_text' for x in qfields]
    #idfields = [f'{x}_id' for x in qfields]
    if rawq:
        pass
        # fields/text/id must have right order as in client?
        # this because client doesnt send keys to have shorter b64 qstring
#        getq = json.loads(b64decode(rawq))
#        q = {f: getq[i] for i, f in enumerate(idfields + textfields)}
#        q['expand'] = {k: v for k,v in zip(qfields[1:], getq[8:])}
#    else:
#        q = {f: [] for f in idfields}
#        q.update({f: '' for f in textfields})
#        q['expand'] = {'proteins': 0, 'genes': 0, 'experiments': 0}
    



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
    #qfields = ['peptides', 'proteins', 'genes', 'experiments']
    #textfields = [f'{x}_text' for x in qfields]
    #idfields = [f'{x}_id' for x in qfields]

    #exp = m.Experiment.objects.filter(
    all_exp_ids = {y for x in pepquery.values() for y in x}
    exp_files = {eid: m.Condition.objects.filter(cond_type=m.Condition.Condtype['FILE'],
        experiment=eid) for eid in all_exp_ids}
    filterq = Q()
    for pepid, exps in pepquery.items():
        pepexps = [exp_files[eid] for eid in exps]
        filterq |= Q(peptide__sequence_id=pepid) & Q(condition__in=pepexps[0].union(*pepexps[1:]).values('pk'))
    qset = m.PSM.objects.filter(filterq)
    pages = Paginator(qset, 100)
    pnr = request.GET.get('page', 1)
    page = pages.get_page(pnr)
    return render(request, 'mstulos/psms.html', context={'psms': page})


@login_required
def find_query(request):
    pepseq = request.GET['q'].upper()
    query = Q(bareseq__seq__contains=pepseq)
    query |= Q(encoded_pep__contains=pepseq)
    pepmols = m.PeptideMolecule.objects.filter(query).filter(pepfdr__isnull=False)
    results = [{'id': x.pk, 'txt': x.encoded_pep,
        'type': 'peptide',
        'expnum': x.pepfdr_set.distinct('condition__experiment').count(),
        'condnum': x.pepfdr_set.count(),
        'experiments': [{'name': pf['condition__experiment__name'],
            'id': pf['condition__experiment__pk'],
            } for pf in x.pepfdr_set.distinct('condition__experiment').values('condition__experiment__name', 'condition__experiment__pk')],
        } for x in pepmols]
    return JsonResponse({'results': results})


@login_required
def get_results(request, restype, resid):
    if restype == 'peptide':
        m.PepFDR.objects.filter(peptide_id=resid)
    return JsonResponse({})


@login_required
def get_data(request):
    """
    input:
    {type: peptide, ids: [1,2,3,4], experiments: [1,2,3,4]}
    
    output:
    {pepfdr: 
          {exp_id: {name: exp_name, samples: 
    maybe [{exp: 1, sam: 3, 3: 0.002, 4: 0.001, 5: 0, ...}...]
    """
    data = json.loads(request.body.decode('utf-8'))
    pepquant, pepfdr = {}, {}
    if data['type'] == 'peptide':
        for pf in m.PepFDR.objects.filter(peptide_id__in=data['ids'],
                condition__experiment_id__in=data['experiments']).select_related(
                'peptide').order_by('condition__experiment_id'):
            #pepquant[pf.condition.experiment_id].append('sam': sam, 'featid': pf.peptide_id, 'value': pf.value})
            if pf.condition.experiment_id not in pepfdr:
                pepfdr[pf.condition.experiment_id] = {}
            sam = pf.condition.name
            try:
                pepfdr[pf.condition.experiment_id][sam][pf.peptide_id] = pf.value
            except KeyError:
                pepfdr[pf.condition.experiment_id][sam] = {pf.peptide_id: pf.value}
        for pq in m.PeptideQuantResult.objects.filter(peptide_id__in=data['ids'],
                condition__experiment_id__in=data['experiments']).select_related(
                'peptide'):
            sam = pq.condition.name
            try:
                pepquant[sam][pq.peptide_id] = (pq.peptide.encoded_pep, pq.value)
            except KeyError:
                pepquant[sam] = {pq.peptide_id: (pq.peptide.encoded_pep, pq.value)}
    return JsonResponse({'pepfdr': pepfdr, 'pepquant': {}})


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
    organism_proteins = m.Protein.objects.filter(peptideprotein__proteingene__gene__in=organism_genes)
    existing_genes = {x.name: x.pk for x in organism_genes}
    existing_prots = {x.name: x.pk for x in organism_proteins}
    for prot, gene in data['protgenes']:
        if gene in existing_genes:
            store_gid = existing_genes[gene]
        else:
            store_gid = m.Gene.objects.get_or_create(name=gene, organism_id=data['organism_id'])[0].pk
        if prot in existing_prots:
            store_pid = existing_prots[prot]
        else:
            # cannot get_or_create here, we only have name field
            store_pid = m.Protein.objects.create(name=prot).pk
        stored_prots[prot] = store_pid
        stored_genes[gene] = store_gid
    return JsonResponse({'error': False, 'protein_ids': stored_prots, 'gene_ids': stored_genes})


@require_POST
def upload_pepprots(request):
    data = json.loads(request.body.decode('utf-8'))
    try:
        exp = m.Experiment.objects.get(token=data['token'], upload_complete=False)
    except m.Experiment.DoesNotExist:
        return JsonResponse({'error': 'Not allowed to access'}, status=403)
    except KeyError:
        return JsonResponse({'error': 'Bad request to mstulos uploads'}, status=400)
    stored_peps = {}
    for pep, bareseq, prot_id, gene_id in data['pepprots']:
        pepseq, _cr = m.PeptideSeq.objects.get_or_create(seq=bareseq)
        if _cr:
            #m.PeptideProtein.objects.create(peptide=pepseq, protein_id=prot_id, experiment=exp)
            mol = m.PeptideMolecule.objects.create(sequence=pepseq, encoded_pep=pep)
        else:
            mol, _cr = m.PeptideMolecule.objects.get_or_create(sequence=pepseq, encoded_pep=pep)
        pepprot, _cr = m.PeptideProtein.objects.get_or_create(peptide=pepseq, protein_id=prot_id, experiment=exp)
        if gene_id:
            m.ProteinGene.objects.get_or_create(pepprot=pepprot, gene_id=gene_id)
            
        stored_peps[pep] = mol.pk
    return JsonResponse({'error': False, 'pep_ids': stored_peps})


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
    for pep in data['peptides']:
        # FIXME think of encodign for peptide to be created from e.g. MSGF or other SE
        # go over condistions!
        for cond_id, fdr in pep['qval']:
            m.PeptideFDR.objects.create(peptide_id=pep['pep_id'], fdr=fdr, condition_id=cond_id)
        for cond_id, nrpsms in pep['psmcount']:
            m.AmountPSMsPeptide.objects.create(peptide_id=pep['pep_id'], value=nrpsms, condition_id=cond_id)
        for cond_id, quant in pep['isobaric']:
            m.PeptideIsoQuant.objects.create(peptide_id=pep['pep_id'], value=quant, condition_id=cond_id)
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
                condition_id=psm['fncond'], score=psm['score'])
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