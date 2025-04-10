import os
import re
from urllib.parse import urljoin
from collections import defaultdict

from django.urls import reverse
from celery import shared_task
from Bio import SeqIO

from kantele import settings
from jobs.post import update_db

# TODO
# matching to proteins, how will we do that? use sqlite, do not parse fasta, since fasta have
# releases every so often!
# in this case we need the protein to be multiple, per experiment
# upload proteins first, get IDs and track, 

# TODO multi-organism (i.e. metaproteomics) is a bit different, the sample contains a blend
# of organisms and you run multi-db, then we have to keep track of which proteins is which
# organism -> some kind of parsing required. Our ddamsproteomics pipeline doesnt handle
# that smoothly, youd like a fasta/organism coupling then (entered by user), and then
# read fasta to keep track/lookup protein names to organism when running this


@shared_task(bind=True)
def summarize_result_peptable(self, token, organism_id, peptide_file, psm_file, gene_file,
        outheaders, fafns):
    # FIXME maybe not do proteins when running 6FT? Need to make it selectable!
    # FIXME exempt proteogenomics completely for now or make button (will be misused!)

    # Determine wf_output first, on PSM table
    wf_out_count = defaultdict(int)
    for wf_out_pk, psmfn in psm_file.items():
        psmfile_fpath = os.path.join(settings.SHAREMAP[psmfn[0]], psmfn[1])
        with open(psmfile_fpath) as fp:
            header = next(fp).strip('\n').split('\t')
        for coln in ['fn', 'scan', 'charge', 'setname', 'peptide', 'fdr', 'score', 'ms1', 'mz']:
            try:
                header.index(outheaders[wf_out_pk]['psm'][coln])
            except IndexError:
                pass
            else:
                wf_out_count[wf_out_pk] += 1
    best_match_wf = max(wf_out_count, key=wf_out_count.get)
    peptide_file = peptide_file[best_match_wf]
    psm_file = psm_file[best_match_wf]
    if best_match_wf in gene_file:
        gene_file = gene_file[best_match_wf]
    else:
        gene_file = False
    outheaders = outheaders[best_match_wf]
    fafns = fafns[best_match_wf]

    # Update the experiment with best_match_wf, delete existing values and get sampletable 
    init_url = urljoin(settings.KANTELEHOST, reverse('mstulos:init_store'))
    resp = update_db(init_url, json={'token': token, 'wfout_found': best_match_wf})
    resp.raise_for_status()
    samplesets, samples = resp.json()['samplesets'], resp.json()['samples']

    # Store proteins, genes found, first fasta
    protgenes = []
    storedproteins, storedgenes = {}, {}
    protein_url = urljoin(settings.KANTELEHOST, reverse('mstulos:upload_proteins'))
    all_seq = {}
    for fa_id, fa_server, fafn in fafns:
        fa_path = os.path.join(settings.SHAREMAP[fa_server], fafn)
        all_seq[fa_id] = SeqIO.index(fa_path, 'fasta')
    # Now parse proteins/peptides from PSM table (as in peptide table proteins
    # are possibly grouped
    psmfile_fpath = os.path.join(settings.SHAREMAP[psm_file[0]], psm_file[1])
    psm_header = outheaders['psm']
    pepprots_nopk = defaultdict(set)
    pepprots = defaultdict(list)
    with open(psmfile_fpath) as fp:
        header = next(fp).strip('\n').split('\t')
        protfield = header.index(psm_header['protein'])
        pepfield = header.index(psm_header['peptide'])
        for line in fp:
            line = line.strip('\n').split('\t')
            bareseq = re.sub('[^A-Z]', '', line[pepfield])
            # This assumes proteins are in a field separated by semicolons, which is
            # the case for MSGF, Sage, msstitch PSM tables (and msstitch peptide table)
            for rawprotein in line[protfield].split(';'):
                # Assume protein names are A-Z0-9, i.e. strip other characters after that
                protein = re.sub('[^A-Za-z_\-0-9\.].*', '', rawprotein)
                if protein not in storedproteins:
                    # Protein found not yet knowing the DB ID of, so pass it to backend
                    gene = False
                    for fa_id, acc_seqs in all_seq.items():
                        if protein in acc_seqs:
                            fahead = acc_seqs[protein].description.split()
                            protseq = str(acc_seqs[protein].seq)
                            ens = [x for x  in [x.split(':') for x in fahead] if x[0] == 'gene_symbol']
                            sp = [x for x  in [x.split('=') for x in fahead] if x[0] == 'GN']
                            if rec_gene := ens or sp:
                                gene = rec_gene[0][1]
                            break
                    protgenes.append([fa_id, protein, gene, protseq])
                    storedproteins[protein] = False
                    pepprots_nopk[protein].add(bareseq)
                elif bareseq not in pepprots:
                    # Protein has DB ID, but peptide seq is not yet encountered, so
                    # map peptide to stored protein for later use
                    for fa_id, acc_seqs in all_seq.items():
                        if protein in acc_seqs:
                            protpos = str(acc_seqs[protein].seq).index(bareseq)
                            if storedproteins[protein] is False:
                                pepprots_nopk[protein].add(bareseq)
                            else:
                                pepprots[bareseq].append([storedproteins[protein], protpos])
                        break
                if len(protgenes) == 1000:
                    resp = update_db(protein_url, json={'protgenes': protgenes, 'token': token,
                        'organism_id': organism_id, 'fa_ids': [x[0] for x in fafns]})
                    resp.raise_for_status()
                    rj = resp.json()
                    storedproteins.update(rj['protein_ids'])
                    storedgenes.update(rj['gene_ids'])
                    protgenes = []
                    for newprot, bareseqs_tosave in pepprots_nopk.items():
                        storeprot = storedproteins[newprot]
                        for fa_id, acc_seqs in all_seq.items():
                            if newprot in acc_seqs:
                                for bseq_tosave in bareseqs_tosave:
                                    protpos = str(acc_seqs[newprot].seq).index(bseq_tosave)
                                    pepprots[bseq_tosave].append([storeprot, protpos])
                            break
                    pepprots_nopk = defaultdict(set)
    if len(protgenes):
        resp = update_db(protein_url, json={'protgenes': protgenes, 'token': token,
            'organism_id': organism_id, 'fa_ids': [x[0] for x in fafns]})
        resp.raise_for_status()
        rj = resp.json()
        storedproteins.update(rj['protein_ids'])
        storedgenes.update(rj['gene_ids'])
        for newprot, bareseqs_tosave in pepprots_nopk.items():
            storeprot = storedproteins[newprot]
            for fa_id, acc_seqs in all_seq.items():
                if newprot in acc_seqs:
                    for bseq_tosave in bareseqs_tosave:
                        protpos = str(acc_seqs[newprot].seq).index(bseq_tosave)
                        pepprots[bseq_tosave].append([storeprot, protpos])
                break

    # Now store all peptide sequences found with proteins/genes association, and
    # any conditions used
    pepheader = outheaders['pep']
    pepurl = urljoin(settings.KANTELEHOST, reverse('mstulos:upload_peptides'))
    storedpeps = {} # for ID tracking
    pepfile_fpath = os.path.join(settings.SHAREMAP[peptide_file[0]], peptide_file[1])
    with open(pepfile_fpath) as fp:
        header = next(fp).strip('\n').split('\t')
        conditions = {'ms1': [], 'qval': [], 'posterior': [], 'isobaric': []}
        # find header fields and match with conditions by setname/sample/channel
        pepfield = header.index(pepheader['peptide'])
        for ix, field in enumerate(header):
            # bit strict handling with f'_PSM count' to avoid Quanted PSM count fields..
            # Text parsing is not always super clean. We may need to think about that in
            # the pipeline itself, to make sure it is parseable, or encode somehow
            if pepheader['fdr'] and pepheader['fdr'] in field:
                setname = field.replace(f'_{pepheader["fdr"]}', '')
                conditions['qval'].append((ix, samplesets[setname]['set_id']))
            elif pepheader['posterior'] and pepheader['posterior'] in field:
                setname = field.replace(f'_{pepheader["posterior"]}', '')
                conditions['posterior'].append((ix, samplesets[setname]['set_id']))
            elif pepheader['ms1'] and pepheader['ms1'] in field:
                setname = field.replace(f'_{pepheader["ms1"]}', '')
                conditions['ms1'].append((ix, samplesets[setname]['set_id']))
            elif outheaders['isobaric'] and any(plex in field for plex in outheaders['isobaric']):
                plex_re = '(.*)_[a-z0-9]+plex_([0-9NC]+)'
                # Need to remove e.g. plex_126 - Quanted PSMs, with $
                if re.match(f'{plex_re}$', field):
                    sample_set_ch = re.sub(plex_re, '\\1___\\2', field)
                    conditions['isobaric'].append((ix, samples[sample_set_ch]))

        # conditions is now e.g. {'qval': [(header_ix, set_id), ...],
        #                       'isobaric': [(header_ix, sample_set_ch
        # parse peptide table and upload
        pep_values = []
        for line in fp:
            line = line.strip('\n').split('\t')
            bareseq = re.sub('[^A-Z]', '', line[pepfield])
            storepep = {'pep': line[pepfield], 'prots': pepprots[bareseq]}
            for datatype, col_conds in conditions.items():
                storepep[datatype] = []
                for col, cond_id in col_conds:
                    storepep[datatype].append((cond_id, line[col]))
            pep_values.append(storepep)
            if len(pep_values) == 1000:
                resp = update_db(pepurl, json={'peptides': pep_values, 'token': token})
                resp.raise_for_status()
                pep_values = []
                storedpeps.update(resp.json()['pep_ids'])
        if len(pep_values):
            resp = update_db(pepurl, json={'peptides': pep_values, 'token': token})
            resp.raise_for_status()
            storedpeps.update(resp.json()['pep_ids'])

            # TODO encoded_pep is likely for searching stuff, not sure how this was
            # thought out! Or if the MSGF+ format is the one we're using, but here we go
            # get frontend to search peptides to decide on how to store
            # mods need parsing etc, but nice with cut/paste from PSM table
            # NO! encoded pep should be possible to store without MSGF also
            # just easy to calculate!!!
            # so we need MSGF->encoded pep and other SE -> encoded pep
            # on storing and on looking up!! A hash would be good of peptide
            # with mods 

    # Go through PSM table with peptide_ids as map
    psms = []
    psmurl = urljoin(settings.KANTELEHOST, reverse('mstulos:upload_psms'))
    with open(psmfile_fpath) as fp:
        header = next(fp).strip('\n').split('\t')
        # FIXME catch these index() calls!
        fncol = header.index(psm_header['fn'])
        scancol = header.index(psm_header['scan'])
        chargecol = header.index(psm_header['charge'])
        setcol = header.index(psm_header['setname'])
        pepcol = header.index(psm_header['peptide'])
        fdrcol = header.index(psm_header['fdr'])
        posteriorcol = header.index(psm_header['posterior'])
        scorecol = header.index(psm_header['score'])
        mzcol = header.index(psm_header['mz'])
        ms1col = header.index(psm_header['ms1'])
        rtcol = header.index(psm_header['rt'])
        for line in fp:
            line = line.strip('\n').split('\t')
            storepsm = {}
            fn_cond_id = samplesets[line[setcol]]['files'][line[fncol]]
            # FIXME catch no peps, no fdr, no scan -- what??
            storepsm = {'scan': line[scancol], 'qval': line[fdrcol], 'PEP': line[posteriorcol],
                    'fncond': fn_cond_id, 'score': line[scorecol], 'charge': line[chargecol],
                    'ms1': line[ms1col], 'rt': line[rtcol], 'pep_id': storedpeps[line[pepcol]],
                    'mz': line[mzcol]}
            psms.append(storepsm)
            if len(psms) > 10000:
                resp = update_db(psmurl, json={'psms': psms, 'token': token})
                resp.raise_for_status()
                psms = []
        if len(psms):
            resp = update_db(psmurl, json={'psms': psms, 'token': token})
            resp.raise_for_status()

    # Gene table parse if any
    if gene_file:
        geneurl = urljoin(settings.KANTELEHOST, reverse('mstulos:upload_geneq'))
        genefile_fpath = os.path.join(settings.SHAREMAP[gene_file[0]], gene_file[1])
        with open(genefile_fpath) as fp:
            header = next(fp).strip('\n').split('\t')
            conditions = {'isobaric': []}
            # find header fields and match with conditions by setname/sample/channel
            genefield = header.index(outheaders['gene']['genename'])
            for ix, field in enumerate(header):
                # bit strict handling with f'_PSM count' to avoid Quanted PSM count fields..
                # Text parsing is not always super clean. We may need to think about that in
                # the pipeline itself, to make sure it is parseable, or encode somehow
                if outheaders['isobaric'] and any(plex in field for plex in outheaders['isobaric']):
                    plex_re = '(.*)_[a-z0-9]+plex_([0-9NC]+)'
                    # Need to remove e.g. plex_126 - Quanted PSMs, with $
                    if re.match(f'{plex_re}$', field):
                        sample_set_ch = re.sub(plex_re, '\\1___\\2', field)
                        conditions['isobaric'].append((ix, samples[sample_set_ch]))
            gene_values = []
            for line in fp:
                line = line.strip('\n').split('\t')
                storegene = {'gene': storedgenes[line[genefield]]}
                for datatype, col_conds in conditions.items():
                    storegene[datatype] = []
                    for col, cond_id in col_conds:
                        storegene[datatype].append((cond_id, line[col]))
                gene_values.append(storegene)
                if len(gene_values) == 1000:
                    resp = update_db(geneurl, json={'genes': gene_values, 'token': token})
                    resp.raise_for_status()
                    gene_values = []
            if len(gene_values):
                resp = update_db(geneurl, json={'genes': gene_values, 'token': token})
                resp.raise_for_status()

    # Finished, report done
    update_db(urljoin(settings.KANTELEHOST, reverse('mstulos:upload_done')), json={'token': token, 'task_id': self.request.id})
