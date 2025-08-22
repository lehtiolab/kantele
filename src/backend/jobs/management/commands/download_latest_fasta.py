import requests
from django.core.management.base import BaseCommand

from kantele import settings
from analysis.models import UniProtFasta, EnsemblFasta

from jobs import models as jm
from jobs import jobs as jj
from jobs.jobutil import create_job


class Command(BaseCommand):
    help = 'Queue a job to download fasta files automatically from ENSEMBL and Swissprot'

    def add_arguments(self, parser):
        '''Arguments are for testing script to only download single db'''
        parser.add_argument('--dbtype')
        parser.add_argument('--species')
        parser.add_argument('--ensembl_ver')
        parser.add_argument('--uptype', nargs='+', default=['SWISS_ISOFORMS', 'REFERENCE_ISOFORMS'])

    def handle(self, *args, **options):
        # First check the releases available:
        if ens_version := options['ensembl_ver']:
            pass
        else:
            r = requests.get(settings.ENSEMBL_API, headers={'Content-type': 'application/json'})
            ens_version = r.json()['release'] if r.ok else ''
        r = requests.get(settings.UNIPROT_API.format(settings.UP_ORGS['Homo sapiens'], ''),
                stream=True)
        up_version = r.headers['X-UniProt-Release'] if r.ok else ''
        dbmods = {'ensembl': EnsemblFasta, 'uniprot': UniProtFasta}
        to_download = {'ensembl': {'Homo sapiens', 'Mus musculus'}, 'uniprot': {
            'SWISS': {'Homo sapiens', 'Mus musculus'},
            'SWISS_ISOFORMS': {'Homo sapiens', 'Mus musculus'},
            'REFERENCE': {'Homo sapiens', 'Mus musculus'},
            'REFERENCE_ISOFORMS': {'Homo sapiens', 'Mus musculus'}}}
        if options['dbtype']:
            to_download = {k: v if k == options['dbtype'] else {} for k, v in to_download.items()}
        if options['species']:
            if to_download.get('ensembl'):
                to_download['ensembl'] = {options['species']}
            if dbs := to_download.get('uniprot'):
                to_download['uniprot'] = {k: {options['species']} for k in dbs.keys()}
        if 'ensembl' in to_download:
            for local_ens in EnsemblFasta.objects.filter(version=ens_version):
                to_download['ensembl'].remove(local_ens.organism)
        if 'uniprot' in to_download:
            to_download['uniprot'] = {k: v for k,v in to_download['uniprot'].items()
                    if k in options['uptype']}
            for local_up in UniProtFasta.objects.filter(version=up_version):
                uptype = UniProtFasta.UniprotClass(local_up.dbtype).name
                if uptype in to_download['uniprot']:
                    to_download['uniprot'][uptype].remove(local_up.organism)

        # Queue jobs for downloading if needed
        dljobs = jm.Job.objects.filter(funcname='download_fasta_repos').exclude(state__in=[
            jj.Jobstates.REVOKING, jj.Jobstates.CANCELED])
        for org in to_download['ensembl']:
            ens_jobs = dljobs.filter(kwargs__db='ensembl', kwargs__version=ens_version,
                    kwargs__organism=org)
            if not ens_jobs.count():
                create_job('download_fasta_repos', db='ensembl', version=ens_version,
                        organism=org)
        for dbtype, orgs in to_download['uniprot'].items():
            for org in orgs:
                up_jobs = dljobs.filter(kwargs__db='uniprot', kwargs__version=up_version,
                        kwargs__dbtype=dbtype, kwargs__organism=org)
                if not up_jobs.count():
                    create_job('download_fasta_repos', db='uniprot', version=up_version,
                            dbtype=dbtype, organism=org)
