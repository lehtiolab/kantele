from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db.models import Max, F

from kantele import settings

from rawstatus import models as rm
from datasets import models as dm
from analysis import models as am
from jobs.jobutil import create_job
from home import models as hm


class Command(BaseCommand):
    '''Go through files and purge those from disks which are expired'''

    def add_arguments(self, parser):
        # --dry-run only shows how many files will be deleted
        parser.add_argument('--dry-run', action='store_const', const=True, default=False)
        parser.add_argument('--analysis', action='store_const', const=True, default=False)
        parser.add_argument('--inbox', action='store_const', const=True, default=False)
        parser.add_argument('--datasets', action='store_const', const=True, default=False)
        parser.add_argument('--reports', action='store_const', const=True, default=False)
        parser.add_argument('--library', action='store_const', const=True, default=False)
        parser.add_argument('--mzml', action='store_const', const=True, default=False)

    def handle(self, *args, **options):
        def chunk_iter(qset, chunk_size):
            '''Django iterator has chunk_size but that only affects the database caching level
            and not the chunked output. Here we use this to output chunks to create jobs from
            otherwise the job params become very large'''
            chunk = []
            # Use iterator to avoid Django caching, uses less memory
            # for when result set is large
            for item in qset.iterator():
                chunk.append(item)
                if len(chunk) == chunk_size:
                    yield chunk
                    chunk = []
            yield chunk

        run_all = not any([options['analysis'], options['inbox'], options['datasets'],
                options['reports'], options['library'], options['mzml']])

        activefns_raw = rm.StoredFileLoc.objects.filter(active=True)
        activefns_raw_bup = activefns_raw.filter(
                sfile__rawfile__storedfile__pdcbackedupfile__success=True,
                sfile__rawfile__storedfile__pdcbackedupfile__deleted=False)

        if run_all or options['datasets']:
            # Files in dset (dont delete half a dataset)
            # Loop over shares (not many) to be able to connect dss and sfloc via their share
            for share in rm.ServerShare.objects.filter(active=True, function=rm.ShareFunction.RAWDATA,
                    maxdays_data__gt=0):
                total_rm_ds_fns = 0

                dss_exps = dm.DatasetServer.objects.filter(storageshare=share, active=True,
                        last_date_used__lt=timezone.now() - timedelta(days=share.maxdays_data))
                for dss_exp in dss_exps.values('dataset_id', 'pk'):
                    dss_sfl = activefns_raw.filter(servershare=share,
                            sfile__rawfile__datasetrawfile__dataset_id=dss_exp['dataset_id'])
                    dssrmsfl = activefns_raw_bup.filter(servershare=share,
                            sfile__rawfile__datasetrawfile__dataset_id=dss_exp['dataset_id'])
                    if dss_sfl.count() == dssrmsfl.count():
                        if options['dry_run']:
                            nr_dsssfl = dssrmsfl.count()
                            dss_nr = dss_exps.filter(pk=dss_exp['pk']).count()
                            print(f'Dry run, would queue expired dss {dss_exp["pk"]} from dset '
                                    f'{dss_exp["dataset_id"]} with {nr_dsssfl} files on share '
                                    f'{share.name} for deletion')
                        else:
                            # Dss does not need to "have all dset files" (maybe previously it has been
                            # half-deleted), but files-to-remove must all have backed up file
                            dssrmsfl_ids = [x['pk'] for x in dssrmsfl.values('pk')]
                            create_job('remove_dset_files_servershare', dss_id=dss_exp['pk'],
                                    sfloc_ids=dssrmsfl_ids)
                            nr_dsssfl = dssrmsfl.update(active=False)
                            dss_nr = dss_exps.filter(pk=dss_exp['pk']).update(active=False)
                            print(f'Queued expired dss {dss_exp["pk"]} from dset '
                                    f'{dss_exp["dataset_id"]} with {nr_dsssfl} files on share '
                                    f'{share.name} for deletion')
                        total_rm_ds_fns += nr_dsssfl
                    else:
                        # Possibilities:
                        # - old files with no backup, or on old backup system (dont delete!) FIXME back them up!
                        # - files manually deleted somehow (active=False)
                        # - no files are added
                        #   - subcategory: files added in pending but are not accepted by user yet
                        print(f'DSS {dss_exp["pk"]} for dataset {dss_exp["dataset_id"]}, has no '
                                'deletable files, skipping auto-deletion')

                # Now weve set expired dsets, the rest is "soon to expire", message users:
                if not options["dry_run"]:
                    for ds_soon in dm.Dataset.objects.filter(datasetserver__storageshare=share,
                            datasetserver__active=True,
                            datasetserver__last_date_used__lt=timezone.now() -
                            timedelta(days=share.maxdays_data + settings.DATASET_EXPIRY_DAYS_MESSAGE)):
                        # Test this to see if the joins are correctly done
                        if ds_soon.datasetserver_set.filter(active=True).count() == 1:
                            # found dataset with only one datasetserver active, msg each owner
                            for ds_usr in ds_soon.filter(datasetowner__user_active=True).values('pk',
                                    'datasetowner__user', 'runname__experiment__project__name'):
                                hm.UserMessage.create_message(ds_usr['datasetowner__user'], 
                                    msgtype=hm.DsetMsgTypes.DELETE_SOON, dset_id=ds_usr['pk'])
                print(f'In total {total_rm_ds_fns} dataset files {"could" if options["dry_run"] else "will"}'
                        ' be deleted')

        if run_all or options['analysis']:
            # Result files from analysis (batch per analysis so we dont delete half)
            # note that we take source fn date as last date, so they can have been input for
            # something also. If any file from an analysis has been used, dont delete the rest
            # of the analysis files either!
            all_ana_fns = activefns_raw_bup.filter(sfile__analysisresultfile__isnull=False)
            all_rm_anas = set()
            all_shared_sfls = set()
            for share in rm.ServerShare.objects.filter(active=True, maxdays_data__gt=0,
                    function__in=[rm.ShareFunction.ANALYSIS_DELIVERY, rm.ShareFunction.ANALYSISRESULTS]):
                share_ana_fns = all_ana_fns.filter(servershare=share)
                # get analysis_id, max_date of last_used in analysis share files
                exp_ana = share_ana_fns.values('sfile__analysisresultfile__analysis_id').filter(
                        last_date_used__lt=timezone.now() - timedelta(days=share.maxdays_data))
                rm_ana_ids = {x['sfile__analysisresultfile__analysis_id'] for x in exp_ana}
                all_rm_anas.update(rm_ana_ids)
                # if file is shared between two analyses, of which only one has expired, 
                # make sure the file doesnt get deleted!
                pre_multiana_fns = share_ana_fns.filter(
                        sfile__analysisresultfile__analysis_id__in=rm_ana_ids)
                notrm_ana = am.Analysis.objects.filter(
                        analysisresultfile__sfile__storedfileloc__id__in=pre_multiana_fns.filter(
                            last_date_used__gt=timezone.now() - timedelta(days=share.maxdays_data))
                        ).exclude(pk__in=rm_ana_ids)
                if rm_ana_sfl := pre_multiana_fns.exclude(sfile__analysisresultfile__analysis__in=notrm_ana):
                    if options['dry_run']:
                        ana_nr = rm_ana_sfl.count()
                        print(f'Dry run, could queue {ana_nr} expired analysis result files from '
                                f'share {share.name} for deletion')
                    else:
                        for chunk in chunk_iter(rm_ana_sfl.values('pk'), 100):
                            rm_ana_pks = [x['pk'] for x in chunk]
                            create_job('purge_files', sfloc_ids=rm_ana_pks)
                        ana_nr = rm_ana_sfl.update(active=False)
                        # set deleted=True on analysis if all its files are inactive,
                        # when a shared active file is there, currently dont delete, 
                        # this may be done but we're not bothering now
                        anas_removed = am.Analysis.objects.filter(pk__in=all_rm_anas).exclude(
                                analysisresultfile__sfile__storedfileloc__active=True)
                        anas_removed.update(deleted=True)
                        for ana_msg in anas_removed.values('pk', 'user_id'):
                            hm.UserMessage.create_message(ana_msg['user_id'],
                                    msgtype=hm.AnalysisMsgTypes.DELETE_SOON, analysis_id=ana_msg['pk'])
                        print(f'Queued {ana_nr} expired analysis result files from share {share.name} for deletion')

        # Other files (tmp, library, web report)
        functions = []
        if run_all or options['inbox']:
            functions.append(rm.ShareFunction.INBOX)
        if run_all or options['reports']:
            functions.append(rm.ShareFunction.REPORTS)
        if run_all or options['library']:
            functions.append(rm.ShareFunction.LIBRARY)

        for share in rm.ServerShare.objects.filter(active=True, maxdays_data__gt=0,
                function__in=functions):
            other_fns_rm = activefns_raw_bup.filter(servershare=share,
                last_date_used__lt=timezone.now() - timedelta(days=share.maxdays_data),
                sfile__rawfile__datasetrawfile__isnull=True, sfile__analysisresultfile__isnull=True)
            if options['dry_run']:
                other_nr = other_fns_rm.count()
                print(f'Dry run, could queue {other_nr} expired files from share {share.name} for deletion')
            else:
                for chunk in chunk_iter(other_fns_rm.values('pk'), 100):
                    rm_other_pks = [x['pk'] for x in chunk]
                    create_job('purge_files', sfloc_ids=rm_other_pks)
                other_nr = other_fns_rm.update(active=False)
                print(f'Queued {other_nr} expired files from share {share.name} for deletion')

        if run_all or options['mzml']:
            # Mzml dont get backed up, are intermediate files
            maxtime_mzml = timezone.now() - timedelta(settings.MAX_MZML_STORAGE_TIME_POST_ANALYSIS)
            activefns_mzml = rm.StoredFileLoc.objects.filter(active=True,
                    sfile__mzmlfile__isnull=False, last_date_used__lt=maxtime_mzml)
            if options['dry_run']:
                nr_mzml = activefns_mzml.count()
                print(f'Dry run, could queue {nr_mzml} expired mzML files for deletion')
            else:
                for chunk in chunk_iter(activefns_mzml.values('pk'), 100):
                    rm_mzml_pks = [x['pk'] for x in chunk]
                    create_job('purge_files', sfloc_ids=rm_mzml_pks)
                nr_mzml = activefns_mzml.update(active=False)
                print(f'Queued {nr_mzml} expired mzML files for deletion')
