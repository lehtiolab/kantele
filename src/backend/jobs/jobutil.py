from django.utils import timezone

from datasets import jobs as dsjobs
from rawstatus import jobs as rsjobs
from analysis import jobs as anjobs
from mstulos import jobs as mtjobs
from jobs.jobs import Jobstates

from rawstatus import models as rm

from kantele import settings


alljobs = [
        dsjobs.RenameDatasetStorageLoc,
        dsjobs.RsyncDatasetServershare,
        dsjobs.RemoveDatasetFilesFromServershare,
        dsjobs.ConvertDatasetMzml,
        #dsjobs.DeleteDatasetPDCBackup,
        rsjobs.RsyncFilesServershare,
        rsjobs.HttpUploadFilesServershare,
        rsjobs.DownloadFilesToServerShare,
        rsjobs.RsyncFileTransferFromWeb,
        rsjobs.RemoveFilesFromServershare,
        rsjobs.CreatePDCArchive,
        rsjobs.RestoreFromPDC,
        rsjobs.BackupPDCDataset,
        rsjobs.ReactivateDeletedDataset,
        rsjobs.RenameFile,
        rsjobs.DeleteEmptyDirectory,
        rsjobs.DownloadPXProject, # FIXME later
        rsjobs.RegisterExternalFile,  # FIXME later
        rsjobs.ClassifyMSRawFile,
        anjobs.RunLongitudinalQCWorkflow,
        anjobs.RunNextflowWorkflow,
        anjobs.RefineMzmls,
        anjobs.DownloadFastaFromRepos,
        mtjobs.ProcessAnalysis,
        ]
jobmap = {job.refname: job for job in alljobs}



def check_job_error(name, **kwargs):
    jwrap = jobmap[name](False)
    return jwrap.check_error(**kwargs)


def create_job(name, state=False, check_errors=True, **kwargs):
    '''Checks errors and then creates the job'''
    if not state:
        state = Jobstates.PENDING
    jwrap = jobmap[name](False)
    if check_errors:
        if error := check_job_error(name, **kwargs):
            return {'id': False, 'kwargs': kwargs, 'error': error}
    # Fall through if no errors or no checking    
    # addkwargs should be here, after error check, otehrwise
    # new SFL will be created at error check even when its failing
    kwargs.update(jwrap.on_create_addkwargs(**kwargs))
    jwrap.update_sourcefns_lastused(**kwargs)
    on_create_prep_rsync_jobs(jwrap, **kwargs)
    return jwrap.create_db_job(name, state, kwargs)


def create_job_without_check(name, state=False, **kwargs):
    '''In case you do error checking before creating jobs, you can use this
    for quicker creation without another check'''
    return create_job(name, state=state, check_errors=False, **kwargs)


def on_create_prep_rsync_jobs(jwrap, **kwargs):
    '''If needed, rsync the DB and other singlefiles which is not on the analysis share,
    this function provides a dict with the job name and kwargs for that'''
    newjobs, job_count = [], 0
    extra_sfs, dstshare_id, dstsharefun = jwrap._find_extrafiles_to_rsync(**kwargs)
    # Make rsync jobs for those extrafiles which there is no sfl in the servershare we want:
    # Loop over files not already in servershare
    for extra_sf in extra_sfs:
        extra_sf_sfl_q = extra_sf.storedfileloc_set.filter(active=True,
                servershare__active=True, servershare__fileservershare__server__active=True
                ).distinct('servershare')
        blankpathshares = [rm.ShareFunction.LIBRARY, rm.ShareFunction.INBOX]
        dstpaths, src_share_ids = {}, []
        for ex_sfl in extra_sf_sfl_q.values('pk', 'servershare_id', 'servershare__function', 'path'):
        
            src_share_ids.append(ex_sfl['servershare_id'])

            if ex_sfl['servershare__function'] in blankpathshares and dstsharefun not in blankpathshares:
                # e.g. libfile from path '' needs a path in the dst if that is rawdata!
                path = settings.LIBRARY_FILE_PATH_INBOX
            else:
                path = ex_sfl['path']
            # FIXME get path from share model?
            dstpaths[dstshare_id] = path
        distribute_file_servershares(src_share_ids, extra_sf_sfl_q, dstpaths)


def distribute_file_servershares(src_share_ids, existing_sfl, dstpaths):
    rsync_shares = set(dstpaths.keys())
    _jc, src_share_ids, rsync_shares, dstshare_classes, srcshare_id = queue_rsync_and_update_sharestate(
            src_share_ids, rsync_shares, dstpaths, existing_sfl)
    # Now only 2-step transfers over rsync possible, e.g. between 2 HPCs
    http_inboxjob, rs_inboxjob = False, False
    if rsync_shares and dstshare_classes['rsync_inbox_id']:
        # need to rsync to non-dset INBOX (as it is guaranteed to be on an rsyncing capable
        # controller (and sensitive classed), which is not the upload INBOX
        inboxshare = rm.ServerShare.objects.exclude(fileservershare__server__is_upload_server=True
                ).filter(function=rm.ShareFunction.INBOX).values('pk').first()
        sflpk = existing_sfl.filter(servershare_id=srcshare_id).values('pk').get()['pk']
        rs_inboxjob = create_job('rsync_files_to_servershare', sfloc_ids=[sflpk],
                dstshare_id=inboxshare['pk'], dstpath=rm.ServerShare.get_inbox_path())
        src_share_ids.append(inboxshare['pk'])
        _jc, src_share_ids, rsync_shares, dstshare_classes, srcshare_id = queue_rsync_and_update_sharestate(
                src_share_ids, rsync_shares, dstpaths, existing_sfl)
    if rsync_shares and dstshare_classes['http_inbox_id']:
        # FIXME protect security class
        if http_inboxshare_q := rm.ServerShare.objects.filter(
                fileservershare__id=dstshare_classes['http_inbox_id']):
            http_inboxshare = http_inboxshare_q.values('pk').first()
            src_sfloc_ids = [x['pk'] for x in existing_sfl.filter(
                    servershare_id=srcshare_id).values('pk')]
            http_inboxjob = create_job('http_upload_servershare',
                    dstshare_id=http_inboxshare['pk'],
                    inbox_fss=dstshare_classes['http_inbox_id'],
                    dstpath=rm.ServerShare.get_inbox_path(),
                    sfloc_ids=src_sfloc_ids)
        src_share_ids.append(http_inboxshare['pk'])
    if rsync_shares:
        last_queue = queue_rsync_and_update_sharestate(src_share_ids, rsync_shares, dstpaths,
                    existing_sfl)
        src_share_ids = last_queue[1]
    # delete jobs to remove stuff from inbox after were done with it
    if http_inboxjob:
        create_job('purge_files', sfloc_ids=http_inboxjob['kwargs']['dstsfloc_ids'])
        rm.StoredFileLoc.objects.filter(pk__in=http_inboxjob['kwargs']['dstsfloc_ids']
                    ).update(active=False)
    if rs_inboxjob:
        create_job('purge_files', sfloc_ids=rs_inboxjob['kwargs']['dstsfloc_ids'])
        rm.StoredFileLoc.objects.filter(pk__in=rs_inboxjob['kwargs']['dstsfloc_ids']
                    ).update(active=False)
    return src_share_ids


def queue_rsync_and_update_sharestate(src_share_ids, rsync_shares, dstshare_ids_paths, existing_sfl):
    '''Given source files and dst shares for files in a dataset, this will try to
    place the files in the dst shares, by rsync or download from the web server (proxy)
    as far as that is possible through DB-acquired knowledge of the shares accessibility.
    '''
    srcshare_id, dstshare_classes = rm.ServerShare.classify_shares_by_rsync_reach(
            src_share_ids, rsync_shares)
    # Try first all local and all rsync_sourcable
    dstshares_in_round = (dstshare_classes['local'] or dstshare_classes['rsync_sourcable']
                          or dstshare_classes['download'])
    transfer_job_count = 0
    while dstshares_in_round:
        src_sfloc_ids = [x['pk'] for x in existing_sfl.filter(
                servershare_id=srcshare_id).values('pk')]
        for dstshare_pk in dstshares_in_round:
            # sfloc_ids are here per defintion created by job, but maybe not after some
            # rsync jobs
            if dstshare_classes['local'] or dstshare_classes['rsync_sourcable']:
                srcjob = create_job('rsync_files_to_servershare', dstshare_id=dstshare_pk,
                                    dstpath=dstshare_ids_paths[dstshare_pk],
                                    sfloc_ids=src_sfloc_ids)
                transfer_job_count += 1
            else:
                # Download job is different, same input kwargs though
                srcjob = create_job('download_files_to_servershare', dstshare_id=dstshare_pk,
                                    dstpath=dstshare_ids_paths[dstshare_pk],
                                    sfloc_ids=src_sfloc_ids)
                transfer_job_count += 1
            # Add dstshare to src_shares as files are now there for next round of rsync
            src_share_ids.append(dstshare_pk)
            rsync_shares.remove(dstshare_pk)
        # Re-check if exist have local etc files for remaining shares after this round
        srcshare_id, dstshare_classes = rm.ServerShare.classify_shares_by_rsync_reach(
                src_share_ids, rsync_shares)
        dstshares_in_round = (dstshare_classes['local'] or dstshare_classes['rsync_sourcable']
                           or dstshare_classes['download'])
    return transfer_job_count, src_share_ids, rsync_shares, dstshare_classes, srcshare_id
