"""
This management task contains a VERY SIMPLE job scheduler.
More advanced job scheduling is to be done by celery chains etc,
Nextflow, or Galaxy or whatever one likes.

Scheduler runs sequential and waits for each job that contains files running in another job
"""
from celery import states
from django.core.management.base import BaseCommand
from time import sleep

from kantele import settings
from jobs.models import Task, Job
from jobs.jobs import Jobstates
from jobs.views import revoke_and_delete_tasks
from jobs import jobs as jj
from rawstatus.models import FileJob
from jobs.jobutil import jobmap


class Command(BaseCommand):
    help = 'Run job runner until you terminate it'

    def handle(self, *args, **options):
        '''Only run once when in testing mode, so it returns'''
        if settings.TESTING:
            run_ready_jobs({}, {}, set())
        else:
            job_fn_map, job_ds_map, active_jobs = {}, {}, set()
            while True:
                job_fn_map, job_ds_map, active_jobs = run_ready_jobs(job_fn_map, job_ds_map, active_jobs)
                sleep(settings.JOBRUNNER_INTERVAL)


def run_ready_jobs(job_fn_map, job_ds_map, active_jobs):
    print('Checking job queue')
    jobs_not_finished = Job.objects.order_by('timestamp').exclude(
        state__in=jj.JOBSTATES_DONE + [Jobstates.WAITING])
    print(f'{jobs_not_finished.count()} jobs in queue, including errored jobs')
    # Jobs that changed to waiting are excluded from active
    # Jobs that are on HOLD also, because they will get added to active when encountered
    # This way they will only seen as active to jobs after the held job
    wait_jobs = Job.objects.filter(state__in=[Jobstates.WAITING, Jobstates.HOLD],
            pk__in=active_jobs).values('pk')
    active_jobs.difference_update([x['pk'] for x in wait_jobs])
    for job in jobs_not_finished:
        # First check if job is new or already registered
        jwrapper = jobmap[job.funcname](job.id) 
        if not job.id in job_fn_map:
            print(f'Registering new job {job.id} - {job.funcname} - {job.state}')
            # Register files
            # FIXME do some jobs really have no files?
            job_fn_map[job.id] = [x['storedfile_id'] for x in
                    FileJob.objects.filter(job_id=job.pk).values('storedfile_id')]
            # Register dsets FIXME
            ds_ids = jwrapper.get_dsids_jobrunner(**job.kwargs)
            job_ds_map[job.id] = set(ds_ids)
            # New jobs can be running/error/just-revoked when e.g. the jobrunner is restarted:
            if job.state in [Jobstates.PROCESSING, Jobstates.ERROR, Jobstates.REVOKING]:
                active_jobs.add(job.pk)

        # Just print info about ERROR-jobs, but also process tasks
        # job.refresh_from_db() # speed up job runner after processing
        tasks = job.task_set.all()
        if job.state == Jobstates.DONE:
            del(job_ds_map[job.id])
            del(job_fn_map[job.id])
            active_jobs.remove(job.id)

        elif job.state == Jobstates.ERROR:
            print('ERROR MESSAGES:')
            if not tasks.count():
                print(f'Job {job.id} has state error, without tasks')
            if hasattr(job, 'joberror'):
                print(job.joberror.message)
                
        elif job.state == Jobstates.REVOKING:
            # canceling job from revoke status only happens here
            # we do a new query update where state=revoking, as to not get race with someone quickly un-revoking
            canceled = Job.objects.filter(pk=job.pk, state=jj.Jobstates.REVOKING).update(state=jj.Jobstates.CANCELED)
            # There is an extra check if the job actually has revokable tasks
            # Most jobs are not, but very long running user-ordered tasks are.
            if jwrapper.revokable and canceled:
                revoke_and_delete_tasks(tasks)
            del(job_fn_map[job.id])
            del(job_ds_map[job.id])
            active_jobs.remove(job.id)

        # Ongoing jobs get updated
        elif job.state == Jobstates.PROCESSING:
            print(f'Updating task status for active job {job.id} - {job.funcname}')
            if tasks.filter(state=states.FAILURE).count():
                print(f'Failed tasks for job {job.id}, setting to error')
                jwrapper.set_error(job, errmsg=False)
            elif tasks.count() == tasks.filter(state=states.SUCCESS).count():
                print(f'All tasks finished, job {job.id} done')
                job.state = Jobstates.DONE
                job.save()
                del(job_ds_map[job.id])
                del(job_fn_map[job.id])
                active_jobs.remove(job.id)
            else:
                print(f'Job {job.id} continues processing, no failed tasks')

        elif job.state == Jobstates.HOLD:
            # When held job is found, add it to active (remove again at starting
            # job loop) - so it will stop downstream jobs
            active_jobs.add(job.id)

        # Pending jobs are trickier, wait queueing until any previous job on same files
        # is finished. Errored jobs thus block pending jobs if they are on same files.
        elif job.state == Jobstates.PENDING:
            # In case job changed from error to pending by retry, remove it from active jobs
            active_jobs.discard(job.id)
            jobfiles = job_fn_map[job.id]
            job_ds = job_ds_map[job.id]
            # do not start job if there is activity on files or datasets
            active_files = {sf for jid in active_jobs for sf in job_fn_map[jid]}
            # Blocking Dataset seem to be never used in production 
#            active_datasets = {ds for jid in active_jobs for ds in job_ds_map[jid]}
            if blocking_files := active_files.intersection(jobfiles):
                print(f'Deferring job since files {blocking_files} are being used in other job')
#            elif blocking_ds := active_datasets.intersection(job_ds):
#                print(f'Deferring job since datasets {blocking_ds} are being used in other job')
            else:
                print('Executing job {}'.format(job.id))
                active_jobs.add(job.id)
                job.state = Jobstates.PROCESSING
                if errmsg := jwrapper.check_error_on_running(**job.kwargs):
                    jwrapper.set_error(job, errmsg=errmsg)
                else:
                    try:
                        jwrapper.run(**job.kwargs)
                    except RuntimeError as e:
                        print('Error occurred, trying again automatically in next round')
                        jwrapper.set_error(job, errmsg=str(e))
                    except Exception as e:
                        print(f'Error occurred: {e} --- not executing this job')
                        jwrapper.set_error(job, errmsg=str(e))
                    else:
                        # PROCESSING state update saved here:
                        job.save()
    return job_fn_map, job_ds_map, active_jobs
