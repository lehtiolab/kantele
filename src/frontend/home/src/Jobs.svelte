<script>

import {querystring} from 'svelte-spa-router';
import { getJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'

let selectedjobs = [];
let notif = {errors: {}, messages: {}};
let treatItems;
let jobs;

const tablefields = [
  {id: 'state', name: '__hourglass-half', type: 'state', multi: false},
  {id: 'name', name: 'Job name', type: 'str', multi: false},
  {id: 'files', name: '', help: 'Files', type: 'icon', icon: 'database', multi: false, links: 'fn_ids', linkroute: '#/files'},
  {id: 'analysis', name: '', help: 'Analysis', type: 'icon', icon: 'cogs', multi: false, links: 'analysis', linkroute: '#/analyses'},
  {id: 'datasets', name: '', help: 'Datasets', type: 'icon', icon: 'clipboard-list', multi: false, links: 'dset_ids', linkroute: '#/datasets'},
  {id: 'usr', name: 'Users', type: 'str', multi: false},
  {id: 'date', name: 'Date', type: 'str', multi: false},
  {id: 'actions', name: 'Actions', type: 'button', multi: true, confirm: ['delete']},
];

const fixedbuttons = [
  {name: '__redo', alt: 'Refresh job', action: refreshJob},
]


function retryJob(jobid) {
  const callback = (job) => {refreshJob(job.id)};
  treatItems('/jobs/retry/', 'job', 'retrying', callback, [jobid]);
}

function pauseJob(jobid) {
  const callback = (job) => {refreshJob(job.id)};
  treatItems('/jobs/pause/', 'job', 'pausing', callback, [jobid]);
}

function resumeJob(jobid) {
  const callback = (job) => {refreshJob(job.id)};
  treatItems('/jobs/resume/', 'job', 'resuming', callback, [jobid]);
}

function deleteJob(jobid) {
  const callback = (job) => {refreshJob(job.id)};
  treatItems('/jobs/delete/', 'job', 'deleting', callback, [jobid]);
}

function jobAction(action, jobid) {
  const actionmap = {
    retry: retryJob,
    'force retry': retryJob,
    pause: pauseJob,
    resume: resumeJob,
    delete: deleteJob,
  }
  actionmap[action](jobid);
}

async function refreshJob(jobid) {
  const resp = await getJSON(`/refresh/job/${jobid}`);
  if (!resp.ok) {
    const msg = `Something went wrong trying to refresh job ${jobid}: ${resp.error}`;
    notif.errors[msg] = 1;
     setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
   } else {
     jobs[jobid] = Object.assign(jobs[jobid], resp);
   }
}

async function getJobDetails(jobId) {
       const resp = await getJSON(`/show/job/${jobId}`);
  return `
    <p>${resp.timestamp}</p>
    <p>${resp.files} files in job</p>
    ${resp.errmsg ? `<p>Error msg: ${resp.errmsg}</p>` : ''}
    <p>
    <span class="tag is-danger">${resp.tasks.error}</span>
    <span class="tag is-warning">${resp.tasks.procpen}</span>
    <span class="tag is-success">${resp.tasks.done}</span>
    </p>
  `;
}
</script>

<Tabs tabshow="Jobs" notif={notif} />

<Table tab="Jobs" bind:items={jobs} bind:treatItems={treatItems} bind:notif={notif} bind:selected={selectedjobs} fetchUrl="/show/jobs" findUrl="find/jobs" getdetails={getJobDetails} fixedbuttons={fixedbuttons} fields={tablefields} inactive={['canceled']} on:rowAction={e => jobAction(e.detail.action, e.detail.id)} />
