<script>

import {querystring} from 'svelte-spa-router';
import { getJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import { treatItems } from './util.js'

let selectedjobs = [];
let notif = {errors: {}, messages: {}};
let jobs;

const tablefields = [
  {id: 'state', name: '__hourglass-half', type: 'state', multi: false},
  {id: 'name', name: 'Job name', type: 'str', multi: false},
  {id: 'files', name: '', help: 'Files', type: 'icon', icon: 'database', multi: false, links: 'fn_ids', linkroute: '#/files'},
  {id: 'analysis', name: '', help: 'Analysis', type: 'icon', icon: 'cogs', multi: false, links: 'analysis', linkroute: '#/analyses'},
  {id: 'datasets', name: '', help: 'Datasets', type: 'icon', icon: 'clipboard-list', multi: false, links: 'dset_ids', linkroute: '#/datasets'},
  {id: 'usr', name: 'Users', type: 'str', multi: false},
  {id: 'date', name: 'Date', type: 'str', multi: false},
  {id: 'actions', name: 'Actions', type: 'button', multi: true, confirm: ['delete', 'force delete']},
];

const fixedbuttons = [
  {name: '__redo', alt: 'Refresh job', action: refreshJob},
]


async function retryJob(jobid) {
  await treatItems('/jobs/retry/', 'job', 'retrying', jobid, notif);
  refreshJob(jobid);
  updateNotif();
}

async function holdJob(jobid) {
  await treatItems('/jobs/hold/', 'job', 'holding', jobid, notif);
  refreshJob(jobid);
  updateNotif();
}

async function pauseJob(jobid) {
  await treatItems('/jobs/pause/', 'job', 'pausing', jobid, notif);
  refreshJob(jobid);
  updateNotif();
}

async function resumeJob(jobid) {
  await treatItems('/jobs/resume/', 'job', 'resuming', jobid, notif);
  refreshJob(jobid);
  updateNotif();
}

async function deleteJob(jobid) {
  await treatItems('/jobs/delete/', 'job', 'deleting', jobid, notif);
  refreshJob(jobid);
  updateNotif();
}

const actionmap = {
  retry: retryJob,
  'force retry': retryJob,
  hold: holdJob,
  pause: pauseJob,
  resume: resumeJob,
  delete: deleteJob,
  'force delete': deleteJob,
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
  jobs = jobs;
}

function updateNotif() {
  // This does not work if we import it from e.g. util.js, svelte wont update the components
  // showing the notifications
  Object.entries(notif.errors)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg));
  Object.entries(notif.messages)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg));
  notif = notif;
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

<Table tab="Jobs"
  bind:items={jobs}
  bind:notif={notif}
  bind:selected={selectedjobs}
  fetchUrl="/show/jobs"
  findUrl="/show/jobs"
  defaultQ="state:active"
  show_deleted_or_q="from:2025, to:20250701, state:active state:waiting state:queued, state:error state:old"
  getdetails={getJobDetails}
  fixedbuttons={fixedbuttons}
  fields={tablefields}
  inactive={['canceled']}
  allowedActions={Object.keys(actionmap)}
  on:rowAction={e => actionmap[e.detail.action](e.detail.id)}
  />
