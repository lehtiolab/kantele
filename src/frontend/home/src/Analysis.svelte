<script>

import {querystring, push} from 'svelte-spa-router';
import { getJSON } from '../../datasets/src/funcJSON.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import Details from './AnalysisDetails.svelte'
import { flashtime } from '../../util.js'

let selectedAnalyses = [];
let notif = {errors: {}, messages: {}}
let treatItems;
let detailsVisible = false;
let analyses;

const tablefields = [
  {id: 'jobstate', name: '__hourglass-half', type: 'state', multi: false, links: 'jobid', linkroute: '#/jobs'},
  {id: 'name', name: 'Analysis name', type: 'str', multi: false},
  {id: 'files', name: '', help: 'Input files', type: 'icon', icon: 'database', multi: false, links: 'fn_ids', linkroute: '#/files'},
  {id: 'datasets', name: '', help: 'Datasets', type: 'icon', icon: 'clipboard-list', multi: false, links: 'dset_ids', linkroute: '#/datasets'},
  {id: 'mstulos', name: '', help: 'ResultsDB', type: 'icon', icon: 'chart-bar', multi: false, links: 'mstulosq', linkroute: '/mstulos/', qparam: 'q'},
  {id: 'wf', name: 'Workflow', type: 'str', multi: false, links: 'wflink', linkroute: false},
  {id: 'usr', name: 'Users', type: 'str', multi: false},
  {id: 'date', name: 'Date', type: 'str', multi: false},
  {id: 'actions', name: 'Actions', type: 'button', multi: true},
];

const fixedbuttons = [
  {name: '__redo', alt: 'Refresh analysis info', action: refreshAnalysis},
]

function editViewAnalysis(anid) {
  window.open(`/analysis/${anid}`, '_blank');
} 


function stopJob(anid) {
  const callback = (analysis) => {refreshAnalysis(analysis.id)};
  treatItems('/analysis/stop/', 'job for analysis', 'stopping', callback, [anid]);
}

function startJob(anid) {
  const callback = (analysis) => {refreshAnalysis(analysis.id)};
  treatItems('/analysis/start/', 'job for analysis', 'starting', callback, [anid]);
}

function doAction(action, anid) {
  const actionmap = {
    edit: editViewAnalysis,
    view: editViewAnalysis,
    'stop job': stopJob,
    'run job': startJob,
  }
  actionmap[action](anid);
}

async function refreshAnalysis(nfsid) {
  const resp = await getJSON(`/refresh/analysis/${nfsid}`);
  if (!resp.ok) {
    const msg = `Something went wrong trying to refresh analysis data for ${nfsid}: ${resp.error}`;
    notif.errors[msg] = 1;
     setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
   } else {
     analyses[nfsid] = Object.assign(analyses[nfsid], resp);
   }
}


function showDetails(event) {
  detailsVisible = event.detail.ids;
}

async function getAnalysisDetails(anaId) {
	const resp = await getJSON(`/show/analysis/${anaId}`);
  const links = resp.servedfiles.map(([link, name]) => { return `<div><a href="analysis/showfile/${link}" target="_blank">${name}</a></div>`}).join('\n');
  let errors = resp.errmsg ? resp.errmsg.map(x => `<div>${x}</div>`).join() : false;
  errors = errors ? `<div class="notification is-danger is-light"><div>ERROR(s):</div>${errors}</div>` : '';
  return `
    ${errors}
    <p><span class="has-text-weight-bold">Workflow version:</span> ${resp.wf.update}</p>
    <p>${resp.nrfiles} raw files from ${resp.nrdsets} dataset(s) analysed</p>
    <p><span class="has-text-weight-bold">Quant type:</span> ${resp.quants.join(', ')}</p>
    <p>${links}</p>
    <p><span class="has-text-weight-bold">Last lines of log:</span></p>
    <p class="is-family-monospace">${resp.log.join('<br>')}</p>
  `;
}

function deleteAnalyses() {
  const callback = (analysis) => {analysis.deleted = true};
  treatItems('/analysis/delete/', 'analysis', 'deleting', callback, selectedAnalyses);
}

function unDeleteAnalyses() {
  const callback = (analysis) => {analysis.deleted = false};
  treatItems('/analysis/undelete/', 'analysis', 'undeleting', callback, selectedAnalyses);
}


function purgeAnalyses() {
  const callback = (analysis) => {analysis.deleted = true};
  treatItems('/analysis/purge/', 'analysis', 'purging', callback, selectedAnalyses);
}
</script>

<Tabs tabshow="Analyses" notif={notif} />

<a class="button" href="/analysis/new/" target="_blank">New analysis</a>
{#if selectedAnalyses.length}
<a class="button" on:click={deleteAnalyses}>Delete analyses</a>
<a class="button" on:click={unDeleteAnalyses}>Undelete analyses</a>
<a class="button" on:click={purgeAnalyses}>Purge analyses</a>
{:else}
<a class="button" disabled>Delete analyses</a>
<a class="button" disabled>Undelete analyses</a>
<a class="button" disabled>Purge analyses</a>
{/if}

<Table tab="Analyses" bind:items={analyses} bind:treatItems={treatItems} bind:notif={notif} bind:selected={selectedAnalyses} fetchUrl="/show/analyses" findUrl="/find/analyses" on:detailview={showDetails} getdetails={getAnalysisDetails} fixedbuttons={fixedbuttons} fields={tablefields} inactive={['deleted', 'purged']} on:rowAction={e => doAction(e.detail.action, e.detail.id)} />
 
{#if detailsVisible}
<Details closeWindow={() => {detailsVisible = false}} anaIds={detailsVisible} />
{/if}
