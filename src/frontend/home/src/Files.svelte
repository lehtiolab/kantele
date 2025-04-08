<script>

import {querystring, push} from 'svelte-spa-router'
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import Details from './FileDetails.svelte'
import { flashtime } from '../../util.js'

let selectedFiles = []
let notif = {errors: {}, messages: {}};
let detailsVisible = false;
let cleanupsize = false;
let fetchingCleanup = false;
let treatItems;

const tablefields = [
  {id: 'jobstate', name: '__hourglass-half', type: 'state', multi: true, links: 'job_ids', linkroute: '#/jobs'},
  {id: 'name', name: 'File', type: 'str', multi: false},
  {id: 'smallstatus', name: '', type: 'smallcoloured', multi: true},
  {id: 'dataset', name: '', type: 'icon', help: 'Dataset', icon: 'clipboard-list', multi: false, links: 'dataset', linkroute: '#/datasets'},
  {id: 'analyses', name: '', type: 'icon', help: 'Analyses', icon: 'cogs', multi: false, links: 'analyses', linkroute: '#/analyses'},
  {id: 'date', name: 'Date', type: 'str', multi: false},
  {id: 'size', name: 'Size', type: 'str', multi: false},
  {id: 'backup', name: 'Backed up', type: 'bool', multi: false},
  {id: 'owner', name: 'Belongs', type: 'str', multi: false},
  {id: 'ftype', name: 'Type', type: 'str', multi: false},
];

//const statecolors = {
//  stored: false,
//}

function showDetails(event) {
  detailsVisible = event.detail.ids;
}

async function getFileDetails(fnId) {
	const resp = await getJSON(`/show/file/${fnId}`);
  return `
    <p><span class="has-text-weight-bold">Producer:</span> ${resp.producer}</p>
    <p><div class="has-text-weight-bold">Storage locations</div>
    ${resp.servers.map(x => `<div class="has-text-primary">${x[0]} / ${x[1]} </div>`)}
    </p>
    ${resp.description ? `<p><span class="has-text-weight-bold">Description:</span> ${resp.description}</p>` : ''}
    `;
}


function reactivateFiles() {
  const callback = (file) => {file.deleted = false; }
  treatItems('files/undelete/', 'file','reactivating', callback, selectedFiles);
}


function archiveFiles() {
  const callback = (file) => {file.deleted = true; }
  treatItems('files/archive/', 'file','archiving', callback, selectedFiles);
}


function purgeFiles() {
}


async function runCleanup() {
  fetchingCleanup = true;
  cleanupsize = false;
  let msg;
  const resp = await postJSON('files/cleanup/', {queue_job: true} );
  if (!resp.ok) {
    if ('error' in resp) {
      msg = resp.error;
    } else {
      msg = 'Something went wrong trying to run mzML clean-up';
    }
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    fetchingCleanup = false;
    msg = 'Queued job to delete old mzML from disk';
    notif.messages[msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
  }
}


async function fetchCleanup() {
  fetchingCleanup = true;
  let msg;
  const resp = await postJSON('files/cleanup/', {queue_job: false} );
  if (!resp.ok) {
    if ('error' in resp) {
      msg = resp.error;
    } else {
      msg = 'Something went wrong trying to fetch mzML clean-up size';
    }
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    cleanupsize = resp.mzml_cleanupsize_raws;
    setTimeout(function() {cleanupsize = false } , 300000);
  }
  fetchingCleanup = false;
}

</script>

<Tabs tabshow="Files" notif={notif} />
{#if cleanupsize}
<a class="button is-small" on:click={runCleanup}>Cleanup mzML for {cleanupsize} of raw files</a>
{:else if fetchingCleanup}
<a class="button is-small is-loading">Cleanup mzML for {cleanupsize} of raw files</a>
{:else}
<a class="button is-small" on:click={fetchCleanup}>Get old mzML cleanup space</a>
{/if}

{#if selectedFiles.length}
<a class="button is-small" title="Move deleted files to active storage (admins only)" on:click={reactivateFiles}>Undelete files</a>
<a class="button is-small" title="Move files to cold storage (admins only)" on:click={archiveFiles}>Archive files</a>
{:else}
<a class="button is-small" title="Move deleted files to active storage (admins only)" disabled>Undelete files</a>
<a class="button is-small" title="Move files to cold storage (admins only)" disabled>Archive files</a>
<a class="button is-small" title="PERMANENTLY delete files from active and cold storage (admins only)" disabled>Purge files</a>
{/if}
  
<Table tab="Files" bind:treatItems={treatItems} bind:notif={notif} bind:selected={selectedFiles} fetchUrl="/show/files" findUrl="/find/files" getdetails={getFileDetails} fields={tablefields} inactive={['deleted']} on:detailview={showDetails} />

{#if detailsVisible}
<Details closeWindow={() => {detailsVisible = false}} fnIds={detailsVisible} />
{/if}
