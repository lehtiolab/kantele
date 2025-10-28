<script>

import {querystring, push} from 'svelte-spa-router'
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import Details from './FileDetails.svelte'
import { flashtime } from '../../util.js'
import { treatItems } from './util.js'

let files;
let selectedFiles = []
let notif = {errors: {}, messages: {}};
let detailsVisible = false;

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


async function refreshFile(fnid) {
  const resp = await getJSON(`/refresh/file/${fnid}`);
  if (!resp.ok) {
    const msg = `Something went wrong trying to refresh file ${fnid}: ${resp.error}`;
    notif.errors[msg] = 1;
     setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
   } else {
     files[fnid] = Object.assign(files[fnid], resp);
   }
  files = files;
}

async function archiveFiles() {
  for (let fnid of selectedFiles) {
    await treatItems('files/archive/', 'file','archiving', fnid, notif);
    refreshFile(fnid);
  }
  updateNotif();
}


function purgeFiles() {
}

</script>

<Tabs tabshow="Files" notif={notif} />
{#if selectedFiles.length}
<a class="button is-small" title="Move files to cold storage (admins only)" on:click={archiveFiles}>Archive files</a>
{:else}
<a class="button is-small" title="Move files to cold storage (admins only)" disabled>Archive files</a>
<a class="button is-small" title="PERMANENTLY delete files from active and cold storage (admins only)" disabled>Purge files</a>
{/if}
  
<Table tab="Files" bind:items={files} bind:notif={notif} bind:selected={selectedFiles} fetchUrl="/show/files" findUrl="/find/files" getdetails={getFileDetails} fields={tablefields} inactive={['deleted']} on:detailview={showDetails} />

{#if detailsVisible}
<Details on:refresh={e => refreshFile(e.detail.fnid)} closeWindow={() => {detailsVisible = false}} fnIds={detailsVisible} />
{/if}
