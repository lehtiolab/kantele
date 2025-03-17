<script>

import {querystring, push} from 'svelte-spa-router';
import { getJSON } from '../../datasets/src/funcJSON.js'
import ImportExternal from './ImportExternal.svelte'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import Details from './DatasetDetails.svelte'
import { flashtime } from '../../util.js'

const inactive = ['deleted'];
let importVisible = false;
let selectedDsets = []
let notif = {errors: {}, messages: {}};
let detailsVisible = false;
let treatItems;
let purgeConfirm = false;

const tablefields = [
  {id: 'ptype', name: 'Project type', type: 'str', multi: false},
  {id: 'storestate', name: 'Stored', type: 'tag', multi: false, links: 'fn_ids', linkroute: '#/files/'},
  {id: 'jobstate', name: '__hourglass-half', type: 'state', multi: true, links: 'jobids', linkroute: '#/jobs'},
  {id: 'smallstatus', name: '', type: 'smallcoloured', multi: true},
  {id: 'analyses', name: '', type: 'icon', icon: 'cogs', links: 'ana_ids', linkroute: '#/analyses'},
  {id: 'proj', name: 'Project', type: 'str', multi: false, links: 'proj_ids', linkroute: '#/projects'},
  {id: 'exp', name: 'Experiment', type: 'str', multi: false},
  {id: 'run', name: 'Run', type: 'str', multi: false},
  {id: 'usr', name: 'Creator', type: 'str', multi: false},
  {id: 'dtype', name: 'Datatype', type: 'str', multi: false},
];

const fixedbuttons = [
  {name: '__edit', alt: 'Show metadata', action: showMeta},
]

function showMeta(dsid) {
  window.open(`/datasets/show/${dsid}`, '_blank');
} 

function showDetails(event) {
  detailsVisible = event.detail.ids;
}


async function getDsetDetails(dsetId) {
	const resp = await getJSON(`/show/dataset/${dsetId}`);
  return `
    <p><span class="has-text-weight-bold">Storage location:</span> ${resp.storage_loc}</p>
    <p><span class="has-text-weight-bold">Owners:</span> ${Object.values(resp.owners).join(', ')}</p>
    <hr>
    <p>
    ${Object.entries(resp.nrstoredfiles).map(x => {return `<div>${x[1]} stored files of type ${x[0]}</div>`;}).join('')}
    </p>
    `;
}

async function analyzeDatasets() {
  window.open(`/analysis/new?dsids=${selectedDsets.join(',')}`, '_blank');
}

function archiveDataset() {
  const callback = (dset) => {dset.deleted = true; }
  treatItems('datasets/archive/dataset/', 'dataset', 'archiving', callback, selectedDsets);
}

function reactivateDataset() {
  const callback = (dset) => {dset.deleted = false; }
  treatItems('datasets/undelete/dataset/', 'dataset','reactivating', callback, selectedDsets);
}

function purgeDatasets() {
  const callback = (dset) => {dset.deleted = true; }
  treatItems('datasets/purge/dataset/', 'dataset', 'reactivating', callback, selectedDsets);
}

function setConfirm() {
  purgeConfirm = true;
  setTimeout(() => { purgeConfirm = false} , flashtime);
}

</script>

<Tabs tabshow="Datasets" notif={notif} />

{#if selectedDsets.length}
<a class="button is-small" title="Search MS data" on:click={analyzeDatasets}>Analyze datasets</a>
<a class="button is-small" title="Move datasets to cold storage (delete)" on:click={archiveDataset}>Retire datasets</a>
<a class="button is-small" title="Move datasets to active storage (undelete)" on:click={reactivateDataset}>Reactivate datasets</a>
  {#if purgeConfirm}
  <a class="button is-small is-danger is-light" title="PERMANENTLY delete datasets from active and cold storage" on:click={purgeDatasets}>Are you sure? Purge datasets</a>
  {:else}
  <a class="button is-small" title="PERMANENTLY delete datasets from active and cold storage" on:click={setConfirm}>Purge datasets</a>
  {/if}
{:else}
<a class="button is-small" title="Search MS data" disabled>Analyze datasets</a>
<a class="button is-small" title="Move datasets to cold storage (delete)" disabled>Retire datasets</a>
<a class="button is-small" title="Move datasets to active storage (undelete)" disabled>Reactivate datasets</a>
<a class="button is-small" title="PERMANENTLY delete datasets from active and cold storage" disabled>Purge datasets</a>
{/if}
{#if is_staff}
<a class="button is-small" title="Already downloaded files on tmp inbox" on:click={e => importVisible = importVisible === false}>Import external data</a>
{/if}


<Table tab="Datasets" bind:treatItems={treatItems} bind:notif={notif} bind:selected={selectedDsets} fetchUrl="/show/datasets" findUrl="/find/datasets" getdetails={getDsetDetails} fixedbuttons={fixedbuttons} fields={tablefields} inactive={inactive} on:detailview={showDetails} />

{#if importVisible}
<ImportExternal toggleWindow={e => importVisible = importVisible === false} />
{/if}

{#if detailsVisible}
<Details closeWindow={() => {detailsVisible = false}} dsetIds={detailsVisible} />
{/if}
