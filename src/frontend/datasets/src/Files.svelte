<script>

import { onMount } from 'svelte';
import { getJSON, postJSON } from './funcJSON.js'
import { dataset_id, datasetFiles } from './stores.js';
import ErrorNotif from './ErrorNotif.svelte';

let errors = [];

let files = {
  pendingFiles: [],
  newFiles: {},
  dsfn_order: [],
  newfn_order: [],
};
let acceptedPendingFiles = [];
let addedFiles = {};
let removed_files = {};
let findQuery = '';
let allDsSelector = false;
let allNewSelector = false;
let sortkey = 'date';
let sortascending = {
  'date': true,
  'name': true,
  'size': true,
  'instrument': true,
}
let checkallPending = true;
$: checkallPending = acceptedPendingFiles.length === files.pendingFiles.length;

let ok_files = [];
let outside_files = [];
$: ok_files = Object.values(addedFiles).concat(files.dsfn_order.map(x => $datasetFiles[x])).sort((a, b) => a[sortkey] > b[sortkey] === sortascending[sortkey])
  ;
$: outside_files = Object.values(removed_files).concat(files.newfn_order.map(x => files.newFiles[x])).sort((a, b) => a[sortkey] > b[sortkey] === sortascending[sortkey]);


function reSort(key) {
  if (sortkey === key) {
    sortascending[key] = sortascending[key] === false;
  } else {
    sortkey = key;
  }
}


async function findFiles(event) {
  if (event.keyCode === 13) {
    const response = await getJSON(`/datasets/find/files?q=${findQuery.split(' ').join(',')}`);
    for (let [key, val] of Object.entries(response)) { files[key] = val; }
  }
}

function isoTime(timestamp) {
  let x = new Date(timestamp);
  return x.toISOString();
}

function selectAllNew() {
  let select_state = allNewSelector === false;
  for (let fnid in files.newFiles) {
    files.newFiles[fnid].checked = select_state;
  }
}

$: changed = Object.keys(addedFiles).length || Object.keys(removed_files).length;
$: selectedFiles = Object.values(files.newFiles).concat(Object.values(removed_files)).filter(fn => fn.checked);

function deleteFile(fnid) {
  if (fnid in $datasetFiles) {
    removed_files[fnid] = $datasetFiles[fnid]
    files.dsfn_order = files.dsfn_order.filter(x => x !== fnid);
  } else if (fnid in addedFiles) {
    addedFiles = Object.fromEntries(Object.entries(addedFiles).filter(x => x[1].id !== fnid));
  }
}

function addFiles() {
  for (let fn of Object.values(removed_files).filter(fn => fn.checked)) {
    fn.checked = false;
    //removed_files = file.removed_files.filter[fn.id] = fn;
    delete(removed_files[fn.id]);
    files.dsfn_order = [fn.id].concat(files.dsfn_order);
    //files.newfn_order = files.newfn_order.filter(fnid => fnid !== fn.id);
  }
  for (let fn of Object.values(files.newFiles).filter(fn => fn.checked)) {
    fn.checked = false;
    addedFiles[fn.id] = fn;
    files.newfn_order = files.newfn_order.filter(fnid => fnid !== fn.id);
    //delete(files.newFiles[fn.id]);
  }
}

async function save() {
  let url = '/datasets/save/files/';
  let postdata = {
    dataset_id: $dataset_id,
    added_files: addedFiles,
    removed_files: removed_files,
  };
  const response = await postJSON(url, postdata);
  if (response.error) {
    errors = [...errors, response.error];
  }
  fetchFiles();
}


async function fetchFiles() {
  let url = '/datasets/show/files/';
  url = $dataset_id ? url + $dataset_id : url;
	const response = await getJSON(url);
  for (let [key, val] of Object.entries(response)) { files[key] = val; }
  for (let key in $datasetFiles) { delete($datasetFiles[key]); }
  for (let [key, val] of Object.entries(response.datasetFiles)) {
    $datasetFiles[key] = val;
  }
  acceptedPendingFiles = files.pendingFiles.map(x => x[0]);
  addedFiles = {};
  removed_files = {};
}

function toggleSelectAllPending() {
  if (acceptedPendingFiles.length < files.pendingFiles.length) {
    acceptedPendingFiles = files.pendingFiles.map(x => x[0]);
  } else {
    acceptedPendingFiles = [];
  }
}

async function updatePendingFiles() {
  const url = '/datasets/save/files/pending/';
  const rejectedFiles = files.pendingFiles
    .filter(x => acceptedPendingFiles.indexOf(x[0]) === -1)
    .map(x => x[0]);
  let postdata = {
    dataset_id: $dataset_id,
    accepted_files: acceptedPendingFiles,
    rejected_files: rejectedFiles,
  };
  const response = await postJSON(url, postdata);
  if (response.error) {
    errors = [...errors, response.error];
  } else {
    acceptedPendingFiles = [];
    files.pendingFiles = [];
  }
}


onMount(async() => {
  fetchFiles();
});

</script>

<ErrorNotif errors={Object.values(errors).flat()} />

{#if files.pendingFiles.length}
<article class="message">
  <div class="message-header">
    <p>This datasets has {files.pendingFiles.length} pending files from an instrument</p>
  </div>
  <div class="message-body">
  <div class="control">
    <button class="button" on:click={updatePendingFiles}>
    {#if acceptedPendingFiles.length}
      Keep checked files
    {:else}
      Reject all files
    {/if}
    </button>
  </div>
  <div>
    <label class="checkbox">
      <input on:click={toggleSelectAllPending} checked={checkallPending} type="checkbox">
    </label>
  </div>
  {#each files.pendingFiles as [pk, fn]}
  <p>
    <label class="checkbox">
      <input bind:group={acceptedPendingFiles} value={pk} type="checkbox">
      {fn}
    </label>
  </p>
  {/each}
  </div>
</article>
{/if}
 
<div class="content is-small">
  <input class="input is-small" on:keyup={findFiles} bind:value={findQuery} type="text" placeholder="Type a query and press enter to find analyses">
  <div>Showing {files.newfn_order.length} new files ({selectedFiles.length} selected), {files.dsfn_order.length} files in dataset (incl. {Object.keys(removed_files).length}, excl. {Object.keys(addedFiles).length} added files)</div>
  <div>
    <button on:click={save} class="button is-danger is-small" disabled={!changed}>Save</button>
    <button on:click={fetchFiles} class="button is-info is-small">Revert</button>
    <button on:click={addFiles} class="button is-small" disabled={!selectedFiles.length} >Add selected files</button>
  </div>
  <table class="table">
    <thead>
      <tr>
        <th><input type="checkbox" bind:checked={allNewSelector} on:click={selectAllNew}></th>
        <th></th>
        <th>
          File <span on:click={e => reSort('name')} class="icon is-small"><i class="fas fa-sort"></i></span>
        </th>
        <th>
          Date <span on:click={e => reSort('date')} class="icon is-small"><i class="fas fa-sort"></i></span>
        </th>
        <th>
          Size <span on:click={e => reSort('size')} class="icon is-small"><i class="fas fa-sort"></i></span>
        </th>
        <th>
          Instrument <span on:click={e => reSort('instrument')} class="icon is-small"><i class="fas fa-sort"></i></span>
        </th>
      </tr>
    </thead> 
    <tbody>
      
      {#each ok_files as fn}
      <tr>
        <td><span on:click={e => deleteFile(fn.id)} class="icon is-small has-text-danger"><i class="fas fa-times"></i></span></td>
        <td>
          {#if fn.id in $datasetFiles}
          <span class="icon is-small has-text-primary"><i class="fas fa-database"></i></span>
          {/if}
        </td>
        <td>{fn.name}</td>
        <td>{isoTime(fn.date)}</td>
        <td>{fn.size}MB</td>
        <td>{fn.instrument}</td>
      </tr>
      {/each}
      {#each outside_files as fn}
      <tr>
        <td>
          <input type="checkbox" bind:checked={fn.checked}>
        </td>
        <td></td>
        <td>{fn.name}</td>
        <td>{isoTime(fn.date)}</td>
        <td>{fn.size}MB</td>
        <td>{fn.instrument}</td>
      </tr>
      {/each}
    </tbody>
  </table>
</div>
