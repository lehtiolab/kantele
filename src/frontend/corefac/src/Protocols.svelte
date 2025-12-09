<script>
import { createEventDispatcher } from 'svelte';
import { postJSON } from '../../datasets/src/funcJSON.js'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import Table from '../../home/src/Table.svelte';

const dispatch = createEventDispatcher();

//async function editMethod(name, method) {
//  const url = 'sampleprep/method/edit/';
//  const resp = await postJSON(url, {'newname': name, 'paramopt_id': method.id});
//  if (resp.error) {
//    dispatch('error', {error: resp.error});
//  } else {
//    method.name = name;
//    dispatch('updateprotocols', {});
//  }
//  editing = false;
//}
//

async function addProtocol() {
  const url = 'sampleprep/method/add/';
    console.log(Object.entries(loadedProtocols));
  const resp = await postJSON(url, {name: newprotocol_name, doi: newprot_doi,
    version: newprot_version, param_id: newprot_type_id});
  if (resp.error) {
    dispatch('error', {error: resp.error});
  } else {
    addItem(resp);
    cancelProtocol();
  }
}


async function addProtocolVersion() {
  const url = 'sampleprep/version/add/';
  const resp = await postJSON(url, {'doi': newver_protocol_doi,
    'version': newver_protocol_version, 'paramopt_id': newver_protocol_id});
  if (resp.error) {
    dispatch('error', {error: resp.error});
  } else {
    addItem(resp);
    cancelProtocol();
  }
}


//async function editProtocol() {
//  const url = 'sampleprep/version/edit/';
//  const resp = await postJSON(url, {'doi': newDOI, 'version': newVersion, 'prepprot_id': selectedProtocol.id});
//  if (resp.error) {
//    dispatch('error', {error: resp.error});
//  } else {
//    selectedProtocol.version = newVersion;
//    selectedProtocol.doi = newDOI;
//    dispatch('updateprotocols', {});
//  }
//  cancelProtocol();
//}
//
//
//async function archiveProtocol() {
//  const url = 'sampleprep/version/disable/';
//  const resp = await postJSON(url, {'prepprot_id': selectedProtocol.id});
//  if (resp.error) {
//    dispatch('error', {error: resp.error});
//  } else {
//    selectedProtocol.active = false;
//    meth.versions = meth.versions;
//    selectedProtocol = false;
//    dispatch('updateprotocols', {});
//  }
//  cancelProtocol();
//}
//
async function reactivateProtocolVersion(pver_id) {
  const url = 'sampleprep/version/enable/';
  const resp = await postJSON(url, {'prepprot_id': pver_id});
  if (resp.error) {
    dispatch('error', {error: resp.error});
  } else {
    loadedProtocols[pver_id].inactive = false;
    loadedProtocols[pver_id].actions = ['new version', 'deactivate'];
  }
}


async function deactivateProtocolVersion(pver_id) {
  const url = 'sampleprep/version/disable/';
  const resp = await postJSON(url, {'prepprot_id': pver_id});
  if (resp.error) {
    dispatch('error', {error: resp.error});
  } else {
    inactivate(pver_id);
    loadedProtocols[pver_id].actions = ['reactivate'];
  }
}


async function deleteProtocol() {
  const url = 'sampleprep/version/delete/';
  const resp = await postJSON(url, {'prepprot_ids': selectedProtocols});
  if (resp.error) {
    dispatch('error', {error: resp.error});
  } else {
    selectedProtocols.forEach(x => inactivate(x));
    selectedProtocols = [];
  }
}


function inactivate(pid) {
  loadedProtocols[pid].inactive = true;
  loadedProtocols[pid].actions = [];
}


function cancelProtocol() {
  newprotocol_name = '';
  newprot_type_id = false;
  newprot_version = '';
  newprot_doi = '';
  newver_protocol_id = '';
  newver_protocol_name = '';
  newver_protocol_type = false;
  newver_protocol_version = '';
  newver_protocol_doi = '';
  creatingNewProtocol = false;
  creatingNewVersion = false;
}


//function startEditProtocol() {
//  newVersion = selectedProtocol.version;
//  newDOI = selectedProtocol.doi;
//}

let addItem;
let selectedProtocols = [];
let loadedProtocols;

function getProtocolDetails() {
  // TODO
}


const tablefields = [
  {id: 'name', name: 'Name', type: 'str', multi: false},
  {id: 'version', name: 'Version', type: 'str', multi: false},
  {id: 'doi', name: 'DOI', type: 'str', multi: false},
  {id: 'ptype', name: 'Type', type: 'str', multi: false},
  {id: 'start', name: 'Registered', type: 'str', multi: false},
  {id: 'actions', name: '', type: 'button', multi: true, confirm: ['close']},
];

const actionmap = {
  'new version': newProtocolVersion,
  'reactivate': reactivateProtocolVersion,
  'deactivate': deactivateProtocolVersion,
}

async function doAction(action, protver_id) {
  await actionmap[action](protver_id, loadedProtocols);
  // TODO Update notifs
  loadedProtocols = loadedProtocols;
  //updateNotif();
}

function newProtocolVersion(protver_id) {
  newver_protocol_name = loadedProtocols[protver_id].name;
  newver_protocol_id = loadedProtocols[protver_id].protocol_id;
  newver_protocol_type = loadedProtocols[protver_id].ptype;
  creatingNewVersion = true;
}

function showDetails(event) {
  detailsVisible = event.detail.ids;
}

let creatingNewProtocol = false;
let creatingNewVersion = false;
let newprotocol_name;
let newprot_type_id;
let newprot_version;
let newprot_doi;

let newver_protocol_name;
let newver_protocol_id;
let newver_protocol_type;
let newver_protocol_version;
let newver_protocol_doi;

</script>

<div>
Deactivated protocols can not be added to pipelines
</div>


{#if !creatingNewProtocol && !creatingNewVersion}
  <a class="button is-small" title="Create new protocol" on:click={e => creatingNewProtocol=true}>New protocol</a>
  {#if selectedProtocols.length}
  <a class="button is-small" title="Delete version" on:click={deleteProtocol}>Delete version</a>
  {:else}
  <a class="button is-small" title="Delete version" disabled>Delete version</a>
  {/if}

{:else if creatingNewVersion}
  <div class="box">
    <h5 class="title is-5">New protocol version
      <button on:click={e => creatingNewVersion=false} class="button is-small is-danger">Cancel</button>
    </h5>
  
    <div class="field">
      <label class="label">Protocol name: </label>
{newver_protocol_name}
    </div>
  
    <div class="field">
      <label class="label">Type: </label>
{newver_protocol_type}
    </div>
  
    <div class="field">
      <label class="label">Version</label>
      <input class="input" type="text" bind:value={newver_protocol_version}>
    </div>
  
    <div class="field">
      <label class="label">DOI</label>
      <input class="input" type="text" bind:value={newver_protocol_doi}>
    </div>
  
    <button class="button is-small is-success" on:click={addProtocolVersion}>Save</button>
  
  </div>

{:else if creatingNewProtocol}
  <div class="box">
    <h5 class="title is-5">New protocol
      <button on:click={e => creatingNewProtocol=false} class="button is-small is-danger">Cancel</button>
    </h5>
  
    <div class="field">
      <label class="label">Protocol name</label>
      <input class="input" bind:value={newprotocol_name} type="text" placeholder="Protocol name">
    </div>
  
    <div class="field">
      <label class="label">Type</label>
      <div class="select">
        <select bind:value={newprot_type_id}>
        {#each cf_init_data.ptypes as {id, name}}
          <option value={id}>{name}</option>
        {/each}
        </select>
      </div>
    </div>
  
    <div class="field">
      <label class="label">Version</label>
      <input class="input" type="text" bind:value={newprot_version}>
    </div>
  
    <div class="field">
      <label class="label">DOI</label>
      <input class="input" type="text" bind:value={newprot_doi}>
    </div>
  
    <button class="button is-small is-success" on:click={addProtocol}>Save</button>
  
  </div>
{/if}

<Table tab="Protocols" bind:addItem={addItem}
  bind:selected={selectedProtocols}
  bind:items={loadedProtocols}
  fetchUrl="sampleprep/method/find/"
  findUrl="sampleprep/method/find/"
  show_deleted_or_q="from:2025, to:20250801, from:202504, active:true/false/yes/no"
  defaultQ="active:true"
  getdetails={getProtocolDetails}
  fixedbuttons={[]}
  fields={tablefields}
  inactive={['inactive']}
  on:detailview={showDetails}
  allowedActions={Object.keys(actionmap)}
  on:rowAction={e => doAction(e.detail.action, e.detail.id)}
  />

