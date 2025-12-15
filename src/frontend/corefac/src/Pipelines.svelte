<script>
import { postJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import Pipeline from './Pipeline.svelte';
import Table from '../../home/src/Table.svelte';
import Tabs from '../../home/src/Tabs.svelte'

let addItem;
let selectedPipelines = [];
let loadedPipelines;
let tableOrder = [];
let editing = false;
let newpipe_name;
let newpipe_version;

let newver_id;
let newver_pipeline_name;
let newver_pipeline_id;
let newver_pipeline_version;
let newver_steps;
let newver_enzymes;

let notif = {errors: {}, messages: {}};

const tablefields = [
  {id: 'name', name: 'Name', type: 'str', multi: false},
  {id: 'version', name: 'Version', type: 'str', multi: false},
  {id: 'locked', name: 'Locked', type: 'bool', multi: false},
  {id: 'start', name: 'Registered', type: 'str', multi: false},
  {id: 'actions', name: '', type: 'button', multi: true, confirm: ['close']},
];

const actionmap = {
  'new version': newPipelineVersion,
  'reactivate': reactivatePipelineVersion,
  'deactivate': deactivatePipelineVersion,
  'edit': editPipelineVersion,
  'lock': lockPipeline,
}


function updateNotif() {
  Object.entries(notif.errors)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg));
  Object.entries(notif.messages)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg));
  notif = notif;
}

async function doAction(action, pipever_id) {
  await actionmap[action](pipever_id);
  loadedPipelines = loadedPipelines;
  updateNotif();
}


function createNewPipeline() {
  newver_pipeline_name = '';
  newver_pipeline_id = '';
  newver_id = false;
  newver_pipeline_version = '';
  newver_steps = [];
  newver_enzymes = [];
  editing = true;
}

function newPipelineVersion(pipever_id) {
  newver_pipeline_name = loadedPipelines[pipever_id].name;
  newver_pipeline_id = loadedPipelines[pipever_id].pipeline_id;
  newver_id = false;
  newver_pipeline_version = '';
  newver_steps = [];
  newver_enzymes = [];
  editing = true;
}

function editPipelineVersion(pipever_id) {
  newver_pipeline_name = loadedPipelines[pipever_id].name;
  newver_pipeline_id = loadedPipelines[pipever_id].pipeline_id;
  newver_id = pipever_id;
  newver_pipeline_version = loadedPipelines[pipever_id].version;
  newver_steps = loadedPipelines[pipever_id].steps;
  newver_enzymes = loadedPipelines[pipever_id].enzymes;
  editing = true;
}


async function reactivatePipelineVersion(pipever_id) {
  const url = 'sampleprep/pipeline/enable/';
  const resp = await postJSON(url, {'id': pipever_id});
  if (resp.error) {
    notif.errors[resp.error] = 1;
  } else {
    loadedPipelines[pipever_id].inactive = false;
    if (loadedPipelines[pipever_id].locked) {
      loadedPipelines[pipever_id].actions = ['new version', 'deactivate', 'edit'];
    } else {
      loadedPipelines[pipever_id].actions = ['new version', 'deactivate', 'lock', 'edit'];
    }
  updateNotif();
  }
}

async function deactivatePipelineVersion(pipever_id) {
  const url = 'sampleprep/pipeline/disable/';
  const resp = await postJSON(url, {'id': pipever_id});
  if (resp.error) {
    notif.errors[resp.error] = 1;
  } else {
    inactivate(pipever_id);
    loadedPipelines[pipever_id].actions = ['reactivate'];
  }
  updateNotif();
}

async function deletePipelineVersions() {
  const url = 'sampleprep/pipeline/delete/';
  const resp = await postJSON(url, {'ids': selectedPipelines});
  if (resp.error) {
    notif.errors[resp.error] = 1;
  } else {
    selectedPipelines.forEach(x => inactivate(x));
    selectedPipelines = [];
  }
  updateNotif();
}

async function savePipelineVersion() {
  const url = 'sampleprep/pipeline/save/';
  const resp = await postJSON(url, {id: newver_id, version: newver_pipeline_version,
    pipe_id: newver_pipeline_id, pipename: newver_pipeline_name, steps: newver_steps,
    enzymes: newver_enzymes,
  });
  if (resp.error) {
    notif.errors[resp.error] = 1;
  } else {
    newver_id = resp.pipeline.id;
    newver_pipeline_id = resp.pipeline.pipeline_id;
    loadedPipelines[resp.pipeline.id] = resp.pipeline;
    tableOrder = [resp.pipeline.id, ...tableOrder];
  }
  updateNotif();
}

async function lockPipeline(pipever_id) {
  // First save pipeline, then lock
  const url = 'sampleprep/pipeline/lock/';
  const resp = await postJSON(url, {id: pipever_id});
  if (resp.error) {
    notif.errors[resp.error] = 1;
    updateNotif();
  } else {
    loadedPipelines[pipever_id].locked = true;
    loadedPipelines[pipever_id].actions = ['new version', 'deactivate', 'edit'];
  }
}


function inactivate(pid) {
  loadedPipelines[pid].inactive = true;
  loadedPipelines[pid].actions = [];
}

</script>

<Tabs tabs={['Protocols', 'Pipelines', 'Dashboard']} tabshow="Pipelines" notif={notif} />

<div class="has-text-danger">Pipelines must be locked before use, but locking pipelines is irreversible, they cannot be unlocked</div>

{#if !editing}
  <a class="button is-small" title="Create new pipeline" on:click={createNewPipeline}>New pipeline</a>
  {#if selectedPipelines.length}
  <a class="button is-small" title="Delete version" on:click={deletePipelineVersions}>Delete version</a>
  {:else}
  <a class="button is-small" title="Delete version" disabled>Delete version</a>
  {/if}

{:else}
  <div class="box">
    <h5 class="title is-5">
      {#if newver_id}
        Editing
      {:else}
        New
      {/if}
      pipeline
      {#if newver_pipeline_id}
        version
      {/if}
      <button on:click={e => editing=false} class="button is-small is-danger">Cancel</button>
    </h5>
  
    <div class="field">
      {#if newver_pipeline_id}
      <label class="label">Pipeline name: </label> {newver_pipeline_name}
      {:else}
      <input class="input" type="text" bind:value={newver_pipeline_name}>
      {/if}
    </div>
  
    <div class="field">
      <label class="label">Version</label>
      <input class="input" type="text" bind:value={newver_pipeline_version}>
    </div>
  
    <button class="button is-small is-success" on:click={savePipelineVersion}>Save</button>
  
    <Pipeline bind:steps={newver_steps}
      locked={newver_id ? loadedPipelines[newver_id].locked : false} 
      timestamp={newver_id ? loadedPipelines[newver_id].start : false} 
      protocols={cf_init_data.protocols} all_enzymes={cf_init_data.enzymes} 
      bind:enzymes={newver_enzymes}
      on:error={e => showError(e.detail.error)} 
   />
  </div>
{/if}

<Table tab="Pipelines" bind:addItem={addItem}
  bind:selected={selectedPipelines}
  bind:items={loadedPipelines}
  bind:order={tableOrder}
  bind:notif={notif}
  fetchUrl="sampleprep/pipeline/find/"
  findUrl="sampleprep/pipeline/find/"
  show_deleted_or_q="from:2025, to:20250801, from:202504, active:true/false/yes/no"
  defaultQ="active:true"
  fixedbuttons={[]}
  fields={tablefields}
  inactive={['inactive']}
  allowedActions={Object.keys(actionmap)}
  on:rowAction={e => doAction(e.detail.action, e.detail.id)}
  />
