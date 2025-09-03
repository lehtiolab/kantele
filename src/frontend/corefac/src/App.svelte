<script>

import { flashtime, statecolors, helptexts } from '../../util.js'
import { postJSON } from '../../datasets/src/funcJSON.js'
import Inputfield from './Inputfield.svelte';
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import Method from './Protocols.svelte';
import Pipeline from './Pipeline.svelte';
import * as Plot from '@observablehq/plot';

let notif = {errors: {}, messages: {}, links: {}};

let selectedPipeline;
let selectedDisabledPipeline;
let selectedDisabledMethod = {};
let protocols = cf_init_data.protocols;
let pipelines = cf_init_data.pipelines;
let perProject = cf_init_data.dash.per_proj;
let showAddPipeField = false;
let showAddPipeVersionField = false;
let newPipeName = '';
let all_enzymes = cf_init_data.enzymes;
let tabshow = 'dashboard';

let plots

let flattened_protocols;
$: {
  flattened_protocols = Object.fromEntries(Object.values(protocols)
    .filter(x => x.methods.length && x.active)
    .flatMap(prepstep => prepstep.methods
      .filter(x => x.versions.length && x.active)
      .flatMap(meth => meth.versions
        .filter(x => x.active)
        .map(ver => {
          return {name: `${prepstep.title} - ${meth.name} - ${ver.doi} - ${ver.version}`, id: ver.id} })
      )
    ).map(x => [x.id, x]));
}

let selectable_pipelines;
let selectable_inactive_pipelines;

$: {
  selectable_pipelines = Object.fromEntries(Object.values(pipelines)
    .filter(x => x.active)
    .map(x => {return {name: `${x.name} - ${x.version}`, id: x.id}})
    .map(x => [x.id, x]));
  selectable_inactive_pipelines = Object.fromEntries(Object.values(pipelines)
    .filter(x => !x.active)
    .map(x => {return {name: `${x.name} - ${x.version}`, id: x.id}})
    .map(x => [x.id, x]));
}

function showError(error) {
  notif.errors[error] = 1;
  setTimeout(function(msg) { notif.errors[error] = 0 } , flashtime, error);
}

function startNewPipelineInput() {
  selectedPipeline = false;
  showAddPipeVersionField = false;
  showAddPipeField = true;
  newPipeName = '';
}


function startNewPipelineVersionInput() {
  showAddPipeVersionField = true;
  newPipeName = '';
}


function stopNewPipelineInput() {
  newPipeName = '';
  showAddPipeVersionField = false;
  showAddPipeField = false;
}


async function addMethod(name, category_id) {
  const url = 'sampleprep/method/add/';
  const resp = await postJSON(url, {newname: name, param_id: category_id});
  if (resp.error) {
    showError(resp.error);
  } else {
    protocols[category_id].methods.push({name: name, id: resp.id, versions: [], active: true});
    protocols = protocols;
  }
}


async function activateMethod(proto_id) {
  const url = 'sampleprep/method/enable/';
  const resp = await postJSON(url, {paramopt_id: selectedDisabledMethod[proto_id]});
  if (resp.error) {
    showError(resp.error);
  } else {
    protocols[proto_id].methods
      .filter(x => x.id === selectedDisabledMethod[proto_id])
      .forEach(x => {
        x.active = true;
      });
    protocols = protocols;

  }
}


async function archiveMethod(method) {
  const url = 'sampleprep/method/disable/';
  const resp = await postJSON(url, {paramopt_id: method.id});
  if (resp.error) {
    showError(resp.error);
  } else {
    method.active = false;
    protocols = protocols;
  }
}


async function deleteMethod(method, category_id) {
  const url = 'sampleprep/method/delete/';
  const resp = await postJSON(url, {paramopt_id: method.id});
  if (resp.error) {
    showError(resp.error);
  } else {
    protocols[category_id].methods = protocols[category_id].methods.filter(x => x.id != method.id);
  }
}


async function addPipeline(pipeversion) {
  const url = 'sampleprep/pipeline/add/';
  const pipename = newPipeName || pipelines[selectedPipeline].name
  const resp = await postJSON(url, {name: pipename, version: pipeversion});
  if (resp.error) {
    showError(resp.error);
  } else {
    pipelines[resp.id] = {id: resp.id, pipe_id: resp.pipe_id, name: pipename, version: pipeversion, steps: [], active: true};
    selectedPipeline = resp.id;
    stopNewPipelineInput();
  }
}


async function enablePipeline() {
  const url = 'sampleprep/pipeline/enable/';
  let pipe = pipelines[selectedDisabledPipeline];
  const resp = await postJSON(url, {id: pipe.id})
  if (resp.error) {
    showError(resp.error);
  } else {
    pipe.active = true;
    pipelines = pipelines;
  }
}


async function deletePipeline(pvid) {
  const url = 'sampleprep/pipeline/delete/';
  const resp = await postJSON(url, {id: pvid});
  if (resp.error) {
    showError(resp.error);
  } else {
    delete(pipelines[pvid]);
    pipelines = pipelines;
  }
  selectedPipeline = false;
}


async function fetchData() {
  return {
    perProject: [
    {proj: 'test1', stage: 'Opened', start: new Date('2025-07-01'), end: new Date('2025-07-10')},
    {proj: 'test1', stage: 'Prep', start: new Date('2025-07-13'), end: new Date('2025-07-15')},
    {proj: 'test1', stage: 'MS queue', start: new Date('2025-07-15'), end: new Date('2025-07-20')},

    {proj: 'haha', stage: 'Opened', start: new Date('2025-07-02'), end: new Date('2025-07-03')},
    {proj: 'haha', stage: 'Prep', start: new Date('2025-07-03'), end: new Date('2025-07-05')},
    {proj: 'haha', stage: 'MS queue', start: new Date('2025-07-05'), end: new Date('2025-07-20')},
  ]
}
}


async function replot() {

  let individual_proj_plot;

  perProject = perProject.map(x => Object.assign(x, {start: new Date(x.start), end: new Date(x.end)}))
//  const x  = await fetchData();
  //perProject = x.perProject;
  //console.log(perProject);

  try {
    individual_proj_plot = 
      Plot.plot({
          marginLeft: 130,
          axis: null,
          color: {
                 legend: true,
                  opacity: 0.3,
              },
          x: {
                axis: "top",
                grid: true,
              },
          marks: [
                Plot.barX(perProject, {
                        x1: "start",
                        x2: "end",
                        y: "dset",
                        fill: "stage",
                  opacity: 0.3,
                        //sort: {y: "x1"}
                      }),
                Plot.text(perProject, {
                  filter: (d) => d.first,
                        x: "start",
                        y: "dset",
                        text: "proj",
                        textAnchor: "end",
                        dx: -3
                      })
              ]
      })


  } catch (error) {
    console.log(error);
    showError(`Some error occurred: ${error}`);
  }

  if (individual_proj_plot) {
    console.log('add to plots');
    plots?.append(individual_proj_plot);
  }
}

async function openDash() {
  tabshow = 'dashboard';
  console.log('ja');
  await replot();
  console.log('ja');
}
</script>

<style>
.errormsg {
  position: -webkit-sticky;
  position: sticky;
  top: 20px;
  z-index: 50000;
}
</style>

<div class="errormsg">
{#if Object.values(notif.errors).some(x => x === 1)}
<div class="notification is-danger is-light"> 
    {#each Object.entries(notif.errors).filter(x => x[1] == 1).map(x=>x[0]) as error}
    <div>{error}</div>
    {/each}
</div>
{/if}

{#if Object.values(notif.links).some(x => x === 1)}
<div class="notification is-danger is-light errormsg"> 
    {#each Object.entries(notif.links).filter(x => x[1] == 1).map(x=>x[0]) as link}
    <div>Click here: <a target="_blank" href={link}>here</a></div>
    {/each}
</div>
{/if}

{#if Object.values(notif.messages).some(x => x === 1)}
<div class="notification is-success is-light errormsg"> 
    {#each Object.entries(notif.messages).filter(x => x[1] == 1).map(x=>x[0]) as message}
    <div>{message}</div>
    {/each}
</div>
{/if}
</div>

<div class="tabs is-toggle is-centered is-small">
	<ul>
    <li class={tabshow === 'admin' ? 'is-active': ''}><a on:click={e => tabshow = 'admin'}>
        <span>Admin</span>
    </li>
    <li class={tabshow === 'dashboard' ? 'is-active': ''}><a on:click={openDash}>
        <span>Dashboard</span>
    </li>
	</ul>
</div>

<div class="content">
  {#if tabshow === 'admin'}
  <div class="columns">
    <div class="column">
      <div class="box has-background-info-light">

       ILab token
      </div>
      <div class="box has-background-info-light">

       Sample locations 
      </div>
    </div>
    <div class="column">
      <div class="box">
        <h4 class="title is-4">Sample prep protocols</h4>
        {#each Object.values(protocols) as proto}
          <hr />
          <h5 class="title is-5">{proto.title}</h5>
          <Inputfield addIcon={true} title={`${proto.title.toLowerCase()} method`} on:newvalue={e => addMethod(e.detail.text, proto.id)} />
  
          {#each proto.methods.filter(x => x.active) as meth}
<Method {meth} on:archive={e => archiveMethod(meth)} on:delete={e => deleteMethod(meth, proto.id)} on:error={e => showError(e.detail.error)} on:updateprotocols={e => protocols = protocols} />
          {/each}

          {#if proto.methods.filter(x => !x.active).length}
          <h6 class="title is-6">{proto.title}, disabled</h6>
          <DynamicSelect placeholder="Type to select method" 
  fixedoptions={Object.fromEntries(proto.methods.filter(x => !x.active).map(x => [x.id, x]))}
  bind:selectval={selectedDisabledMethod[proto.id]} niceName={x => x.name} />
          <button class="button" title="Reactivate" on:click={e => activateMethod(proto.id)}>
            <span class="icon"><i class="has-text-grey far fa-arrow-alt-circle-up"></i></span>
            <span>Reactivate</span>
          </button>
          {/if}  
        {/each}
      </div>
    </div>
    <div class="column">
      <div class="box has-background-link-light">
        <h4 class="title is-4">Pipelines</h4>
        <hr />

        {#if showAddPipeField}
        <button class="button" on:click={stopNewPipelineInput}>Cancel</button>
        <input class="input" type="text" bind:value={newPipeName} placeholder="Add pipeline" />

        {:else}
        <button class="button" on:click={startNewPipelineInput}>New pipeline</button>
        {#if selectedPipeline}
        <button class="button" on:click={startNewPipelineVersionInput}>New pipeline version</button>
        {/if}

        <DynamicSelect placeholder="Type to select pipeline" fixedoptions={selectable_pipelines} bind:selectval={selectedPipeline} niceName={x => x.name} />
        {/if}

        {#if showAddPipeVersionField || newPipeName}
        <Inputfield addIcon={true} title="pipeline version" on:newvalue={e => addPipeline(e.detail.text)} />
        {/if}

        {#if selectedPipeline}
        <Pipeline pipe={selectedPipeline ? pipelines[selectedPipeline] : false} 
          {flattened_protocols} {all_enzymes} 
          bind:enzymes={pipelines[selectedPipeline].enzymes}
          on:error={e => showError(e.detail.error)} 
          on:pipelineupdate={e => pipelines=pipelines}
          on:deletepipeline={e => deletePipeline(e.detail.id)} />
        {/if}
      </div>
      <div class="box">
        {#if Object.values(pipelines).filter(x => !x.active).length}
        <h6 class="title is-6">Disabled pipelines</h6>
        {/if}  
        <DynamicSelect placeholder="Type to select pipeline" fixedoptions={selectable_inactive_pipelines} bind:selectval={selectedDisabledPipeline} niceName={x => x.name} />
        <button class="button" title="Reactivate" on:click={enablePipeline}>
          <span class="icon"><i class="has-text-grey far fa-arrow-alt-circle-up"></i></span>
          <span>Reactivate</span>
        </button>
      </div>
    </div>

  </div>
  {:else if tabshow === 'dashboard'}

<div class="box" bind:this={plots} id="plots">
  <h4 class="title is-4">Projects</h4>
</div>
  <div class="columns">
    <div class="column">
    </div>
    <div class="column">
    </div>
  </div>
  {/if}

</div>
