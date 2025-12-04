<script>

import { querystring, push } from 'svelte-spa-router';
import { onMount } from 'svelte';
import { flashtime, statecolors, helptexts } from '../../util.js'
import { postJSON, getJSON, parseResponse } from '../../datasets/src/funcJSON.js'
import { closeProject } from '../../home/src/util.js'
import Inputfield from './Inputfield.svelte';
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import Method from './Protocols.svelte';
import Pipeline from './Pipeline.svelte';
import Table from '../../home/src/Table.svelte';


import * as Plot from '@observablehq/plot';

let notif = {errors: {}, messages: {}, links: {}};

let selectedPipeline;
let selectedDisabledPipeline;
let selectedDisabledMethod = {};
let protocols = cf_init_data.protocols;
let pipelines = cf_init_data.pipelines;
let showAddPipeField = false;
let showAddPipeVersionField = false;
let newPipeName = '';
let all_enzymes = cf_init_data.enzymes;
let tabshow;

// Proj table
let loadedProjects = {};
$: { Object.keys(loadedProjects).length ? replot(plot_sort_by) : false ;
}
let selectedProjs = false;
let addItem;
const inactive = ['inactive'];
const fixedbuttons = [
]
function showDetails(event) {
  detailsVisible = event.detail.ids;
}

const actionmap = {
  close: closeProject,
}
async function doAction(action, projid) {
  await actionmap[action](projid, loadedProjects, notif);
  loadedProjects = loadedProjects;
  updateNotif();
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

async function getProjDetails(projid) {
	const resp = await getJSON(`/show/project/${projid}`);
  return `
    <p><span class="has-text-weight-bold">Storage amount:</span> ${resp.stored_total_xbytes}</p>
    <p><span class="has-text-weight-bold">Owners:</span> ${resp.owners.join(', ')}</p>
    <hr>
    ${Object.entries(resp.nrstoredfiles).map(x => {return `<div>${x[1]} stored files of type ${x[0]}</div>`;}).join('')}
    <div>Instrument(s) used: <b>${resp.instruments.join(', ')}</b></div>
    `;
}
const tablefields = [
  {id: 'name', name: 'Name', type: 'str', multi: false},
  {id: 'ptype', name: 'Type', type: 'str', multi: false},
  {id: 'dsets', name: 'Datasets', type: 'count', count: 'dset_ids', multi: false},
  {id: 'start', name: 'Registered', type: 'str', multi: false},
  {id: 'lastactive', name: 'Last active', type: 'str', multi: false},
  {id: 'actions', name: '', type: 'button', multi: true, confirm: ['close']},
];

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


let plots;
let plot_sort_asc = true;
let plot_sort_by = 'owner';

const sortkeys = {start: 'Start date',
  end: 'End date',
  owner: 'User',
  duration: 'Duration',
  stage: 'Current/last stage',
}
  

async function replot(sortkey) {
  const url = 'dashboard/projects/'
  const active_projid = Object.entries(loadedProjects)
    .map(kv => kv[0]);
  const resp  = await postJSON(url, {proj_ids: active_projid});
  if (!resp.ok) {
    showError(resp.error);
    return;
  }

  let perProject = resp.per_proj.map(x => Object.assign(x, {
    start: new Date(x.start),
    startdset: new Date(x.startdset),
    end: new Date(x.end),
    state: x.open ? 'Open' : 'Closed',
  })); 
  perProject = perProject.map(x => Object.assign(x, {
    // Bin projects start and end by month for aggregate plot
    monthend: (parseInt(`${x.end.getFullYear()}00`) + x.end.getMonth()+1).toString(),
    monthstart: (parseInt(`${x.start.getFullYear()}00`) + x.start.getMonth()+1).toString(),
    monthstartdset: (parseInt(`${x.startdset.getFullYear()}00`) + x.startdset.getMonth()+1).toString(),
  }))
  
  const today = new Date(resp.today);
  const firstdate = new Date(resp.firstdate);

  // Sorting: switch asc / desc when pressing sort button twice
  plot_sort_asc = plot_sort_by === sortkey ? plot_sort_asc === false : plot_sort_asc;
  const sort_order = `${{true: 'a', false: 'de'}[plot_sort_asc]}scending`;
  plot_sort_by = sortkey;
  const bar_to_sort = ['owner', 'end', 'stage'].indexOf(sortkey) > -1 ? 'last' : 'first';
  let individual_proj_plot;
  let aggregate_plot;

  try {
    individual_proj_plot = Plot.plot({
          marginRight: 130,
          marginRight:150,
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
                        sort: {
                          y: '-data',
                          reduce: (D) => D.find((d) => d[bar_to_sort])?.[sortkey],
                          order: sort_order,
                        },
                      }),
                Plot.axisY({
                  text: (y) => perProject.find((d) => d[bar_to_sort] && d.dset === y)?.owner,
                  tickSize: 0,
                  anchor: 'right',
                  label: null,
                }),
                Plot.text(perProject, {
                  filter: (d) => d.first,
                        x: firstdate,
                        y: "dset",
                        text: 'proj',
                        textAnchor: "start",
                        dx: 5,
                      }),

                Plot.text(perProject, {
                  filter: (d) => d.first && resp.closedates[d.pid],
                    x: (d) => new Date(resp.closedates[d.pid]),
                    y: "dset",
                    //U+2705
                    text: (d) => '✅',
                    }),
                Plot.text(perProject, {
                  filter: (d) => d.first && !resp.closedates[d.pid],
                    x: (d) => today,
                    y: "dset",
                    //U+23F1
                    text: (d) => '⏳',
                    dx: -10,
                    }),
              ]
      })
  const colorscale = individual_proj_plot.scale('color');

   const xlabels = {duration: 'Days',
     start: 'Month',
     end: 'Month',
     owner: null,
     stage: null,
   }
   const ylabels = {
     duration: 'Datasets (stacked)',
     start: 'Datasets',
     end: 'Datasets',
     owner: 'Datasets (stacked)',
     stage: 'Datasets',
   }
  const bar_to_aggregate = ['owner', 'end', 'start', 'stage'].indexOf(sortkey) > -1 ? 'last' : 'first';
  const agg_plotdata = perProject.filter(d => d[bar_to_aggregate]);
  let sortkey_mod = ['stage', 'owner'].indexOf(sortkey) > -1 ? sortkey : `month${sortkey}`;
  sortkey_mod = sortkey_mod === 'monthstart' ? 'monthstartdset' : sortkey_mod;
  const rotation = ['stage', 'owner'].indexOf(sortkey) > -1 ? 0 : 90;

   if (['start', 'end', 'owner', 'stage'].indexOf(sortkey) > -1) {
     aggregate_plot = Plot.plot({
            marginBottom: 75,
            color: colorscale,
            y: { label: ylabels[sortkey], grid: true },
            x: { axis: null},
            fx: {axis: 'bottom', tickRotate: rotation, label: xlabels[sortkey]},
            marks: [
                  Plot.barY(agg_plotdata,
                    Plot.groupX({y: 'count'}, {fx: sortkey_mod, x: 'state', fill: 'stage', opacity: 0.3}),
                  ),
                  Plot.text(agg_plotdata,
                    Plot.groupX({text: "first", y: "count"}, {
                      fx: sortkey_mod,
                      x: 'state',
                      text: (d) => d.open ? '⏳' : '✅',
                      fontSize: 20,
                      dy: -15,
                    }),
                  ),
            Plot.ruleY([0]),
                ]
     })

   } else if (sortkey === 'duration') {
 // bin plot

     aggregate_plot= Plot.plot({
            color: {
                   legend: true,
                    opacity: 0.3,
                },
            y: { label: 'Datasets', grid: true },
            x: {label: xlabels[sortkey]},
            marks: [
                  Plot.barY(agg_plotdata,
                    Plot.binX({y: 'count'},
                      {x: {thresholds: 20, value: sortkey}, fill: 'state', opacity: 0.3}),
                  ),
                  Plot.ruleY([0]),
            ]
     })
    }

  } catch (error) {
    console.log(error);
    showError(`Some error occurred: ${error}`);
  }

  if (individual_proj_plot) {
    while (plots.firstChild) {
      plots?.lastChild?.remove();
    }
    plots?.append(individual_proj_plot);
    plots?.append(aggregate_plot);
  }
}

onMount(async() => {
  push(`#/?tab=dash&q=type:cf,active:yes`);
  tabshow = 'dashboard';
})

async function openDash() {
  tabshow = 'dashboard';
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

<div class="box" id="plots">
  <h4 class="title is-4">Projects</h4>
  <span class="is-size-7">Sort and aggregate on: </span>
  {#each Object.entries(sortkeys) as [key, txt] }
  <button class={`${plot_sort_by === key ? 'is-focused' : ''} button is-small`} on:click={e => replot(key)}>{txt}</button>

  {/each}

  <div style="display: flex" bind:this={plots}></div>
</div>

<Table tab="Projects" bind:addItem={addItem}
  bind:notif={notif}
  bind:selected={selectedProjs}
  bind:items={loadedProjects}
  fetchUrl="/show/projects"
  findUrl="/show/projects"
  show_deleted_or_q="type:cf, from:2025, to:20250801, from:202504, active:true/false/yes/no, user:username"
  defaultQ="type:cf active:true"
  getdetails={getProjDetails}
  fixedbuttons={fixedbuttons}
  fields={tablefields}
  inactive={inactive}
  on:detailview={showDetails}
  allowedActions={Object.keys(actionmap)}
  on:rowAction={e => doAction(e.detail.action, e.detail.id)}
  />

  <div class="columns">
    <div class="column">
    </div>
    <div class="column">
    </div>
  </div>
  {/if}

