<script>

import { onMount } from 'svelte';
import { getJSON, postJSON } from './funcJSON.js'
import Param from './Param.svelte';
import ErrorNotif from './ErrorNotif.svelte';
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import DatasetPipeline from './DatasetPipeline.svelte'
import { dataset_id } from './stores.js';

export let errors;

let samplepreperrors = [];
let useTrackingPipeline = false;
let selectedPipeline = false;
let pipelineName = '';
let pipelines = {};
let pipeselector;
let dset_pipe_id;


let dsinfo = {
  params: [],
  enzymes: [],
  no_enzyme: false,
}

let saved = false;
let edited = false;
$: stored = $dataset_id && !edited && saved;

function editMade() { 
  errors = errors.length ? validate() : [];
  edited = true;
}


export async function save() {
  samplepreperrors = [];
  let postdata = {
    dataset_id: $dataset_id,
    enzymes: dsinfo.no_enzyme ? [] : dsinfo.enzymes,
    params: dsinfo.params,
    pipeline: selectedPipeline,
  };
  let url = '/datasets/save/mssampleprep/';
  const resp = await postJSON(url, postdata);
  if ('error' in resp) {
    samplepreperrors = [...samplepreperrors, resp.error];
  } 
  fetchData();
}


async function fetchData() {
  let url = '/datasets/show/mssampleprep/';
  url = $dataset_id ? url + $dataset_id : url;
	const response = await getJSON(url);
  for (let [key, val] of Object.entries(response.dsinfo)) { dsinfo[key] = val; }
  edited = false;
  saved = response.saved;
  pipelines = response.pipelines;
  selectedPipeline = response.dsinfo.pipe_id;
  useTrackingPipeline = selectedPipeline !== false;
  dset_pipe_id = response.dsinfo.dspipe_id;
}


function pipelineSelected() {
  useTrackingPipeline = true;
  pipelines[selectedPipeline].steps.forEach(step => {
    dsinfo.params[step[0]].model = step[1];
  });
  editMade();
}

function togglePipeline() {
  if (useTrackingPipeline) {
    useTrackingPipeline = false;
    selectedPipeline = false;
    // Blank the pipeline name from the selector
    pipelineName = '';
    // If we run immediately then the selectedPipeline will not be updated yet
    setTimeout(pipeselector.inputdone, 20);
    editMade();
  } else {
    useTrackingPipeline = true;
    setTimeout(pipeselector.inputdone, 20);
  }
}

function showError(error) {
  samplepreperrors = [...samplepreperrors, error];
}


onMount(async() => {
  fetchData();
})

</script>

<style>
</style>

<h5 class="has-text-primary title is-5">
  {#if stored}
  <i class="icon fas fa-check-circle"></i>
  {:else}
  <i class="icon fas fa-edit"></i>
  {/if}
  MS sample prep 
  <button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
  <button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
</h5>

<ErrorNotif errors={samplepreperrors} />


<div class="field">
  <label class="label">Sample prep pipeline</label>
  <input type="checkbox" on:change={togglePipeline} checked={useTrackingPipeline}>Use a tracking pipeline
  <DynamicSelect bind:this={pipeselector} placeholder="Type to select an active, locked pipeline" 
    fixedoptions={pipelines}
    bind:intext={pipelineName}
    bind:selectval={selectedPipeline}
    niceName={x => x.name}
    on:selectedvalue={pipelineSelected} />
</div>

{#if useTrackingPipeline && selectedPipeline && dset_pipe_id}
<DatasetPipeline on:error={e => showError(e.detail.error)} pipeSteps={pipelines[selectedPipeline].steps} samplePrepCategories={dsinfo.params} bind:savedStageDates={dsinfo.prepdatetrack} bind:pipeStepsDone={dsinfo.prepsteptrack} bind:dspipeId={dset_pipe_id} />

{:else if !useTrackingPipeline}
<div class="field">
  <label class="label">Enzymes</label>
  <label class="checkbox">
    <input type="checkbox" on:change={editMade} bind:checked={dsinfo.no_enzyme}>No enzyme
  </label>
  {#if !dsinfo.no_enzyme}
  {#each dsinfo.enzymes as enzyme}
  <div>
    <label class="checkbox">
      <input on:change={editMade} bind:checked={enzyme.checked} type="checkbox">{enzyme.name}
    </label>
  </div>
  {/each}
  {/if}
</div>

{#each Object.entries(dsinfo.params) as [param_id, param]}
<Param bind:param={param} on:edited={editMade}/>
{/each}
{/if}

<button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
<button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
