<script>
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';

export let steps;
export let locked;
export let protocols;
export let all_enzymes = [];
export let enzymes = [];

let editingName = false;
let selectedStep = {};
let showStepEdit = {};
let editMade = false;

let no_enzyme;
$: no_enzyme = enzymes.length === 0;


function addPipelineStep(ix, step_id) {
  const step = {name: protocols[step_id].name, id: protocols[step_id].id};
  steps.splice(ix, 0, step);
  delete(selectedStep[ix]);
  showStepEdit[ix] = false;
  steps = steps.map((x, ix) => {return {...x, ix: ix}});
  editMade = true;
  console.log(steps)
}

function deletePipelineStep(rmix) {
  steps = steps.filter((step, ix) => ix !== rmix);
  steps = steps.map((x, ix) => {return {...x, ix: ix}});
  editMade = true;
}

</script>

    {#if locked}
    <div class="is-size-7"> Locked at {timestamp} </div>
    {/if}
  
<div class="field">
  <label class="label">Enzymes</label>
  <input type="checkbox" bind:checked={no_enzyme} on:click={e => enzymes = []}>No enzyme
  {#each all_enzymes as enzyme}
  <div class="control">
    <input bind:group={enzymes} value={enzyme.id} type="checkbox">{enzyme.name}
  </div>
  {/each}
</div>

  <div class="is-flex is-justify-content-center">
    <div class="tag is-primary is-medium">Samples arrived</div>
  </div>
  <div class="mt-2 is-flex is-justify-content-center">
    <i class="icon fas fa-arrow-down"></i>
    <a on:click={e => showStepEdit[0] = true}><i class="icon fas fa-plus-square"></i></a>
  </div>

  {#if showStepEdit[0]}
  <DynamicSelect placeholder="Type to add pipeline step" fixedoptions={protocols} bind:selectval={selectedStep[0]} niceName={x => x.name} on:selectedvalue={e => addPipelineStep(0, selectedStep[0])} />
  {/if}

  {#each steps as step, ix}
  <div class="is-flex is-justify-content-center">
    <div class="tag is-info is-medium">
      {step.name}
      <button on:click={e => deletePipelineStep(ix)} class="delete is-medium"></button>
    </div>
  </div>

  {#if showStepEdit[ix+1]}
  <DynamicSelect placeholder="Type to add pipeline step" fixedoptions={protocols} bind:selectval={selectedStep[ix+1]} niceName={x => x.name} on:selectedvalue={e => addPipelineStep(ix + 1, selectedStep[ix+1])} />
  {:else}
  <div class="mt-2 is-flex is-justify-content-center">
    <i class="icon fas fa-arrow-down"></i>
    <a on:click={e => showStepEdit[ix+1] = true}><i class="icon fas fa-plus-square"></i></a>
  </div>
  {/if}
  {/each}

  <div class="is-flex is-justify-content-center">
    <div class="tag is-danger is-medium">Samples in MS queue</div>
  </div>
