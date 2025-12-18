<script>
import { onMount } from 'svelte';
import { getJSON, postJSON } from './funcJSON.js'
import ErrorNotif from './ErrorNotif.svelte';
import { dataset_id } from './stores.js';

export let errors;

let preperrors = [];
let edited = false;
let saved = false;

$: stored = $dataset_id && !edited && saved;


function editMade() { 
  errors = errors.length ? validate() : [];
  edited = true; 
}

let prepdata = {
  quanttype: '',
  quants: {},
}

export function validate() {
  let comperrors = [];
	if (!prepdata.quanttype) {
		comperrors.push('Quant type selection is required');
	}
  return comperrors;
}

export async function save() {
  errors = validate();
  if (!$dataset_id) {
    preperrors = [...preperrors, 'Save dataset before saving sample prep'];
  }
  if (errors.length === 0 && preperrors.length === 0) { 
    let postdata = {
      pooled: true,
      dataset_id: $dataset_id,
      quanttype: prepdata.quanttype,
    };
    let url = '/datasets/save/labelcheck/';
    const resp = await postJSON(url, postdata);
    if (resp.error) {
      errors = [resp.error, ...errors];
    }
    fetchData();
  }
}

async function fetchData() {
  let url = '/datasets/show/labelcheck/';
  url = $dataset_id ? url + $dataset_id : url;
  const response = await getJSON(`${url}?lctype=pooled`);
  for (let [key, val] of Object.entries(response)) { prepdata[key] = val; }
  edited = false;
  if (prepdata.quanttype) {
    saved = true;
  }
}

onMount(async() => {
  await fetchData();
})

</script>


<h5 id="sampleprep" class="has-text-primary title is-5">
  {#if stored}
  <i class="icon fas fa-check-circle"></i>
  {:else}
  <i class="icon fas fa-edit"></i>
  {/if}
  Label check
  <button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
  <button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
</h5>

<ErrorNotif errors={preperrors} />

<div class="field">
  <label class="label">Quant type</label>
  <div class="control">
    <div class="select">
      <select on:change={editMade} bind:value={prepdata.quanttype}>
        <option disabled value="">Please select one</option>
        {#each Object.values(prepdata.quants) as quant}
        <option value={quant.id}>{quant.name}</option>
        {/each}
      </select>
    </div>
  </div>
</div>

<button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
<button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
