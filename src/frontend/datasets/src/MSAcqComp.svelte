<script>

import { onMount } from 'svelte';
import { getJSON, postJSON } from './funcJSON.js'
import Param from './Param.svelte';
import ErrorNotif from './ErrorNotif.svelte';
import { dataset_id } from './stores.js';

export let errors;

let acquierrors = [];


let dsinfo = {
  operator_id: '',
  dynamic_rp: false,
  rp_length: '',
  params: [],
  acqmode: '',
}

let acqdata = {
  operators: [],
  acqmodes: [],
}

let saved = false;
let edited = false;
$: stored = $dataset_id && !edited && saved;

function editMade() { 
  errors = errors.length ? validate() : [];
  edited = true;
}

export function validate() {
  let comperrors = [];
	if (!dsinfo.operator_id) {
		comperrors.push('Operator is required');
	}
	if (!dsinfo.acqmode) {
		comperrors.push('Acquisition mode is required');
	}
	if (!dsinfo.dynamic_rp && !dsinfo.rp_length) {
		comperrors.push('Reverse phase is required');
	}
	for (let key in dsinfo.params) {
    if (dsinfo.params[key].model === undefined || dsinfo.params[key].model === '') {
			comperrors.push(dsinfo.params[key].title + ' is required');
		}
	}
  return comperrors;
}

export async function save() {
  acquierrors = [];
  errors = validate();
  if (errors.length === 0) { 
    let postdata = {
      dataset_id: $dataset_id,
      operator_id: dsinfo.operator_id,
      acqmode: dsinfo.acqmode,
      params: dsinfo.params,
      rp_length: dsinfo.dynamic_rp ? '' : dsinfo.rp_length,
    };
    let url = '/datasets/save/msacq/';
    const resp = await postJSON(url, postdata);
    if ('error' in resp) {
      acquierrors = [...acquierrors, resp.error];
    } 
    fetchData();
  }
}


async function fetchData() {
  let url = '/datasets/show/msacq/';
  url = $dataset_id ? url + $dataset_id : url;
	const response = await getJSON(url);
  for (let [key, val] of Object.entries(response.acqdata)) { acqdata[key] = val; }
  for (let [key, val] of Object.entries(response.dsinfo)) { dsinfo[key] = val; }
  edited = false;
  if (dsinfo.operator_id) {
    saved = true;
  }
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
  MS Acquisition 
  <button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
  <button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
</h5>

<ErrorNotif errors={acquierrors} />

<div class="field">
  <label class="label">MS Operator</label>
  <div class="control">
    <div class="select">
      <select on:change={editMade} bind:value={dsinfo.operator_id}>
        <option disabled value="">Please select one</option>
        {#each acqdata.operators as operator}
        <option value={operator.id}>{operator.name}</option>
        {/each}
      </select>
    </div>
  </div>
</div>


<div class="field">
  <label class="label">Acquisition mode</label>
  <div class="control">
    <div class="select">
      <select on:change={editMade} bind:value={dsinfo.acqmode}>
        <option disabled value="">Please select one</option>
        {#each acqdata.acqmodes as aq}
        <option value={aq.id}>{aq.name}</option>
        {/each}
      </select>
    </div>
  </div>
</div>

<div class="field">
  <label class="label">Reverse phase length</label>
  <div class="control">
    <label class="checkbox">
      <input type="checkbox" on:change={editMade} bind:checked={dsinfo.dynamic_rp}>Dynamic
    </label>
    {#if !dsinfo.dynamic_rp}
    <input type="number" on:change={editMade} class="input" placeholder="in minutes" bind:value={dsinfo.rp_length}>
    {/if}
  </div>
</div>

{#each Object.entries(dsinfo.params) as [param_id, param]}
<Param bind:param={param} on:edited={editMade}/>
{/each}

<button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
<button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
