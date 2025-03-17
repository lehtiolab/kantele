<script>
import { getJSON, postJSON } from './funcJSON.js'
import { dataset_id, datatype_id, datasetFiles, projsamples } from './stores.js';
import { onMount } from 'svelte';
import MSAcqComp from './MSAcqComp.svelte';
import MSSamplePrepComp from './MSSamplePrepComp.svelte';
import Samplesheet from './Samplesheet.svelte';
// FIXME msdata should be folded into the MScomponent now we dont have acquisition only anymore
import Msdata from './Msdata.svelte';
import LCheck from './LCheck.svelte';
import PooledLCheck from './PooledLCheck.svelte';
import Files from './Files.svelte';
import ErrorNotif from './ErrorNotif.svelte';
import DynamicSelect from './DynamicSelect.svelte';
  
if (init_dataset_id) { dataset_id.set(init_dataset_id) };

let mssubcomp;
let msacqcomp;
let mssampleprepcomp;
let samplesheet;
let lccomp;
let pooledlc;
let filescomp;
let edited = false;
let errors = {
  basics: [],
  samples: [],
  msacq: [],
  mssampleprep: [],
  lc: [],
};
let saveerrors = Object.assign({}, errors);
let comperrors = [];

// Project info
let project_id = projdata.proj_id;
let project_name = projdata.name;
let ptype = projdata.ptype;
let pi_name = projdata.pi_name;
let isExternal = projdata.isExternal;
let datasettypes = projdata.datasettypes;

let dsinfo = {
  datatype_id: '',
  storage_location: '',
  experiment_id: '',
  runname: '',
  externalcontactmail: '',
  prefrac_id: '',
  prefrac_length: '',
  prefrac_amount: '',
  hiriefrange: '',
}

let experiments = [];
let prefracs = [];
let hirief_ranges = [];

let components = [];
let isNewExperiment = false;
let tabshow = 'meta';
let tabcolor = 'has-text-grey-lighter';
  // Yes, for microscopy/genomics, we need separation between samples/prep
  // files is given, and possibly samples as well, check it out but samples is needed for:
  // - QMS, LCheck, IP, TPP, microscopy QC?, genomics

$: showMsdata = components.indexOf('ACQUISITION') > -1;
$: isLabelcheck = datasettypes.filter(x => x.id === dsinfo.datatype_id).filter(x => x.name.indexOf('abelcheck') > -1).length;
$: stored = $dataset_id && !edited;

async function getcomponents() {
  // Could get these in single shot, need not network trip, for all dtype IDs
  const result = await getJSON(`/datasets/show/components/${dsinfo.datatype_id}`);
  components = result.components;
}

function switchDatatype() {
  getcomponents();
  editMade();
}


function editMade() {
  edited = true;
  errors.basics = errors.basics.length ? validate() : [];
}

async function fetchDataset() {
  let url = `/datasets/show/info/${project_id}/`;
  url = $dataset_id ? url + $dataset_id + '/': url;
  const resp = await getJSON(url);
  for (let [key, val] of Object.entries(resp.dsinfo)) { dsinfo[key] = val; }
  for (let key in projsamples) { delete(projsamples[key]);};
  for (let [key, val] of Object.entries(resp.projdata.projsamples)) { projsamples[key] = val; }
  prefracs = resp.projdata.prefracs;
  experiments = resp.projdata.experiments;
  hirief_ranges = resp.projdata.hirief_ranges;
  if ($dataset_id) {
    getcomponents();
    isNewExperiment = false;
  }
  edited = false;
}

function validate() {
  comperrors = [];
  const re = RegExp('^[a-z0-9-_]+$', 'i');
  if (!dsinfo.runname) {
    comperrors.push('Run name is required');
  }
  else if (!re.test(dsinfo.runname)) {
    comperrors.push('Run name may only contain a-z 0-9 - _');
  }
  if (showMsdata && ((isNewExperiment && !dsinfo.newexperimentname) || (!isNewExperiment && !dsinfo.experiment_id))) {
    comperrors.push('Experiment is required');
  } else if (showMsdata && isNewExperiment && dsinfo.newexperimentname && !re.test(dsinfo.newexperimentname)) {
  comperrors.push('Experiment name may only contain a-z 0-9 - _');
  }
  if (isExternal && !dsinfo.externalcontactmail) {
    comperrors.push('External contact is required');
  }
  // This is probably not possible to save in UI, button is disabled
  if (!dsinfo.datatype_id) {
    comperrors.push('Datatype is required');
  }
  return comperrors;
}

async function save() {
  errors.basics = validate();
  if (showMsdata) { 
    let mserrors = mssubcomp.validate();
    errors.basics = [...errors.basics, ...mserrors];
  }
  if (errors.basics.length === 0) { 
    let postdata = {
      dataset_id: $dataset_id,
      datatype_id: dsinfo.datatype_id,
      runname: dsinfo.runname,
      prefrac_id: dsinfo.prefrac_id,
      prefrac_length: dsinfo.prefrac_length,
      prefrac_amount: dsinfo.prefrac_amount,
      hiriefrange: dsinfo.hiriefrange,
      project_id: project_id,
    };
    if (isNewExperiment) {
      postdata.newexperimentname = dsinfo.newexperimentname;
    } else {
      postdata.experiment_id = dsinfo.experiment_id;
    }
    if (isExternal) {
      postdata.externalcontact = dsinfo.externalcontactmail;
    }
    const response = await postJSON('/datasets/save/dataset/', postdata);
    if ('error' in response) {
      saveerrors.basics = [response.error, ...saveerrors.basics];
    } else {
      dataset_id.set(response.dataset_id);
      history.pushState({}, '', `/datasets/show/${response.dataset_id}/`);
      fetchDataset();
    }
  }
}

async function lockDataset() {
    let postdata = {dataset_id: $dataset_id};
    const response = await postJSON('/datasets/save/dataset/lock/', postdata);
    if ('error' in response) {
      saveerrors.basics = [response.error, ...saveerrors.basics];
    } else {
      dsinfo.locked = true;
    }
}


async function unlockDataset() {
    let postdata = {dataset_id: $dataset_id};
    const response = await postJSON('/datasets/save/dataset/unlock/', postdata);
    if ('error' in response) {
      saveerrors.basics = [response.error, ...saveerrors.basics];
    } else {
      dsinfo.locked = false;
    }
}


onMount(async() => {
  await fetchDataset();
})

function showMetadata() {
  tabshow = 'meta';
}

function showFiles() {
  tabshow = $dataset_id ? 'files' : tabshow;
}

</script>


<div class="container">
<ErrorNotif cssclass="sticky" errors={Object.values(saveerrors).flat().concat(Object.values(errors).flat())} />
<!--
{#if Object.values(errors).flat().length || Object.values(saveerrors).flat().length}
<div class="notification errorbox is-danger">
  <ul>
    {#each Object.values(saveerrors).flat() as error}
    <li>&bull; {error}</li>
    {/each}
    {#each Object.values(errors).flat() as error}
    <li>&bull; {error}</li>
    {/each}
  </ul>
</div>
{/if}
-->

<div class="tabs is-toggle is-centered is-small">
	<ul>
    <li class={tabshow === 'meta' ? 'is-active': ''}><a on:click={showMetadata}>
        <span>Metadata</span>
    </li>
    {#if $dataset_id}
    <li class={tabshow === 'files' ? 'is-active': ''}><a on:click={showFiles}>
        <span>Files</span>
    </li>
    {/if}
	</ul>
</div>

<h4 class="title is-4">
  {#if dsinfo.locked}
  <a title="Click to unlock dataset" on:click={unlockDataset}><i class="icon fas fa-lock has-text-grey"></i></a>
  {:else}
  <a title="Click to lock dataset" on:click={lockDataset}><i class="icon fas fa-lock-open has-text-grey"></i></a>
  {/if}
  {!$dataset_id ? 'New dataset' : `Dataset ${$dataset_id}`}
</h4> 
<div style="display: {tabshow !== 'meta' ? 'none' : ''}">
    <div class="box" id="project">
    
    	<article class="message is-info"> 
          <div class="message-body">
            <div>
              <span class="has-text-weight-bold">Project:</span>
              <span>{project_name}</span>
              <span class="tag is-success is-small">{ptype}</span>
            </div>
            <div><span class="has-text-weight-bold">PI:</span> {pi_name}</div>
            {#if dsinfo.storage_location}
            <div><span class="has-text-weight-bold">Storage: {dsinfo.storage_location}</div>
            {/if}
          </div>
    	</article>
    
      <h5 class="has-text-primary title is-5">
        {#if stored}
        <i class="icon fas fa-check-circle"></i>
        {:else}
        <i class="icon fas fa-edit"></i>
        {/if}
        Basics
        <button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
        <button class="button is-small is-info has-text-weight-bold" disabled={!edited || !dsinfo.datatype_id} on:click={fetchDataset}>Revert</button>
      </h5>
    

      {#if isExternal}
      <div class="field">
        <label class="label">contact(s)
        <div class="control">
          <input class="input" type="text" on:change={editMade} bind:value={dsinfo.externalcontactmail} placeholder="operational contact email (e.g. postdoc)">
        </div>
      </div>
      {/if}
    
      <div class="field">
        <label class="label">Dataset type</label>
        <div class="control">
          <div class="select">
            <select bind:value={dsinfo.datatype_id} on:change={switchDatatype}>
              <option disabled value="">Please select one</option>
              {#each datasettypes as dstype}
              <option value={dstype.id}>{dstype.name}</option>
              {/each}
            </select>
          </div>
        </div>
      </div>

      {#if !isLabelcheck}
      <div class="field">
        <label class="label">Experiment name
          <a class="button is-danger is-outlined is-small" on:click={e => isNewExperiment = isNewExperiment === false}>
          {#if isNewExperiment}
          Existing experiment
          {:else}
          Create new experiment
          {/if}
          </a>
        </label>
        <div class="control">
          {#if isNewExperiment}
          <input class="input" bind:value={dsinfo.newexperimentname} on:change={editMade} type="text" placeholder="Experiment name">
          {:else}
          <div class="select">
            <select bind:value={dsinfo.experiment_id} on:change={editMade}>
              <option disabled value="">Please select one</option>
              {#each experiments as exp}
              <option value={exp.id}>{exp.name}</option>
              {/each}
            </select>
          </div>
          {/if}
        </div>
      </div>
      {/if}
  

      {#if showMsdata}
      <!-- storage location depends on prefractionation, so put it here -->
      <Msdata bind:this={mssubcomp} on:edited={editMade} bind:dsinfo={dsinfo} prefracs={prefracs} hirief_ranges={hirief_ranges} />
      {/if}

      {#if showMsdata || dsinfo.datatype_id}
      <div class="field">
        <label class="label">Run name</label>
        <div class="control">
          <input class="input" bind:value={dsinfo.runname} on:change={editMade} type="text" placeholder="E.g set1, lc3, rerun5b, etc">
        </div>
      </div>
      {/if}

      <button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
      <button class="button is-small is-info has-text-weight-bold" disabled={!edited || !dsinfo.datatype_id} on:click={fetchDataset}>Revert</button>

      <hr>

      {#if showMsdata}
      <!-- acquisition and MS data -->
      <MSSamplePrepComp bind:this={mssampleprepcomp} bind:errors={errors.mssampleprep} />
      <hr>
      <MSAcqComp bind:this={msacqcomp} bind:errors={errors.msacq} />
      <hr>
      {/if}

      {#if (Object.keys($datasetFiles).length && components.indexOf('LCSAMPLES')>-1)}
      <LCheck bind:this={lccomp} bind:errors={errors.lc} />
      {:else if (Object.keys($datasetFiles).length && components.indexOf('POOLEDLCSAMPLES')>-1)}
      <PooledLCheck bind:this={pooledlc} bind:errors={errors.lc} />
      {:else}
      <Samplesheet bind:this={samplesheet} bind:errors={errors.samples} />
      {/if}
    </div>
</div>

<div style="display: {tabshow !== 'files' ? 'none' : ''}">
    <Files bind:this={filescomp} />
</div>

</div>
