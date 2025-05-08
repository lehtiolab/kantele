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
let project_id = initdata.proj_id;
let project_name = initdata.name;
let ptype = initdata.ptype;
let pi_name = initdata.pi_name;
let securityclass = initdata.secclass;
let isExternal = initdata.isExternal;
let datasettypes = initdata.datasettypes;
let allprojects = initdata.all_projects;
let allstorlocs = initdata.all_storlocs;
let allsecclasses = initdata.securityclasses;
let allproj_order = Object.keys(initdata.all_projects);

// Danger zone binds
let newProjId;
let newProjName;
let newPtype;
let newPiName;
let newExperiments;
let newClassification;


let dsinfo = {
  datatype_id: '',
  storage_locations: {},
  experiment_id: '',
  newexperimentname: '',
  newexperimentname: '',
  runname: '',
  externalcontactmail: '',
  prefrac_id: '',
  prefrac_length: '',
  prefrac_amount: '',
  hiriefrange: '',
}

// For danger zone selection
let showDangerZone;
let safeWord;

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

async function project_selected() {
  // Gets experiments, project details when selecting a project
  if (newProjId) {
    const result = await getJSON(`/datasets/show/project/${newProjId}/`);
    newPiName = result.pi_name;
    newProjId = result.id;
    newPtype = result.ptype_name;
    newExperiments = result.experiments;
    newProjName= result.name;
    for (let key in projsamples) { delete(projsamples[key]);};
    for (let [key, val] of Object.entries(result.projsamples)) { projsamples[key] = val; }
  }
}


function cancelDangerZoneEdit() {
  newProjId = '';
  newPiName = '';
  newPiName = '';
  newProjName = '';
  newExperiments = [];
  safeWord = '';
  showDangerZone = false;
}


function confirmDangerZoneEdit() {
  if (safeWord === 'kantele') {
    if (newProjId) {
      pi_name = newPiName;
      project_id = newProjId;
      ptype = newPiName;
      project_name = newProjName;
      experiments = newExperiments;
      dsinfo.experiment_id = '';
      editMade();
    }
    if (newClassification !== securityclass) {
      securityclass = newClassification;
      editMade();
    }
    cancelDangerZoneEdit();
  }
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
      project_id: '',
      secclass: securityclass,
      runname: dsinfo.runname,
      prefrac_id: dsinfo.prefrac_id,
      prefrac_length: dsinfo.prefrac_length,
      prefrac_amount: dsinfo.prefrac_amount,
      hiriefrange: dsinfo.hiriefrange,
      project_id: project_id,
      storage_servers: dsinfo.storage_servers,
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
    } else if ('msg' in response) {
      saveerrors.basics = [response.msg, ...saveerrors.basics];
      dsinfo.locked = false;
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
  <i class="icon fas fa-lock has-text-success"></i>
  {:else}
  <i class="icon fas fa-lock-open has-text-grey"></i>
  {/if}
  {!$dataset_id ? 'New dataset' : `Dataset ${$dataset_id}`}
</h4> 
<div style="display: {tabshow !== 'meta' ? 'none' : ''}">
    <div class="box" id="project">
    
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
    
    	<article class="message is-info"> 
          <div class="message-body">
            {#if $dataset_id && !dsinfo.locked}
            <label class="label">Only locked datasets can be used in analysis</label>
            <span class="has-text-danger">Locked datasets cannot be changed or receive files!</span>
            <button class="button is-small" on:click={lockDataset}>Lock dataset</button>
            {/if}
            <div>
              <span class="has-text-weight-bold">Project:</span>
              <span>{project_name}</span>
              <span class="tag is-success is-small">{ptype}</span>
            </div>
            <div><span class="has-text-weight-bold">PI:</span> {pi_name}</div>
            <div>
              <span class="has-text-weight-bold">Classification:</span>
              <span class="tag is-danger is-small">{allsecclasses.filter(x => x.id === securityclass)[0].name}</span>
            </div>

            <div class="has-text-weight-bold">Stored at:</div>
            {#each Object.values(allstorlocs) as loc}
              <div class={`tag is-medium ${loc.id in dsinfo.storage_locations ? 'is-success' : ''}`}>
                  <label class="checkbox">
                    <input value={loc.id} bind:group={dsinfo.storage_servers} on:change={editMade} type="checkbox" />
                    {loc.name}
                </label>
              </div>
            {/each}

            {#if !showDangerZone}
            <div class="field">
              <button on:click={e => showDangerZone = true} class="button is-small">Edit</button>
            </div>
            {:else}
              {#if $dataset_id && dsinfo.locked}
              <div class="field">
              <button class="button is-small mt-4" on:click={unlockDataset}>Unlock dataset</button>
              </div>
              {/if}
            <h4 class="title is-5 mt-4">Danger zone</h4>
            <div class="field">
              <label class="label">Change project</label>
              <DynamicSelect placeholder='Find project' bind:selectval={newProjId} on:selectedvalue={project_selected} niceName={x => x.name} fixedoptions={allprojects} fixedorder={allproj_order} />
            </div>
            <div class="field">
              <label class="label">Change classification</label>
              <div class="select">
                <select bind:value={newClassification}>
                  {#each allsecclasses as secclass}
                  <option value={secclass.id}>{secclass.name}</option>
                  {/each}
                </select>
              </div>
            </div>
            <div class="field">
              <label class="label">Type "kantele" in the box below to confirm</label>
              <input class="input" bind:value={safeWord} type="text" />
            </div>
            <button on:click={confirmDangerZoneEdit} class="button is-small has-text-danger">Confirm changes</button>
            <button on:click={cancelDangerZoneEdit} class="button is-small">Cancel</button>
            {/if}
          </div>
    	</article>
    

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
