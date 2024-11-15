<script>
import { getJSON, postJSON } from './funcJSON.js'
import { dataset_id, datatype_id, datasetFiles, projsamples } from './stores.js';
import { onMount } from 'svelte';
import MSDataComp from './MScomp.svelte';
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
let msdatacomp;
let samplesheet;
let lccomp;
let pooledlc;
let filescomp;
let edited = false;
let errors = {
  basics: [],
  samples: [],
  msdata: [],
  lc: [],
};
let saveerrors = Object.assign({}, errors);
let comperrors = [];


let dsinfo = {
  datatype_id: '',
  old_project_id: '',
  project_id: '',
  project_name: '',
  ptype_id: '',
  storage_location: '',
  oldnewprojectname: '',
  newprojectname: '',
  experiment_id: '',
  runname: '',
  pi_id: '',
  newpiname: '',
  pi_name: '',
  externalcontactmail: '',
  prefrac_id: '',
  prefrac_length: '',
  prefrac_amount: '',
  hiriefrange: '',
}

let datasettypes = [];
let ptypes = [];
let projects = {};
let local_ptype_id = '';
let external_pis = [];
let prefracs = [];
let hirief_ranges = [];
let internal_pi_id = '';


let components = [];
let isNewProject = false;
let isExternal = false;
let isNewExperiment = false;
let isNewPI = false;
let experiments = []
let tabshow = 'meta';
let tabcolor = 'has-text-grey-lighter';
  // Yes, for microscopy/genomics, we need separation between samples/prep
  // files is given, and possibly samples as well, check it out but samples is needed for:
  // - QMS, LCheck, IP, TPP, microscopy QC?, genomics

$: showMsdata = components.indexOf('ACQUISITION') > -1;
$: isExternal = Boolean(dsinfo.ptype_id && dsinfo.ptype_id !== local_ptype_id);
$: isLabelcheck = datasettypes.filter(x => x.id === dsinfo.datatype_id).filter(x => x.name.indexOf('abelcheck') > -1).length;
$: stored = $dataset_id && !edited;

async function getcomponents() {
  // FIXME can get these in single shot? Need not network trip.
  const result = await getJSON(`/datasets/show/components/${dsinfo.datatype_id}`);
  components = result.components;
}

function switchDatatype() {
  getcomponents();
  editMade();
}

async function project_selected(saved=false) {
  // Gets experiments, project details when selecting a project
  if (dsinfo.project_id) {
    const result = await getJSON(`/datasets/show/project/${dsinfo.project_id}`);
    dsinfo.pi_name = external_pis[result.pi_id].name;
    dsinfo.ptype_id = result.ptype_id;
    experiments = result.experiments;
    for (let key in projsamples) { delete(projsamples[key]);};
    for (let [key, val] of Object.entries(result.projsamples)) { projsamples[key] = val; }
    isNewProject = false;
  }
  if (!saved) {
    dsinfo.experiment_id = '';
  }
  editMade();
}

function setNewProj() {
  isNewProject = true;
  dsinfo.ptype_id = '';
  dsinfo.externalcontactmail = '';
  experiments = [];
  for (let key in projsamples) { delete(projsamples[key]);};

  editMade();
}

function editMade() {
  edited = true;
  errors.basics = errors.basics.length ? validate() : [];
}

async function fetchDataset() {
  let url = '/datasets/show/info/';
  url = $dataset_id ? url + $dataset_id : url;
  const resp = await getJSON(url);
  for (let [key, val] of Object.entries(resp.dsinfo)) { dsinfo[key] = val; }
  datasettypes = resp.projdata.datasettypes;
  ptypes = resp.projdata.ptypes;
  projects = resp.projdata.projects;
  local_ptype_id = resp.projdata.local_ptype_id;
  external_pis = resp.projdata.external_pis;
  internal_pi_id = resp.projdata.internal_pi_id;
  prefracs = resp.projdata.prefracs;
  hirief_ranges = resp.projdata.hirief_ranges;
  if ($dataset_id) {
    getcomponents();
    await project_selected(true); // true is saved param
    isNewExperiment = false;
  }
  edited = false;
}

function validate() {
	comperrors = [];
	const re = RegExp('^[a-z0-9-_]+$', 'i');
	if ((isNewProject && !dsinfo.newprojectname) || (!isNewProject && !dsinfo.project_id)) {
		comperrors.push('Project needs to be selected or created');
	}
	else if (isNewProject && dsinfo.newprojectname && !re.test(dsinfo.newprojectname)) {
		comperrors.push('Project name may only contain a-z 0-9 - _');
	}
  if (!dsinfo.ptype_id) {
    comperrors.push('Project type selection is required');
  }
	if (!dsinfo.runname) {
		comperrors.push('Run name is required');
	}
	else if (!re.test(dsinfo.runname)) {
		comperrors.push('Run name may only contain a-z 0-9 - _');
	}
  if (showMsdata && ((isNewExperiment && !dsinfo.newexperimentname) || (!isNewExperiment && !dsinfo.experiment_id))) {
		comperrors.push('Experiment is required');
	}
	else if (showMsdata && isNewExperiment && dsinfo.newexperimentname && !re.test(dsinfo.newexperimentname)) {
		comperrors.push('Experiment name may only contain a-z 0-9 - _');
	}
  if (isExternal) {
		if (isNewProject && !dsinfo.newpiname && (!dsinfo.pi_id || dsinfo.pi_id === internal_pi_id)) {
			comperrors.push('Need to select or create a PI');
		}
		if (!dsinfo.externalcontactmail) {
			comperrors.push('External contact is required');
		}
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
      ptype_id: dsinfo.ptype_id,
      datatype_id: dsinfo.datatype_id,
      runname: dsinfo.runname,
      prefrac_id: dsinfo.prefrac_id,
      prefrac_length: dsinfo.prefrac_length,
      prefrac_amount: dsinfo.prefrac_amount,
      hiriefrange: dsinfo.hiriefrange,
    };
    if (isNewProject) {
      postdata.newprojectname = dsinfo.newprojectname;
    } else {
      postdata.project_id = dsinfo.project_id;
    }
    if (isNewExperiment) {
      postdata.newexperimentname = dsinfo.newexperimentname;
    } else {
      postdata.experiment_id = dsinfo.experiment_id;
    }
    if (dsinfo.newpiname) {
      postdata.newpiname = dsinfo.newpiname;
    } else {
      postdata.pi_id = isExternal ? dsinfo.pi_id : internal_pi_id;
    }
    if (dsinfo.ptype_id !== local_ptype_id) {
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

<h4 class="title is-4">{!$dataset_id ? 'New dataset' : `Dataset ${$dataset_id}`}</h4> 
<div style="display: {tabshow !== 'meta' ? 'none' : ''}">
    <div class="box" id="project">
    
      {#if dsinfo.storage_location}
    	<article class="message is-info"> 
        <div class="message-header">Storage location</div>
        <div class="message-body">{dsinfo.storage_location}</div>
    	</article>
      {/if}
    
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
    
      <div class="field"> 
        <label class="label">Project
        {#if isNewProject}
        <span class="tag is-danger is-outlined is-small">New project</span>
        {#if $dataset_id}
        <span class="tag is-danger is-outlined is-small">Are you sure?</span>
        {/if}
        {/if}
        </label>
        <div class="control">
          <DynamicSelect bind:intext={dsinfo.project_name} fixedoptions={projects} bind:unknowninput={dsinfo.newprojectname} bind:selectval={dsinfo.project_id} on:selectedvalue={e => project_selected()} on:newvalue={setNewProj} niceName={x => x.name}/>
        {#if isNewProject}
        <label class="label">Project type</label>
        <div class="select">
          <select bind:value={dsinfo.ptype_id}>
            <option disabled value="">Please select one</option>
            {#each ptypes as ptype}
            <option value={ptype.id}>{ptype.name}</option>
            {/each}
          </select>
        </div>
        {/if}

        {#if isExternal}
        <span class="tag is-success is-medium">External project: {dsinfo.pi_name}</span>
        {/if}
        </div>
      </div>

      {#if isExternal}
      <div class="field">
        <label class="label">contact(s)
        {#if dsinfo.newpiname}
        <span class="tag is-danger is-outlined is-small">New PI</span>
        {/if}
        <div class="control">
          {#if isNewProject}
          <DynamicSelect bind:intext={dsinfo.pi_name} fixedoptions={external_pis} bind:unknowninput={dsinfo.newpiname} bind:selectval={dsinfo.pi_id} niceName={x => x.name} />
          {/if}
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
      <MSDataComp bind:this={msdatacomp} bind:errors={errors.msdata} />
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
