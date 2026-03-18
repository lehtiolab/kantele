<script>
import { onMount } from 'svelte';
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import DetailBox from './DetailBox.svelte'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte'

export let closeWindow;
export let projId;

let notif = {errors: {}, messages: {}};
let proj;
let newname = false;

let newproj_piname
let newpiname = '';
let newProjIsExternal = false;


// If user clicks proj, show that instead, run when projIds is updated:
$: {
  cleanFetchDetails(projId);
}

async function updateProject() {
  const resp = await postJSON('/datasets/update/project/', {
    newname: newname, pi_id: proj.pi_id, newpiname: newpiname,
    ptype_id: proj.ptype_id, extref: proj.extref, projid: proj.id});
  if (!resp.ok) {
    const msg = `Something went wrong trying to rename the project: ${resp.error}`;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    const msg = 'Updated project, renaming will have queued dataset move';
    notif.messages[msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
    // FIXME Should fix update in table as well
  }
}

async function fetchDetails(id) {
  let fetched = {}
  const resp = await getJSON(`/show/project/${id}`);
  if (!resp.ok) {
    const msg = `Something went wrong fetching project info: ${resp.error}`;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    proj = resp;
    newname = resp.name;
  }
  console.log(proj);
}

function cleanFetchDetails(ids) {
  fetchDetails(ids[0]);
}

onMount(async() => {
  cleanFetchDetails(projId);
});

</script>

<DetailBox notif={notif} closeWindow={closeWindow}>
  {#if proj}
  <p><span class="has-text-weight-bold">Type: </span>{proj.type}</p>
  <p><span class="has-text-weight-bold">Registered: </span>{proj.regdate}</p>
  <p><span class="has-text-weight-bold">Owners: </span>{proj.owners.join(', ')}</p>
  <p><span class="has-text-weight-bold"># datasets: </span>{proj.nrdsets}</p>
  <p><span class="has-text-weight-bold">Stored amount: </span>{proj.stored_total_xbytes}</p>
  <p><span class="has-text-weight-bold"># files: </span>{Object.entries(proj.nrstoredfiles).map(x => `${x[1]} files of type ${x[0]}`).join('; ')}</p>
  <p><span class="has-text-weight-bold">Instruments used: </span>{proj.instruments.join(', ')}</p>

  <hr>
  
  <h5 class="title is-5">Update project
      <button on:click={updateProject} class="button is-small is-danger">Update</button>
  </h5>
  <div class="field is-grouped">
    <p class="control is-expanded">
      <input class="input" bind:value={newname} type="text"> 
    </p>
  </div>

  <div class="field">
    <label class="label">Project type</label>
    <div class="select">
      <select bind:value={proj.ptype_id}>
      {#each ptypes as {id, name}}
        <option value={id}>{name}</option>
      {/each}
      </select>
    </div>
  </div>

  {#if proj.ptype_id !== local_ptype_id}
  <div class="field is-grouped">
    <label class="label">External PI
    {#if newpiname}
    <span class="tag is-danger is-outlined is-small">New PI: {newpiname}</span>
    {/if}
    <p class="control">
      <DynamicSelect bind:intext={newproj_piname} fixedoptions={external_pis} bind:unknowninput={newpiname} bind:selectval={proj.pi_id} niceName={x => x.name} />
    </p>
  </div>
  {/if}

  <div class="field">
    <label class="label">External reference</label>
    <input class="input" type="text" bind:value={proj.extref} placeholder="Optional: reference to external system (e.g. iLab)">
  </div>
  {/if}
</DetailBox>
