<script>

import {querystring, push} from 'svelte-spa-router';
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import Details from './ProjectDetails.svelte'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte'
import { flashtime } from '../../util.js'

const inactive = ['inactive'];
let table;

let selectedProjs = []
let notif = {errors: {}, messages: {}};
let detailsVisible = false;
let treatItems;
let addItem;
let purgeConfirm;
let creatingNewProject = false;

let newproj_name;
let newproj_ptype_id = local_ptype_id;
let newproj_piname;
let newproj_newpiname = '';
let newproj_pi_id;
let newproj_extref;
$: newProjIsExternal = Boolean(newproj_ptype_id !== local_ptype_id);


const tablefields = [
  {id: 'name', name: 'Name', type: 'str', multi: false},
//  {id: 'storestate', name: 'Stored', type: 'tag', multi: false, links: 'fn_ids', linkroute: '#/files/'},
//  {id: 'jobstates', name: '__hourglass-half', type: 'state', multi: true, links: 'jobids', linkroute: '#/jobs'},
  {id: 'datasets', name: '', help: 'Datasets', type: 'icon', icon: 'clipboard-list', multi: false, links: 'dset_ids', linkroute: '#/datasets'},
  {id: 'ptype', name: 'Type', type: 'str', multi: false},
  {id: 'start', name: 'Registered', type: 'str', multi: false},
  {id: 'lastactive', name: 'Last active', type: 'str', multi: false},
  {id: 'actions', name: '', type: 'button', multi: true},
];

const fixedbuttons = [
]


function newDataset(projid) {
  window.open(`/datasets/new/${projid}/`, '_blank');
} 

function doAction(action, projid) {
  const actionmap = {
    'new dataset': newDataset,
  }
  actionmap[action](projid);
}

function showDetails(event) {
  detailsVisible = event.detail.ids;
}


function setConfirm() {
  purgeConfirm = true;
  setTimeout(() => { purgeConfirm = false} , flashtime);
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

function archiveProject() {
  const callback = (proj) => {proj.inactive = true; }
  treatItems('datasets/archive/project/', 'project', 'archiving', callback, selectedProjs);
}

function reactivateProject() {
  const callback = (proj) => {proj.inactive = false; }
  treatItems('datasets/undelete/project/', 'project','reactivating', callback, selectedProjs);
}

function purgeProject() {
  const callback = (proj) => {proj.inactive = true; }
  treatItems('datasets/purge/project/', 'project', 'purging', callback, selectedProjs);
}

function cancelNewProject() {
  creatingNewProject = false;
  newproj_name = '';
  newproj_ptype_id = local_ptype_id;
  newproj_piname = '';
  newproj_newpiname = '';
  newproj_pi_id = false;
  newproj_extref = false;
}

async function saveProject() {
  let postdata = {
    name: newproj_name,
    pi_id: newproj_pi_id,
    ptype_id: newproj_ptype_id,
    extref: newproj_extref,
  };
  if (newProjIsExternal && newproj_newpiname) {
    postdata.newpiname = newproj_newpiname;
  }
  const resp = await postJSON('datasets/save/project/', postdata);
  if (!resp.ok) {
    const msg = resp.error;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    addItem(resp.project)
    cancelNewProject();
  }

}

async function mergeProjects() {
  const resp = await postJSON('datasets/merge/projects/', {
    projids: selectedProjs,
  });
  if (!resp.ok) {
    const msg = `Something went wrong trying to rename the file: ${resp.error}`;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    items[fnid].filename = newname;
    const msg = `Queued file for renaming to ${newname}`;
    notif.messages[msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
  }
}

</script>

<Tabs tabshow="Projects" notif={notif} />

<a class="button is-small" title="New project" on:click={e => creatingNewProject = true}>New project</a>
{#if selectedProjs.length}
<a class="button is-small" title="Move projects to cold storage (delete)" on:click={archiveProject}>Close project(s)</a>
<a class="button is-small" title="Move projects to active storage (undelete)" on:click={reactivateProject}>Reopen project(s)</a>
  {#if purgeConfirm}
  <a class="button is-small is-danger is-light" title="PERMANENTLY delete projects from active and cold storage" on:click={purgeProject}>Are you sure? Purge projects</a>
  {:else}
  <a class="button is-small" title="PERMANENTLY delete projects from active and cold storage" on:click={setConfirm}>Purge projects</a>
  {/if}
  {#if selectedProjs.length>1}
  <a class="button is-small" title="Merge projects to sinlge (earliest) project" on:click={mergeProjects}>Merge projects</a>
  {:else}
  <a class="button is-small" title="Merge projects to single (earliest) project" disabled>Merge projects</a>
  {/if}
{:else}
<a class="button is-small" title="Move projects to cold storage (delete)" disabled>Close project(s)</a>
<a class="button is-small" title="Move projects to active storage (undelete)" disabled>Reopen project(s)</a>
<a class="button is-small" title="PERMANENTLY delete projects from active and cold storage" disabled>Purge projects</a>
<a class="button is-small" title="Merge projects to single (earliest) project" disabled>Merge projects</a>
{/if}

{#if creatingNewProject}
<div class="box">
  <h5 class="title is-5">New project
  <button on:click={cancelNewProject} class="button is-small is-danger">Cancel</button>
  </h5>
  <div class="field">
  <input class="input" bind:value={newproj_name} type="text" placeholder="Project name"> 
  </div>

  <div class="field">
    <label class="label">Project type</label>
    <div class="select">
      <select bind:value={newproj_ptype_id}>
      {#each ptypes as {id, name}}
        <option value={id}>{name}</option>
      {/each}
      </select>
    </div>
  </div>

  <div class="field">
    <label class="label">External reference</label>
    <input class="input" type="text" bind:value={newproj_extref} placeholder="Optional: reference to external system (e.g. iLab)">
  </div>

  {#if newProjIsExternal}
  <div class="field">
    <label class="label">External PI
    {#if newproj_newpiname}
    <span class="tag is-danger is-outlined is-small">New PI: {newproj_newpiname}</span>
    {/if}
    <div class="control">
      <DynamicSelect bind:intext={newproj_piname} fixedoptions={external_pis} bind:unknowninput={newproj_newpiname} bind:selectval={newproj_pi_id} niceName={x => x.name} />
    </div>
  </div>
  {/if}

  <button class="button is-success" on:click={saveProject}>Save</button>
</div>
{/if}

<Table tab="Projects" bind:addItem={addItem}
  bind:treatItems={treatItems}
  bind:notif={notif}
  bind:selected={selectedProjs}
  fetchUrl="/show/projects"
  findUrl="/find/projects"
  getdetails={getProjDetails}
  fixedbuttons={fixedbuttons}
  fields={tablefields}
  inactive={inactive}
  on:detailview={showDetails}
  on:rowAction={e => doAction(e.detail.action, e.detail.id)}
  />

{#if detailsVisible}
<Details closeWindow={() => {detailsVisible = false}} projId={detailsVisible} />
{/if}
