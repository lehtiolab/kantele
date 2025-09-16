<script>

import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import Table from './Table.svelte'
import Tabs from './Tabs.svelte'
import Details from './ProjectDetails.svelte'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte'
import { flashtime } from '../../util.js'
import { closeProject, treatItems } from './util.js'

const inactive = ['inactive'];
let table;

let selectedProjs = []
let notif = {errors: {}, messages: {}};
let detailsVisible = false;
let addItem;
let purgeConfirm;
let creatingNewProject = false;

let newproj_name;
let newproj_ptype_id = local_ptype_id;
let newproj_piname;
let newproj_newpiname = '';
let newproj_pi_id;
let newproj_extref;
let projects;
$: newProjIsExternal = Boolean(newproj_ptype_id !== local_ptype_id);


const tablefields = [
  {id: 'name', name: 'Name', type: 'str', multi: false},
//  {id: 'storestate', name: 'Stored', type: 'tag', multi: false, links: 'fn_ids', linkroute: '#/files/'},
//  {id: 'jobstates', name: '__hourglass-half', type: 'state', multi: true, links: 'jobids', linkroute: '#/jobs'},
  {id: 'datasets', name: '', help: 'Datasets', type: 'icon', icon: 'clipboard-list', multi: false, links: 'dset_ids', linkroute: '#/datasets'},
  {id: 'ptype', name: 'Type', type: 'str', multi: false},
  {id: 'start', name: 'Registered', type: 'str', multi: false},
  {id: 'lastactive', name: 'Last active', type: 'str', multi: false},
  {id: 'actions', name: '', type: 'button', multi: true, confirm: ['close']},
];

const fixedbuttons = [
]


export function newDataset(projid, _projs, _notif) {
  window.open(`/datasets/new/${projid}/`, '_blank');
} 


const actionmap = {
  'new dataset': newDataset,
  'close': closeProject,
}
async function doAction(action, projid) {
  await actionmap[action](projid, projects, notif);
  // Update notifs
  projects = projects;
  updateNotif();
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


function updateNotif() {
  Object.entries(notif.errors)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg));
  Object.entries(notif.messages)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg));
  notif = notif;
}


async function archiveProject() {
  for (let projid of selectedProjs) {
    await closeProject(projid, projects, notif);
  }
  updateNotif();
  projects = projects;
}

//function reactivateProject() {
//  treatItems('datasets/undelete/project/', 'project','reactivating', projid, notif);
//}

async function purgeProject() {
  for (let projid of selectedProjs) {
    await treatItems('datasets/purge/project/', 'project', 'purging', projid, notif);
  }
  updateNotif();
  projects = projects;
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
    projects[fnid].filename = newname;
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

<Table tab="Projects"
  bind:items={projects}
  bind:addItem={addItem}
  bind:notif={notif}
  bind:selected={selectedProjs}
  fetchUrl="/show/projects"
  findUrl="/show/projects"
  defaultQ="active:true"
  show_deleted_or_q="type:cf, type:local, from:2025, to:20250701, active:true, active:false"
  getdetails={getProjDetails}
  fixedbuttons={fixedbuttons}
  fields={tablefields}
  inactive={inactive}
  on:detailview={showDetails}
  allowedActions={Object.keys(actionmap)}
  on:rowAction={e => doAction(e.detail.action, e.detail.id)}
  />

{#if detailsVisible}
<Details closeWindow={() => {detailsVisible = false}} projId={detailsVisible} />
{/if}
