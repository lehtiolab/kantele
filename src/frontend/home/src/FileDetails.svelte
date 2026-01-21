<script>
import { createEventDispatcher } from 'svelte';
import { onMount } from 'svelte';
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import DetailBox from './DetailBox.svelte'

export let closeWindow;
export let fnIds;

const dispatch = createEventDispatcher();

let notif = {errors: {}, messages: {}};
let items = {};
let newname = Object.fromEntries(fnIds.map(x => [x, false]));
let new_storage_shares = {};

// If user clicks new file, show that instead, run when fnIds is updated:
$: {
  cleanFetchDetails(fnIds);
}


function updateNotif() {
  // This does not work if we import it from e.g. util.js, svelte wont update the components
  // showing the notifications
  Object.entries(notif.errors)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg));
  Object.entries(notif.messages)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg));
  notif = notif;
}


async function renameFile(newname, fnid) {
  if (newname !== items[fnid].filename) {
    const resp = await postJSON('/files/rename/', {
      newname: newname,
      sf_id: fnid});
    if (!resp.ok) {
      const msg = `Something went wrong trying to rename the file: ${resp.error}`;
      notif.errors[msg] = 1;
      updateNotif();
    } else {
      items[fnid].filename = newname;
      const msg = `Queued file for renaming to ${newname}`;
      notif.messages[msg] = 1;
      updateNotif();
    }
  }
}


async function updateFileDescription(desc, fnid) {
  if (desc !== items[fnid].desc) {
    const resp = await postJSON('/files/description/', {
      desc: desc,
      sf_id: fnid});
    if (!resp.ok) {
      const msg = `Something went wrong trying to rename the file: ${resp.error}`;
      notif.errors[msg] = 1;
      updateNotif();
    } else {
      items[fnid].desc = desc;
      notif.messages[resp.msg] = 1;
      updateNotif();
    }
  }
}


// This function seems general, but I'm not sure, you could put file specific stuff in it
// maybe with a callback
async function fetchDetails(ids) {
  let fetched = {}
  let fetchedStorages = {}
  const tasks = ids.map(async singleId => {
    const resp = await getJSON(`/show/file/${singleId}`);
    if (!resp.ok) {
      const msg = `Something went wrong fetching file info: ${resp.error}`;
      notif.errors[msg] = 1;
      updateNotif();
    } else {
      fetched[singleId] = resp;
      newname[singleId] = resp.filename;
      fetchedStorages[singleId] = resp.storage_shares;
    }
  });
  const result = await Promise.all(tasks);
  items = Object.assign(items, fetched);
  new_storage_shares = Object.assign({}, fetchedStorages);
}

function cleanFetchDetails(ids) {
  items = {};
  fetchDetails(ids);
}

async function deleteFile(fnid, force) {
  const resp = await postJSON('files/delete/', {item_id: fnid, force: force});
  if (!resp.ok) {
    if (resp.status == 402) {
      const msg = resp.error;
      notif.messages[msg] = 1;
      updateNotif();
      items[fnid].ask_force_delete = true;
    } else {
      const msg = `Something went wrong deleting file id ${fnid}: ${resp.error}`;
      notif.errors[msg] = 1;
      updateNotif();
    }
  } else {
    const msg = `Deleting file with id ${fnid} queued`;
    notif.messages[msg] = 1;
    updateNotif();
    dispatch('refresh', {fnid: fnid});
    cleanFetchDetails(fnIds);
  }
}

async function updateStorage(fnid) {
  const resp = await postJSON('files/storage/', {item_id: fnid,
    share_ids: new_storage_shares[fnid]});
  if (!resp.ok) {
    const msg = `Something went wrong updating storage for file id ${fnid}: ${resp.error}`;
    notif.errors[msg] = 1;
    updateNotif();
  } else {
    const msg = resp.msg;
    notif.messages[msg] = 1;
    updateNotif();
    dispatch('refresh', {fnid: fnid});
    cleanFetchDetails(fnIds);
  }
}

onMount(async() => {
  cleanFetchDetails(fnIds);
});

</script>

<DetailBox notif={notif} closeWindow={closeWindow}>


  {#each Object.entries(items) as [fnid, fn]}

  {#each Object.values(fn.all_storlocs) as loc}
    <div class={`tag is-medium ${fn.storage_shares.indexOf(loc.id) > -1 ? 'is-success' : ''}`}>
        <label class="checkbox">
          <input value={loc.id} bind:group={new_storage_shares[fnid]} type="checkbox" />
          {loc.name}
      </label>
    </div>
  {/each}

  <p><span class="has-text-weight-bold">Producer</span> {fn.producer}</p>
  <div class="has-text-weight-bold">Storage locations:</div>
  {#each fn.servers as [server, path]}
  <div>
    <span class="has-text-primary">{server}</span> / {path}
  </div>
  {/each}

  {#if fn.description}
  <div class="field is-grouped">
    <p class="control is-expanded">
      <input class="input is-small" bind:value={fn.description} type="text"> 
    </p>
    <p class="control">
      <a on:click={updateFileDescription(fn.description, fnid)} class="button is-small is-primary">Update</a>
    </p>
  </div>
  {/if}
  <div class="field is-grouped">
    <p class="control is-expanded">
      <input class="input is-small" bind:value={newname[fnid]} type="text"> 
    </p>
    <p class="control">
      <a on:click={renameFile(newname[fnid], fnid)} class="button is-small is-primary">Rename file</a>
    </p>
  </div>
  {#if fn.deleted}
      <button class="button is-small is-danger" disabled>Delete file</button>
      {#if fn.restorable}
        <button on:click={e => updateStorage(fnid)} class="button is-small is-primary">Update storage</button>
      {:else}
        <button class="button is-small is-primary" disabled>Cannot restore</button>
      {/if}
  {:else}
      {#if fn.ask_force_delete}
        <button on:click={e => deleteFile(fnid, true)} class="button is-small is-danger">Are you sure?</button>
      {:else}
        <button on:click={e => deleteFile(fnid, false)} class="button is-small is-danger">Delete file</button>
      {/if}
      <button on:click={e => updateStorage(fnid)} class="button is-small is-primary">Update storage</button>
  {/if}
  {/each}
</DetailBox>
