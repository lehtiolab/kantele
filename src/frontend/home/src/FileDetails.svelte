<script>
import { onMount } from 'svelte';
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import DetailBox from './DetailBox.svelte'

export let closeWindow;
export let fnIds;

let notif = {errors: {}, messages: {}};
let items = {};
let newname = Object.fromEntries(fnIds.map(x => [x, false]));

// If user clicks new file, show that instead, run when dsetIds is updated:
$: {
  cleanFetchDetails(fnIds);
}

async function renameFile(newname, fnid) {
  if (newname !== items[fnid].filename) {
    const resp = await postJSON('/files/rename/', {
      newname: newname,
      sf_id: fnid});
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
}

// This function seems general, but I'm not sure, you could put file specific stuff in it
// maybe with a callback
async function fetchDetails(ids) {
  let fetched = {}
  const tasks = ids.map(async singleId => {
    const resp = await getJSON(`/show/file/${singleId}`);
    if (!resp.ok) {
      const msg = `Something went wrong fetching file info: ${resp.error}`;
      notif.errors[msg] = 1;
      setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
    } else {
      fetched[singleId] = resp;
      newname[singleId] = resp.filename;
    }
  });
  const result = await Promise.all(tasks);
  items = Object.assign(items, fetched);
}

function cleanFetchDetails(ids) {
  items = {};
  fetchDetails(ids);
}

onMount(async() => {
  cleanFetchDetails(fnIds);
});

</script>

<DetailBox notif={notif} closeWindow={closeWindow}>
  {#each Object.entries(items) as [fnid, fn]}
  <p><span class="has-text-weight-bold">Producer</span> {fn.producer}</p>
  <div class="has-text-weight-bold">Storage locations:</div>
  {#each fn.servers as [server, path]}
  <div>
    <span class="has-text-primary">{server}</span> / {path}
  </div>
  {/each}

  {#if fn.description}
  <p><span class="has-text-weight-bold">Description:</span>{fn.description}</p>
  {/if}
  <div class="field is-grouped">
    <p class="control is-expanded">
      <input class="input is-small" bind:value={newname[fnid]} type="text"> 
    </p>
    <p class="control">
      <a on:click={renameFile(newname[fnid], fnid)} class="button is-small is-primary">Rename file</a>
    </p>
  </div>
  {/each}
</DetailBox>
