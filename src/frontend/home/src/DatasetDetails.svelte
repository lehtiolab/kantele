<script>
import { onMount } from 'svelte';
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'
import DetailBox from './DetailBox.svelte'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import DatasetPipeline from '../../datasets/src/DatasetPipeline.svelte'

export let closeWindow;
export let dsetIds;

let refine_dbid;
let new_storage_shares = {};

let notif = {errors: {}, messages: {}};
let dsets = {};
let owner_to_add = Object.fromEntries(dsetIds.map(x => [x, false]));
let replace_pwiz_id = Object.fromEntries(dsetIds.map(x => [x, false]));
let refine_v_touse = Object.fromEntries(dsetIds.map(x => [x, false]));
let pwstateColors = {
  Ready: 'is-success',
  Incomplete: 'is-warning',
  Processing: 'is-danger is-light',
}

// If user clicks new dataset, show that instead, run when dsetIds is updated:
$: {
  cleanFetchDetails(dsetIds);
}

function new_owners(allowners, oldowners) {
  const difference = Object.keys(allowners).concat(Object.keys(oldowners)).reduce(function(r, cur) {
    if (!r.delete(cur)) {
      r.add(cur);
    } return r}, new Set());
  return Array.from(difference);
}

async function convertDset(dsid, pwiz_id) {
  const resp = await postJSON('mzml/create/', {dsid: dsid, pwiz_id: pwiz_id});
  if (!resp.ok) {
    const msg = `Something went wrong trying to queue dataset mzML conversion: ${resp.error}`;
    showError(msg);
  } else {
    cleanFetchDetails(dsetIds);
    const msg = 'Queued dataset for mzML conversion';
    showError(msg);
  }
}

async function refineDset(dsid, wfid, dbid) {
  const resp = await postJSON('mzml/refine/', {dsid: dsid, wfid: wfid, dbid: dbid});
  if (!resp.ok) {
    const msg = `Something went wrong trying to queue precursor refining: ${resp.error}`;
    showError(msg);
  } else {
    cleanFetchDetails(dsetIds);
    const msg = 'Queued dataset for mzML precursor refining';
    showError(msg);
  }
}

async function deleteMzmls(dsid, pwsets_id, force) {
  if (!force) {
    //pwiz.ask_force_delete = true;
    dsets[dsid].pwiz_sets[pwsets_id].ask_force_delete = true;
    setTimeout(() => { dsets[dsid].pwiz_sets[pwsets_id].ask_force_delete = false} , flashtime);
  } else {
    const pwiz = dsets[dsid].pwiz_sets[pwsets_id];
    const resp = await postJSON('mzml/delete/', {dsid: dsid, pwiz_id: pwiz.id, refined: pwiz.refined});
    if (!resp.ok) {
      const msg = `Something went wrong trying to delete mzMLs: ${resp.error}`;
      showError(msg);
    } else {
      cleanFetchDetails(dsetIds);
      const msg = 'Queued deletion of mzMLs';
      showError(msg);
    }
  }
}

async function changeOwner(dsid, owner, op) {
  const resp = await postJSON('datasets/save/owner/', {
    'dataset_id': dsid, 
    'op': op,
    'owner': owner});
  if (!resp.ok) {
    const msg = `Something went wrong trying to change owner of the dataset: ${resp.error}`;
    showError(msg);
  } else {
    fetchDetails([dsid]);
  }
  owner_to_add[dsid] = false;
}

async function fetchDetails(dsetids) {
  let fetchedDsets = {}
  let fetchedStorages = {}
  const tasks = dsetids.map(async dsetId => {
    const resp = await getJSON(`/show/dataset/${dsetId}`);
    if (!resp.ok) {
      const msg = `Something went wrong fetching dataset info: ${resp.error}`;
      showError(msg);
    } else {
      fetchedDsets[dsetId] = resp;
      fetchedStorages[dsetId] = resp.storage_shares;
    }
  });
  const result = await Promise.all(tasks);
  dsets = Object.assign(dsets, fetchedDsets);
  new_storage_shares = Object.assign({}, fetchedStorages);
}

function cleanFetchDetails(dsetids) {
  dsets = {};
  fetchDetails(dsetids);
}

function showError(error) {
  notif.errors[error] = 1;
  setTimeout(function(error) { notif.errors[error] = 0 } , flashtime, error);
}

async function reactivateDset(dsid, share_ids) {
  const resp = await postJSON('datasets/undelete/dataset/', {
    'dataset_id': dsid, 
    'storage_shares': share_ids,
    });
  if (!resp.ok) {
    const msg = `Something went wrong trying to change reactivate dataset: ${resp.error}`;
    showError(msg);
  } else {
    fetchDetails([dsid]);
  }
}

async function updateDsetStorage(dsid, share_ids) {
  const resp = await postJSON('datasets/save/storage/', {
    'dataset_id': dsid, 
    'storage_shares': share_ids,
    });
  if (!resp.ok) {
    const msg = `Something went wrong trying to change dataset storage: ${resp.error}`;
    showError(msg);
    fetchDetails([dsid]);
  } else {
    notif.messages[resp.msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
    fetchDetails([dsid]);
  }
}


onMount(async() => {
  cleanFetchDetails(dsetIds);
});

</script>

<DetailBox notif={notif} closeWindow={closeWindow}>
  {#each Object.entries(dsets) as [dsid, dset]}
   <div class="has-text-weight-bold">Stored at:</div>
    {#if dset.storage_shares.length}
      <button class="button is-small" on:click={e => updateDsetStorage(dsid, new_storage_shares[dsid])}>Update storage</button>
    {:else}
      <button class="button is-small" on:click={e => reactivateDset(dsid, new_storage_shares[dsid])}>Reactivate</button>
    {/if}
    {#each Object.values(dset.all_storlocs) as loc}
      <div class={`tag is-medium ${dset.storage_shares.indexOf(loc.id) > -1 ? 'is-success' : ''}`}>
          <label class="checkbox">
            <input value={loc.id} bind:group={new_storage_shares[dsid]} type="checkbox" />
            {loc.name}
        </label>
      </div>
    {/each}
  <hr>
  <div class="columns">
    <div class="column is-one-third">

      <div class="field">
        {#each Object.entries(dset.nrstoredfiles) as fn}
        <div>{fn[1]} stored files of type {fn[0]}</div>
        {/each}
      </div>

      {#if dset.qtype}
      <div><span class="has-text-weight-bold">Quant type</span> {dset.qtype.name}</div>
      {/if}

      <div class="has-text-weight-bold">Instrument(s)</div>
      <div class="field is-grouped is-grouped-multiline">
        {#each dset.instruments as instr}
        <div class="control">
          <div class="tags">
            <span class="tag is-light">{instr}</span>
          </div>
        </div>
        {/each}
      </div>


      <div class="has-text-weight-bold">Owners</div>
      <div class="field is-grouped is-grouped-multiline">
        {#each Object.entries(dset.owners) as [usrid, owner]}
        <div class="control">
          <div class="tags has-addons">
            <span class="tag is-light">{owner}</span>
            <a class="tag is-info is-delete" on:click={e => changeOwner(dsid, usrid, 'del')}></a>
          </div>
        </div>
        {/each}
      </div>
      <div class="field">
        <div class="control">
          <div class="select">
            <select bind:value={owner_to_add[dsid]} on:change={e => changeOwner(dsid, owner_to_add[dsid], 'add')}>
              <option disabled value={false}>Add an owner</option>
              {#each new_owners(dset.allowners, dset.owners) as newusrid}
              <option value={newusrid}>{dset.allowners[newusrid]}</option>
              {/each}
            </select>
          </div>
        </div>
      </div>

    </div>
    <div class="column is-two-thirds">

      {#if dset.pipeline}
      <DatasetPipeline on:error={e => showError(e.detail.error)} pipeSteps={dset.pipeline.steps} samplePrepCategories={dset.pipeline.prepcategories} bind:savedStageDates={dset.pipeline.prepdatetrack} bind:pipeStepsDone={dset.pipeline.prepsteptrack} bind:dspipeId={dset.pipeline.dspipe_id} />
      {/if}


    </div>
  </div>

  {#if 'pwiz_versions' in dset}
  <div class="field">
    <label class="label">Conversion mzML results / pipeline version(s)</label>
    <table class="table">
      <tbody>
        {#each Object.entries(dset.pwiz_sets) as [pwid, pw]}
        <tr>
          <td>
            {#if pw.state !== 'No mzmls'}
              {#if pw.ask_force_delete}
                <button class="button is-small is-danger" on:click={e => deleteMzmls(dsid, pwid, true)}>Are you sure?</button>
              {:else}
                <button class="button is-small is-danger is-outlined" on:click={e => deleteMzmls(dsid, pwid, false)} >Delete mzMLs</button>
              {/if}
            {/if}
            {#if (pw.state === 'Incomplete' && pw.refined && pw.active)}
            <div class="select is-small">
              <select bind:value={refine_v_touse[dset.id]}>
                <option value="">Pick a refine version</option>
                {#each dset.refine_versions as {id, name}}
                <option value={id}>Refine {name}</option>
                {/each}
              </select>
            </div>
            <button class="button is-small" on:click={e => refineDset(dsid, refine_v_touse[dset.id], refine_dbid)}>Re-refine</button>
            <DynamicSelect bind:selectval={refine_dbid} niceName={x => x.name} fixedoptions={dset.refine_dbs} placeholder='Pick a db for your organism' />
            {:else if !pw.refined && pw.active && (pw.state === 'Incomplete' || pw.state === 'No mzmls')}
            <button class="button is-small" on:click={e => convertDset(dsid, pw.id)}>Re-convert</button>
            {:else if pw.refineready}
            <div class="select is-small">
              <select bind:value={refine_v_touse[dset.id]}>
                <option value="">Pick a refine version</option>
                {#each dset.refine_versions as {id, name}}
                <option value={id}>Refine {name}</option>
                {/each}
              </select>
            </div>
            <button class="button is-small" on:click={e => refineDset(dsid, refine_v_touse[dset.id], refine_dbid)}>Refine mzML</button>
           <DynamicSelect bind:selectval={refine_dbid} niceName={x => x.name} fixedoptions={dset.refine_dbs} placeholder='Pick a db for your organism' />
            {/if}
          </td>
          <td>
            <span class={`tag ${pwstateColors[pw.state]}`}>
              {pw.state}
            </span>
          </td>
          <td>
            <span class="has-text-weight-bold">{pw.version}</span>
            <span>, created {pw.created}</span>
            {#if pw.state === 'Incomplete'}
            <span>, {pw.existing} files</span>
            {/if}
          </td>

          <td>
            {#if pw.refined}
            <span class="tag is-light is-warning">Refined</span>
            {/if}
          </td>
        </tr>
        {/each}
      </tbody>
    </table>
  </div>
    
  <div class="field">
    {#if Object.keys(dset.pwiz_versions).length}
    <div>Or replace with mzMLs of another version</div>
    <div class="select">
      <select bind:value={replace_pwiz_id[dset.id]}>
        <option value="">Pick a proteowizard version</option>
        {#each Object.entries(dset.pwiz_versions) as pwiz_v}
        <option value={pwiz_v[0]}>{pwiz_v[1]}</option>
        {/each}
      </select>
    </div>
    {#if replace_pwiz_id[dset.id]}
    <button on:click={e => convertDset(dsid, replace_pwiz_id[dset.id])} class="button">Convert!</button>
    {:else}
    <button disabled class="button">Convert!</button>
    {/if}
    {/if}
  </div>
  {/if}

  {/each}
</DetailBox>
