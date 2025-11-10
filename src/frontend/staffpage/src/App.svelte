<script>

import { postJSON } from '../../datasets/src/funcJSON.js'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import { flashtime } from '../../util.js'

let notif = {errors: {}, messages: {}, links: {}};
// pre-existing variables:
// qc_instruments = {id: name}
// selectedTrackedPeptidesInit
// trackedPeptideSetsInit

let servers = init_servers;
let shares = init_shares;

let qc_reruns = Object.fromEntries(
    Object.keys(instruments)
    .map(k => [k, false])
    );
let rerunAllInstruments = false;
let rerunNumberDays = 0;
let rerunFromDate = 'today';
let showConfirm = false;

let selectedSingle = '';
let selectedNewfile = '';
let acqtype = [];

let newPeptideSetName = '';
let editingTrackedPeptides = true;
let trackedPeptidesName;
let trackedPeptides = {frozen: false, active: false, peptides: {}, name: '', date: ''}
let trackedPeptideSets = trackedPeptideSetsInit ? trackedPeptideSetsInit : {};
let selectedTrackedPeptides = selectedTrackedPeptidesInit ? selectedTrackedPeptidesInit : false;

if (selectedTrackedPeptides) {
  trackedPeptides = trackedPeptideSets[selectedTrackedPeptides];
  editingTrackedPeptides = trackedPeptides.frozen !== true;
  trackedPeptidesName = trackedPeptides.name;
}


const runButtons = {
  single: false,
  new: false,
}

let ignoreObsolete = false;
let retrieveBackups = false;


function getRerunFromDate() {
    showConfirm = false;
    if (!rerunNumberDays) {
      rerunFromDate = 'today';
    } else if (rerunNumberDays === 1) {
      rerunFromDate = 'yesterday';
    } else {
      const today = new Date();
      today.setDate(today.getDate() - rerunNumberDays);
      rerunFromDate = today.toLocaleDateString();
    }
  }


function toggleRerunAll() {
    rerunAllInstruments = rerunAllInstruments === false;
    Object.keys(qc_reruns)
      .forEach(k => { qc_reruns[k] = rerunAllInstruments;
        });
  }

function checkRerun() {
  postRerun(false);
}

function confirmRerun() {
  postRerun(true);
}

async function runNewSingleFile() {
  const resp = await postJSON('/manage/qc/newfile/', {sfid: selectedNewfile, acqtype: acqtype});
  if (resp.state === 'ok') {
    notif.messages[resp.msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, resp.msg);
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
  }
}

async function runSingleFile() {
  const resp = await postJSON('/manage/qc/rerunsingle/', {sfid: selectedSingle});
  if (resp.state === 'ok') {
    notif.messages[resp.msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, resp.msg);
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
  }
}

async function postRerun(confirm) {
    const rerun_ids = Object.entries(qc_reruns)
      .filter(([k,v]) => v)
      .map(([k,v]) => k);
    const data = {days: rerunNumberDays, instruments: rerun_ids, confirm: confirm,
        ignore_obsolete: ignoreObsolete, retrieve_archive: retrieveBackups,
      };
    const resp = await postJSON('/manage/qc/rerunmany/', data);
    if (resp.state === 'confirm') {
      showConfirm = true;
      notif.messages[resp.msg] = 1;
      setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, resp.msg);
    } else if (resp.state === 'error') {
      notif.errors[resp.msg] = 1;
      setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
    } else {
      showConfirm = false;
      notif.messages[resp.msg] = 1;
      setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, resp.msg);
      ignoreObsolete = false;
      retrieveBackups = false;
    }
  }

function addTrackedPeptide() {
  let new_ix = 0;
  if (Object.keys(trackedPeptides.peptides).length) {
    new_ix = Math.max(...Object.keys(trackedPeptides.peptides).map(x => parseInt(x))) + 1;
  }
  trackedPeptides.peptides[new_ix] = {seq: '', charge: "2", ix: new_ix};
}

function removeTrackedPeptide(ix) {
  delete(trackedPeptides.peptides[ix]);
  trackedPeptides.peptides = trackedPeptides.peptides;
}

function selectTPS() {
  trackedPeptides = trackedPeptideSets[selectedTrackedPeptides];
  trackedPeptidesName = trackedPeptides.name;
  editingTrackedPeptides = trackedPeptides.frozen !== true;
}


async function saveTrackedPeptides(publish) {
  const resp = await postJSON('/manage/qc/trackpeptides/save/', {
    peptides: Object.values(trackedPeptides.peptides),
    tpsname: trackedPeptidesName,
    tpsid: selectedTrackedPeptides,
    publish: publish,
  });
  trackedPeptides.name = trackedPeptidesName;
  if (resp.state == 'error') {
    notif.errors[resp.msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
  } else {
    const msg = 'Saved peptide set';
    notif.messages[msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
    trackedPeptides.id = resp.data.id;
    trackedPeptides.date = resp.data.date;
    trackedPeptides.frozen = resp.data.frozen;
    trackedPeptides.active = resp.data.active;
    if (!selectedTrackedPeptides) {
      trackedPeptideSets[resp.data.id] = trackedPeptides;
      trackedPeptideSets = trackedPeptideSets;
      selectedTrackedPeptides = resp.data.id;
    }
  }
}

async function deletePepset(publish) {
  const resp = await postJSON('/manage/qc/trackpeptides/delete/', {
    tpsid: selectedTrackedPeptides,
  });

//  trackedPeptides.name = trackedPeptidesName;
  if (resp.state == 'error') {
    notif.errors[resp.msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
  } else {
    const msg = 'Deleted peptide set';
    notif.messages[msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
    trackedPeptides = {frozen: false, active: false, peptides: {}, name: '', date: ''}
    delete(trackedPeptideSets[selectedTrackedPeptides]);
    trackedPeptideSets = trackedPeptideSets;
    selectedTrackedPeptides = false;
  }
}


function newPepsetFromOld() {
  trackedPeptides = {frozen: false, active: false, peptides: trackedPeptides.peptides, name: '', date: ''}
  trackedPeptidesName = '';
  selectedTrackedPeptides = false;
  editingTrackedPeptides = true;
}

function newPepsetBlank() {
  trackedPeptides = {frozen: false, active: false, peptides: {}, name: '', date: ''}
  trackedPeptidesName = '';
  selectedTrackedPeptides = false;
  editingTrackedPeptides = true;
}


function activateRunButton(openthis) {
  // Somehow selectedSingle will not update in the UI even if it is updated in the code?
  // V. strange
  runButtons[openthis] = true;
} 


function addServer() {
  const server = {pk: false, name: '', uri: '', fqdn: '', active: true, can_backup: false,
    can_rsync_remote: false, rsyncusername: '', rsynckeyfile: '', show_analysis_profile: false,
    mounted: [],
  }
  servers = [server, ...servers];
}

function addStorageShare() {
  const share = {pk: false, name: '', max_security: 1, description: '', active: true,
    function: 1, maxdays_data: 0
  }
  shares = [share, ...shares]
}

function addStorageShareToServer(server) {
  server.mounted = [...server.mounted, {share: false, path: ''}];
  servers = servers;
}

async function toggleServerActive(server) {
  server.active = !server.active;
  saveServer(server)
  servers = [server, ...servers.filter(x => x.pk != server.pk)];
}

async function toggleShareActive(share) {
  share.active = !share.active;
  saveShare(share)
  shares = [share, ...shares.filter(x => x.pk != share.pk)];
}


async function saveServer(server) {
  const resp = await postJSON('servers/save/', server);
  if (resp.state === 'ok') {
    notif.messages[resp.msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, resp.msg);
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
  }
}

async function saveShare(share) {
  const resp = await postJSON('shares/save/', share);
  if (resp.state === 'ok') {
    notif.messages[resp.msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, resp.msg);
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, resp.msg);
  }
}

</script>

<style>
.errormsg {
  position: -webkit-sticky;
  position: sticky;
  top: 20px;
  z-index: 50000;
}
</style>

<div class="errormsg">
{#if Object.values(notif.errors).some(x => x === 1)}
<div class="notification is-danger is-light"> 
    {#each Object.entries(notif.errors).filter(x => x[1] == 1).map(x=>x[0]) as error}
    <div>{error}</div>
    {/each}
</div>
{/if}
{#if Object.values(notif.messages).some(x => x === 1)}
<div class="notification is-success is-light errormsg"> 
    {#each Object.entries(notif.messages).filter(x => x[1] == 1).map(x=>x[0]) as message}
    <div>{message}</div>
    {/each}
</div>
{/if}
</div>

<div class="columns">
  <div class="column">
    <div class="box has-background-link-light">
      <h4 class="title is-4">QC</h4>
      <h5 class="title is-5">Rerun a batch of files with latest QC workflow</h5>
      <h5 class="subtitle is-5">Excludes deleted </h5>
      <div class="columns">
        <div class="column">
          <div class="field">
            <label class="checkbox">
              <input on:click={toggleRerunAll} bind:checked={rerunAllInstruments} type="checkbox"> All instruments
            </label>
          </div>
          {#each Object.entries(instruments) as [id, name]}
          <div class="field">
            <label class="checkbox">
              <input on:click={e => showConfirm = false} bind:checked={qc_reruns[id]} type="checkbox"> {name}
            </label>
          </div>
          {/each}
        </div>
        <div class="column">
          <div class="field">
            <label class="label">How many days ago to rerun from</label>
            <input type="number" class="input" on:change={getRerunFromDate} bind:value={rerunNumberDays} />
            Rerun from {rerunFromDate}
          </div>
          {#if Object.entries(qc_reruns).filter(([k,v]) => v).length}
          <button on:click={checkRerun} class="button">Check reruns</button>
          {:else}
          <button on:click={checkRerun} class="button" disabled>Check reruns</button>
          {/if}
          {#if showConfirm}
          <button on:click={confirmRerun} class="button">Confirm</button>
          {:else}
          <button on:click={confirmRerun} class="button" disabled>Confirm</button>
          {/if}
          <div class="field mt-4">
            <label class="checkbox">
              <input bind:checked={ignoreObsolete} type="checkbox"> Ignore obsolete warning
            </label>
          </div>
          <div class="field mt-4">
            <label class="checkbox">
              <input bind:checked={retrieveBackups} type="checkbox"> Retrieve archived files from backup 
            </label>
          </div>

        </div>
      </div>
      <h5 class="title is-5">... or select a single rerun</h5>
      <DynamicSelect bind:selectval={selectedSingle} on:selectedvalue={e => activateRunButton('single')} niceName={x => x.name} fetchUrl="/manage/qc/searchfiles/" placeholder="instrument name, date" />
      {#if runButtons.single}
      <button on:click={runSingleFile} class="button">Run</button>
      {:else}
      <button class="button" disabled>Run</button>
      {/if}
      <hr>

      <h5 class="title is-5">... or designate a new file to QC</h5>
      <DynamicSelect bind:selectval={selectedNewfile} on:selectedvalue={e => activateRunButton('new')} niceName={x => x.name} fetchUrl="/manage/qc/searchnewfiles/" placeholder="instrument name, date" />
      <div class="control">
        <label class="radio">
          <input bind:group={acqtype} value="DDA" name="acqtype" type="radio" />
          DDA
        </label>
        <label class="radio">
          <input bind:group={acqtype} value="DIA" name="acqtype" type="radio" />
          DIA
        </label>
        </div>
      {#if runButtons.new}
      <button on:click={runNewSingleFile} class="button">Run</button>
      {:else}
      <button class="button" disabled>Run</button>
      {/if}
      <hr />

      <h5 class="title is-5">QC tracked peptides</h5>
      <h5 class="subtitle is-5">As of date</h5>
      <span class="has-text-weight-bold is-size-6">
        Select peptide set 
      </span>
      <span class="icon">
        {#if trackedPeptides.frozen}
        <i class="fa fa-lock has-text-grey"></i>
        {:else}
        <i class="fa fa-lock-open has-text-grey"></i>
        {/if}
      </span>
      {#if trackedPeptides.active}
      <span class="tag is-success">Currently tracking</span>
      {:else if trackedPeptides.frozen}
      <span class="tag is-light">Old</span>
      {:else}
      <span class="tag is-info">Draft</span>
      {/if}

      <DynamicSelect on:selectedvalue={selectTPS} bind:intext={trackedPeptidesName} bind:unknowninput={newPeptideSetName} bind:selectval={selectedTrackedPeptides} niceName={x => `${x.name}` } fixedoptions={trackedPeptideSets} />

      {#if Object.keys(trackedPeptides.peptides).length}
        <button class="button is-small is-warning" on:click={e => saveTrackedPeptides(false)}>Save</button>
        <button class="button is-small is-success" on:click={e => saveTrackedPeptides(true)}>Publish</button>
      {/if}

      {#if selectedTrackedPeptides}
        <button class="button is-small is-info" on:click={newPepsetBlank}>New</button>
        {#if trackedPeptides.frozen}
          <button class="button is-small is-info" on:click={newPepsetFromOld}>Copy to new set</button>
        {:else}
          <button class="button is-small is-danger" on:click={deletePepset}>Delete</button>
        {/if}
      {/if}

      {#if trackedPeptides.name && !trackedPeptides.frozen}
        <p class="control">
          <label class="label">Edit peptide set name</label>
          <input class="input" bind:value={trackedPeptidesName} />
        </p>
      {/if}

      <hr /> 
      <span class="has-text-weight-bold is-size-6">Peptide / charge</span>

      {#if editingTrackedPeptides}
      <a title="Add another peptide" on:click={addTrackedPeptide}><i class="fa fa-plus-square"></i></a>
      {/if}

      {#each Object.values(trackedPeptides.peptides) as tp}
      <div class="field has-addons">
        <p class="control">
          <input class="input" bind:value={tp.seq} />
        </p>
        <p class="control">
          <span class="select">
            <select bind:value={tp.charge}>
              <option value="2">+2</option>
              <option value="3">+3</option>
              <option value="4">+4</option>
              <option value="5">+5</option>
              <option value="6">+6</option>
            </select>
          </span>
        </p>
        <p class="control">
          <a class="button" title="Remove this peptide">
            <span on:click={e => removeTrackedPeptide(tp.ix)} class="icon is-small">
              <i class="fa fa-trash-alt"></i>
            </span>
          </a>
        </p>
      </div>
      {/each}
    </div>
  </div>


  <div class="column">
    <div class="box has-background-link-light">
      <h4 class="title is-4">
        Servers
        <button class="button is-small is-info has-text-weight-bold mt-1" on:click={addServer}>Add</button>
      </h4>
      {#each servers.filter(x => x.active).concat(servers.filter(x => !x.active)) as server}
      <div class="box">
        <div class="field is-horizontal">
          <button class="button is-small is-danger has-text-weight-bold mt-1" on:click={e => saveServer(server)}>Save</button>
          {#if server.active}
          <button class="button is-small is-danger has-text-weight-bold mt-1 ml-2" on:click={e => toggleServerActive(server)}>
          Deactivate
          </button>
          {:else}
          <button class="button is-small is-success has-text-weight-bold mt-1 ml-2" on:click={e => toggleServerActive(server)}>
          Activate
          </button>
          {/if}
          <span class="field ml-3 mr-3 mt-2">
            <label class="checkbox">
              <input bind:checked={server.can_rsync_remote} type="checkbox"> Storage controller
            </label>
          </span>
          <span class="field ml-3 mr-3 mt-2">
            <label class="checkbox">
              <input bind:checked={server.can_backup} type="checkbox"> Backup controller
            </label>
          </span>
          <span class="field ml-3 mr-3 mt-2">
            <label class="checkbox">
              <input bind:checked={server.show_analysis_profile} type="checkbox"> Analysis
            </label>
          </span>
        </div>
    
        <div class="field is-horizontal">
          {#if !server.active}
          <div class="field-label is-normal">
            <label class="label has-text-danger">INACTIVE</label>
          </div>
          {/if}
          <div class="field-body">
            <input class="input is-medium" placeholder="Userfriendly short name of server" bind:value="{server.name}" />
          </div>
        </div>

        <div class="field is-horizontal">
          <div class="field-label is-normal">
            <label class="label">URL</label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control">
                <input class="input" bind:value="{server.fqdn}" />
              </p>
            </div>
          </div>
        </div>
        <div class="field is-horizontal">
          <div class="field-label is-normal">
            <label class="label">Rsync username</label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control">
                <input class="input" bind:value="{server.rsyncusername}" />
              </p>
            </div>
          </div>
        </div>
        <div class="field is-horizontal">
          <div class="field-label is-normal">
            <label class="label">Rsync key</label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control">
                <input class="input" bind:value="{server.rsynckeyfile}" />
              </p>
            </div>
          </div>
        </div>


        {#if server.show_analysis_profile}
        <div class="box has-background-info-light">
          <h6 class="title is-6">Analysis server</h6>
          <div class="field is-horizontal">
            <div class="field-label is-normal">
              <label class="label">Nextflow profiles</label>
            </div>
            <div class="field-body">
              <div class="field">
                <p class="control">
                  <input class="input" placeholder='E.g. ["lehtio", "qc"]' bind:value="{server.nfprofiles}" />
                </p>
              </div>
            </div>
          </div>

          <div class="field is-horizontal">
            <div class="field-label is-normal">
              <label class="label">Analysis queue</label>
            </div>
            <div class="field-body">
              <div class="field">
                <p class="control">
                  <input class="input" bind:value="{server.queue_name}" />
                </p>
              </div>
            </div>
          </div>

          <div class="field is-horizontal">
            <div class="field-label is-normal">
              <label class="label">Scratchdir</label>
            </div>
            <div class="field-body">
              <div class="field">
                <p class="control">
                  <input class="input" placeholder="Can be empty" bind:value="{server.scratchdir}" />
                </p>
              </div>
            </div>
          </div>
        </div>
        {/if}
        {#each server.mounted as fss}
          <div class="box">
            <div class="field is-horizontal">
              <div class="field-label is-normal">
                <label class="label">Share</label>
              </div>
              <div class="field-body">
                <div class="select">
                  <select bind:value={fss.share}>
                  {#each shares.filter(x => x.active) as share}
                    <option value={share.pk}>{share.name}</option>
                  {/each}
                  </select>
                </div>
              </div>
            </div>
            <div class="field is-horizontal">
              <div class="field-label is-normal">
                <label class="label">Mount path</label>
              </div>
              <div class="field-body">
                <div class="field">
                  <p class="control">
                    <input class="input" placeholder="Can be empty" bind:value="{fss.path}" />
                  </p>
                </div>
              </div>
            </div>
          </div>
        {/each}
        <button class="button is-small is-info has-text-weight-bold mt-1" on:click={e => addStorageShareToServer(server)}>Add share</button>
      </div>
      {/each}

    </div>

    <div class="box has-background-link-light">
      <h4 class="title is-4">
        Storage shares
        <button class="button is-small is-info has-text-weight-bold mt-1" on:click={addStorageShare}>Add</button>
      </h4>
      {#each shares.filter(x => x.active).concat(shares.filter(x => !x.active)) as share}
      <div class="box">
        <div class="field is-horizontal">
          <button class="button is-small is-danger has-text-weight-bold mt-1" on:click={e => saveShare(share)}>Save</button>
          {#if share.active}
          <button class="button is-small is-danger has-text-weight-bold mt-1 ml-2" on:click={e => toggleShareActive(share)}>
          Deactivate
          </button>
          {:else}
          <button class="button is-small is-success has-text-weight-bold mt-1 ml-2" on:click={e => toggleShareActive(share)}>
          Activate
          </button>
          {/if}
        </div>
    
        <div class="field is-horizontal">
{#if !share.active}
          <div class="field-label is-normal">
            <label class="label has-text-danger">INACTIVE</label>
          </div>
{/if}
          <div class="field-body">
            <input class="input is-medium" placeholder="Userfriendly short name of share" bind:value="{share.name}" />
          </div>
        </div>

        <div class="field is-horizontal">
          <div class="field-label is-normal">
            <label class="label">Function</label>
          </div>
          <div class="field-body">
            <div class="select">
              <select bind:value={share.function}>
              {#each sharefuns as sf}
                <option value={sf[0]}>{sf[1]}</option>
              {/each}
              </select>
            </div>
          </div>
        </div>
        <div class="field is-horizontal">
          <div class="field-label is-normal">
            <label class="label">Security class</label>
          </div>
          <div class="field-body">
            <div class="select">
              <select bind:value={share.max_security}>
              {#each secclasses as sc}
                <option value={sc[0]}>{sc[1]}</option>
              {/each}
              </select>
            </div>
          </div>
        </div>

        <div class="field is-horizontal">
          <div class="field-label is-normal">
            <label class="label">Data lifespan (days)</label>
          </div>
          <div class="field-body">
            <input class="input" type="number" placeholder="Days before auto-deleting data, 0 means never delete" bind:value="{share.maxdays_data}" />
          </div>
        </div>


      </div>
      {/each}
    </div>
  </div>
</div>
