<script>

import { postJSON } from '../../datasets/src/funcJSON.js'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import { flashtime } from '../../util.js'
import Table from '../../home/src/Table.svelte';

let notif = {errors: {}, messages: {}, links: {}};
// pre-existing variables:
// selectedTrackedPeptidesInit
// trackedPeptideSetsInit

let servers = init_servers;
let shares = init_shares;

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

/////// QC table stuff
let showConfirm = false;
let loadedFiles = {};
let selectedFiles = [];

const tablefields = [
  {id: 'jobstate', name: '__hourglass-half', type: 'state', multi: true},
  {id: 'name', name: 'File', type: 'str', multi: false},
  {id: 'smallstatus', name: '', type: 'smallcoloured', multi: true},
  {id: 'dataset', name: '', type: 'icon', help: 'Dataset', icon: 'clipboard-list', multi: false},
  {id: 'date', name: 'Date', type: 'str', multi: false},
  {id: 'size', name: 'Size', type: 'str', multi: false},
  {id: 'backup', name: 'Backed up', type: 'bool', multi: false},
  {id: 'owner', name: 'Owner', type: 'str', multi: false},
  {id: 'ftype', name: 'Type', type: 'str', multi: false},
];
const inactive = ['inactive'];

function updateNotif() {
  Object.entries(notif.errors)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg));
  Object.entries(notif.messages)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg));
  notif = notif;
}


async function runNewQCs(acqtype) {
  const resp = await postJSON('/manage/qc/newfiles/', {sfids: selectedFiles, acqtype: acqtype});
  if (resp.state === 'ok') {
    notif.messages[resp.msg] = 1;
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
  }
  updateNotif();
}


async function removeFromQCs() {
  const resp = await postJSON('/manage/qc/remove/', {sfids: selectedFiles});
  if (resp.state === 'ok') {
    notif.messages[resp.msg] = 1;
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
  }
  updateNotif();
}

async function reRunQCs() {
  const resp = await postJSON('/manage/qc/rerun/', {sfids: selectedFiles, doConfirm: showConfirm});
  if (resp.state === 'confirm') {
    showConfirm = true;
    notif.messages[resp.msg] = 1;
    setTimeout(function() { showConfirm = false } , flashtime);
  } else if (resp.state === 'ok') {
    showConfirm = false;
    notif.messages[resp.msg] = 1;
  } else if (resp.state === 'error') {
    notif.errors[resp.msg] = 1;
  }
  updateNotif();
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

<div class="box">
  {#if selectedFiles.length}
  <a class="button is-small" title="Add files to QC, they must be DDA" on:click={e => runNewQCs('DDA')}>Add DDA file to QC</a>
  <a class="button is-small" title="Add files to QC, they must be DIA" on:click={e => runNewQCs('DIA')}>Add DIA file to QC</a>
  <a class="button is-small" title="Move datasets to cold storage (delete)" on:click={removeFromQCs}>Remove from QC</a>
  {#if showConfirm}
    <a class="button is-small is-danger" title="Rerun QCs for selected files" on:click={reRunQCs}>Yes, rerun!</a>
  {:else}
    <a class="button is-small" title="Rerun QCs for selected files" on:click={reRunQCs}>Rerun QCs</a>
  {/if}
  {:else}
    <a class="button is-small" title="Add files to QC, they must be DDA" disabled>Add DDA file to QC</a>
  <a class="button is-small" title="Add files to QC, they must be DIA" disabled>Add DIA file to QC</a>
    <a class="button is-small" title="Move datasets to cold storage (delete)" disabled>Remove from QC</a>
  <a class="button is-small" title="Rerun QCs for selected files" disabled>Rerun QCs</a>
  {/if}
  <h4 class="title is-4">QC files</h4>
  <Table tab="Files"
    bind:notif={notif}
    bind:selected={selectedFiles}
    bind:items={loadedFiles}
    fetchUrl="/show/files/"
    findUrl="/show/files/"
    show_deleted_or_q="from:365d, from:yyyymmdd, to:, deleted:true/false, type:raw/analysis/shared/qc"
    defaultQ="type:qc from:30d"
    getdetails={() => 'cannot show details'}
    fixedbuttons={[]}
    fields={tablefields}
    inactive={inactive}
    allowedActions={[]}
    />
</div>

<div class="columns">
  <div class="column">
    <div class="box has-background-link-light">
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
