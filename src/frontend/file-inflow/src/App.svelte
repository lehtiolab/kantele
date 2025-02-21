<script>

import { getCookie, postJSON } from '../../datasets/src/funcJSON.js'
import TokenInstructions from './TokenInstructions.svelte'

let ft_selected = false;
let upl_type;
let token = false;
let onlyArchive = false;
let uploaddesc = '';
let selectedFile = [];
let uploadSuccess;
let uploadError;
let uploadRunning;
let copiedToken = false;
const descfts = new Set([libfile_id, userfile_id]);

async function createToken() {
  const resp = await postJSON('../token/', {ftype_id: ft_selected.id,
    archive_only: onlyArchive, uploadtype: upl_type});
  if (resp.error) {
    console.log('error');
    // FIXME
  } else {
    console.log(resp);
    token = resp;
  }
}

function copyToken() {
  navigator.clipboard.writeText(token.user_token);
  copiedToken = true;
  setTimeout(() => {copiedToken = false;}, 2000);
}

async function uploadFile() {
  uploadRunning = true;
  uploadError = uploadSuccess = false;
  let fdata = new FormData();
  fdata.append('file', selectedFile[0]);
  fdata.append('desc', uploaddesc);
  fdata.append('ftype_id', ft_selected.id);
  fdata.append('archive_only', onlyArchive ? '1' : '0');
  fdata.append('uploadtype', upl_type);
  const csrftoken = getCookie('csrftoken');
  let resp = await fetch('/files/upload/userfile/', {
    method: 'POST',
    body: fdata,
    credentials: 'same-origin',
    headers: {'X-CSRFToken': csrftoken},
  })
  uploadRunning = false;
  let jresp = {success: false};
  try {
    jresp = await resp.json();
  } catch {
    if (resp.status == 413) {
      uploadError = 'File to upload is too large - please contact admin';
    } else {
      uploadError = `Status ${resp.status} Something went wrong trying to send file to server, contact admin`;
    }
  }
  if (jresp.success) {
    uploadSuccess = jresp.msg;
  } else {
    uploadSuccess = false;
    uploadError = jresp.msg;
  }
}

</script>

<style>
.test{
  font-color: black;
}
</style>

<div class="columns">
  <div class="column">
    <div class="box has-background-link-light">
      <h5 class="title is-5">Automated file transfer</h5>
      <h5 class="subtitle is-5">for instruments with outbox</h5>
      <form action="download/" method="GET">
        <div class="field">
          <label class="label">Datadisk letter</label>
          <input type="hidden" name="client" value="instrument">
          <input type="text" name="datadisk" class="input" placeholder="Drive letter for data, e.g. D">
        </div>
      
        <div class="field">
          <label class="label">Producer</label>
          <div class="select">
            <select name="prod_id">
              {#each Object.entries(producers) as [pid, prod]}
              <option value={pid}>{prod}</option>
              {/each}
            </select>
          </div>
        </div>
        <div class="field">
          <label class="checkbox">
            <input name="configonly" value="true" type="checkbox">
            Only download configuration (not for new instruments, uploader updates)
          </label>
        </div>
        <input class="button" type="submit" value="Download package">
      </form>

      <h5 class="subtitle is-5">Instructions</h5>
      <div class="content">
        <ul>
          <li>Install <a href="https://www.python.org/downloads/">Python >=3.6</a> on the machine, for all users (not as an admin). When installing, make sure to check "add to PATH"</li>
          <li>Fill in the above form and download the zip file</li>
          <li>Move and unzip the file to an appropriate location, preferably on the Desktop for easy updating</li>
          <li>Run <code>setup.bat</code></li>
          <li>To create scheduled tasks, run the generated file (with administrator rights) <code>tasksetup.bat</code></li>
          <li>Run<code>transfer.bat</code></li>
        </ul>
      </div>
      <h5 class="subtitle is-5 has-margin-top-5">Config updates</h5>
      <div class="content">
        <ul>
          <li>Fill in the above form, tick the config-only box and download the <code>transfer.bat</code> file</li>
          <li>Replace original bat file with the new file</li>
          <li>Stop running transfer program</li>
          <li>Run<code>transfer.bat</code></li>
        </ul>
      </div>
    </div>
  </div>
  <div class="column">
    <div class="box has-background-success-light">
      <h5 class="title is-5">Upload your files</h5>
      <h5 class="subtitle is-5">directly or using a script</h5>
      (Administrators can enable uploads of more file types)

      {#if Object.keys(filetypes).length}
      <div class="field">
        <div class="select">
          <select bind:value={ft_selected}>
            <option value={false}>Select a filetype</option>
            {#each filetypes as ft}
            <option value={ft}>{ft.name}</option>
            {/each}
          </select>
        </div>
      </div>

      <div clas="field">
        <div class="checkbox">
          <input type="checkbox" bind:checked={onlyArchive}>
          <label clas="checkbox">Only store in archive/backup</label>
        </div>
      </div>

      <div clas="field">
        <div class="select">
          <select bind:value={upl_type}>
            <option value={userfile_id}>User file</option>
            <option value={libfile_id}>Library (shared with everyone)</option>
            {#if ft_selected && ft_selected.israw}
            <option value={rawfile_id}>Raw data (to tmp)</option>
            {/if}
          </select>
        </div>
      </div>

      {#if ft_selected}
        <hr>
        <h5 class="subtitle is-5">EITHER upload large/many files with a token</h5>
          <div on:click={createToken} class="button">Create token</div>
          {#if token}
          <button on:click={copyToken} class="button">
            {#if copiedToken}
            <span class="icon is-small"><i class="fa fa-check has-text-success"></i></span>
            {:else}
            <span class="icon is-small"><i class="fa fa-copy"></i></span>
            <span>Copy token</span>
            {/if}
          </button>
          <div>
            <input class="input" value={token.user_token} readonly />
            This token will expire {token.expires}
          </div>
          {/if}
          <hr>
  
        {#if !ft_selected.isfolder}
          <h5 class="subtitle is-5">OR upload a file directly in browser</h5>
          <div class="field">
          <div class="file has-name is-fullwidth">
            <label class="file-label">
              <input bind:files={selectedFile} class="file-input" type="file"> 
              <span class="file-cta">
                <span class="file-icon">
                  <i class="fas fa-upload"></i>
                </span>
                <span class="file-label">Choose a file...</span>
              </span>
              <span class="file-name">
                {#if uploadSuccess}
                <span class="has-icon"><i class="fa fa-check has-text-success"></i></span>
                {:else if uploadError}
                <span class="has-icon"><i class="fa fa-times has-text-danger"></i></span>
                {:else if uploadRunning}
                <span class="has-icon"><i class="fa fa-spinner fa-spin"></i></span>
                {/if}
                {#if selectedFile.length}
                {selectedFile[0].name}
                {/if}
              </span>
            </label>
          </div>
          </div>
          {#if descfts.has(upl_type)}
          <div class="field">
            <input class="input" bind:value={uploaddesc} type="text" placeholder="Description">
          </div>
          {/if}
  
          {#if uploadError}
          <div class="has-text-danger">{uploadError}</div>
          {:else if uploadSuccess}
          <div class="has-text-success">{uploadSuccess}</div>
          {/if}
          {#if selectedFile.length && (ft_selected.israw || uploaddesc) && !uploadRunning}
          <a class="button is-small" on:click={uploadFile}>Upload file</a>
          {:else}
          <a class="button is-small" disabled>Upload file</a>
          {/if}
        {:else}

            <label class="file-label">
              <span class="file-cta">
                <span class="file-icon">
                  <i class="fas fa-upload"></i>
                </span>
                <span class="file-label">Choose a file...</span>
              </span>
            </label>
        <div class="field">
          <a class="button is-small" disabled>Upload file</a>
        </div>
        Cannot upload filetypes that are folders (e.g. `.d` files)
        {/if}
      {/if}

      {:else}
      No upload of any file type is currently enabled!
      {/if}

      <hr>
      <TokenInstructions fileType="raw" />


    </div>
  </div>
</div>
