<script>
import { getJSON, postJSON } from './funcJSON.js'
import { dataset_id, datasetFiles } from './stores.js';
import ErrorNotif from './ErrorNotif.svelte';
import DynamicSelect from './DynamicSelect.svelte';

export let errors;

let lcerrors = [];
let channelError = {};
let pastedfnch = '';

let lcdata = {
  quants: {},
  quanttype: '',
  samples: {},
}
let edited = false;
let saved = false;

$: Object.keys($datasetFiles).length ? fetchData() : false;

$: stored = $dataset_id && !edited && saved;


function editMade() {
  edited = true;
}

function okChannel(fid) {
  lcdata.samples[fid].badChannel = false;
  editMade();
}


function parseSampleNames() {
  /* Parses samples/files/channel combinations pasted in textbox */
  let fnmap = {};
  for (let fn of Object.values($datasetFiles)) {
    fnmap[fn.name] = fn;
  }
  const chmap = Object.fromEntries(
    Object.entries(lcdata.quants[lcdata.quanttype].chans)
    .map(([chid, chan]) => [chan.name, chid])
    );

  for (let line of pastedfnch.trim().split('\n')) {
    if (line.indexOf('\t') > -1) {
      line = line.trim().split('\t').map(x => x.trim());
    } else if (line.indexOf('    ') > -1) {
      line = line.trim().split('    ').map(x => x.trim());
    }
    let chan, aid;
    line[0] in fnmap ? (aid = fnmap[line[0]], chan = line[1]) : false;
    line[1] in fnmap ? (aid = fnmap[line[1]], chan = line[0]) : false;
    if (aid) {
      lcdata.samples[aid.associd].channel = chmap[chan];
    }
  }
  editMade();
}


function validate() {
  let comperrors = [];
	if (!lcdata.quanttype) {
		comperrors.push('Quant type selection is required');
	}
  for (let fn of Object.values($datasetFiles)) {
    if (!lcdata.samples[fn.associd].channel) {
      comperrors.push('Channel for each file/sample is required');
    }
  }	
  return comperrors;
}

async function save() {
  errors = validate();
  if (!Object.keys($datasetFiles).length) {
    lcerrors = [...lcerrors, 'Add files before saving data'];
  }
  if (errors.length === 0) { 
    let postdata = {
      pooled: false,
      dataset_id: $dataset_id,
      quanttype: lcdata.quanttype,
      samples: lcdata.samples,
      filenames: Object.values($datasetFiles),
    }
    const url = '/datasets/save/labelcheck/';
    const response = await postJSON(url, postdata);
    fetchData();
  }
}

async function fetchData() {
  let url = '/datasets/show/labelcheck/';
  url = $dataset_id ? url + $dataset_id : url;
  const response = await getJSON(`${url}?lctype=single`);
  for (let [key, val] of Object.entries(response)) { lcdata[key] = val; }
  edited = false;
  if (lcdata.samples.length) {
    saved = true;
  }
}

</script>


<h5 id="labelcheck" class="has-text-primary title is-5">
  {#if stored}
  <i class="icon fas fa-check-circle"></i>
  {:else}
  <i class="icon fas fa-edit"></i>
  {/if}
  Label check
  <button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
  <button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
</h5>

<ErrorNotif errors={lcerrors} />

<div class="field">
  <label class="label">Quant type</label>
  <div class="control">
    <div class="select">
      <select on:change={editMade} bind:value={lcdata.quanttype}>
        <option disabled value="">Please select one</option>
        {#each Object.values(lcdata.quants) as quant}
        <option value={quant.id}>{quant.name}</option>
        {/each}
      </select>
    </div>
  </div>
</div>


<div class="field">
  <label class="label">Channel/file</label>
  <textarea class="textarea" bind:value={pastedfnch} placeholder="Paste your file names with channels here (one line per file, tab (or 4 spaces) separated file and channel)"></textarea>
  <a class="button is-primary" on:click={parseSampleNames}>Parse sample names</a>
</div>

<table class="table is-fullwidth" >
  <thead>
    <tr>
      <th>Sample</th>
      <th>Channel</th>
    </tr>
  </thead>
  {#if Object.keys(lcdata.samples).length}
  <tbody>
    {#each Object.values($datasetFiles) as file}
    {#if file.associd in lcdata.samples}
    <tr>
      <td>{file.name}</td>
      <td>
        <div class="field">
          <div class="control">
            {#if lcdata.quanttype}
            <DynamicSelect bind:intext={lcdata.samples[file.associd].channelname} niceName={x=>x.name} bind:fixedoptions={lcdata.quants[lcdata.quanttype].chans} bind:fixedorder={lcdata.quants[lcdata.quanttype].chanorder} bind:selectval={lcdata.samples[file.associd].channel} />

            {/if}
          </div>
        </div>
      </td>
    </tr>
    {/if}
    {/each}
  </tbody>
  {/if}
</table>

<button class="button is-small is-danger has-text-weight-bold" disabled={!edited} on:click={save}>Save</button>
<button class="button is-small is-info has-text-weight-bold" disabled={!edited} on:click={fetchData}>Revert</button>
