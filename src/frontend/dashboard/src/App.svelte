<script>
import { onMount } from 'svelte';
import { schemeSet1 } from 'd3-scale-chromatic';

import Instrument from './Instrument.svelte'
import StackedPlot from './StackedPlot.svelte';
import GroupedBarPlot from './GroupedBarPlot.svelte'
import DateSlider from './DateSlider.svelte';


let prodplot;
let cfprodplot;
let projdistplot;
let projageplot;

let instrumenttabs = {};

let firstday = 0;
let maxdays = 30;
let tabshow = 'prod';


let proddata = {
  fileproduction: {},
  projecttypeproduction: {},
  projectdistribution: {},
  projectage: {},
};

async function showInst(iid) {
  tabshow = `instr_${iid}`;
  instrumenttabs[iid].renderPlots();
}

function showProd() {
  tabshow = 'prod';
}

async function fetchProductionData(maxdays, firstday) {
  // maxdays and firstday are explicit arguments, because the binding
  // to them in the dateslider does not update fast enough
  const resp = await fetch(`/dash/proddata/${firstday}/${maxdays}`);
  proddata = await resp.json();
  // setTimeout since after fetching, the plot components havent updated its props
  proddata.fileproduction.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  proddata.fileproduction.instruments = new Set(proddata.fileproduction.data.map(d => Object.keys(d)).flat());
  proddata.projectage.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  proddata.projectage.ptypes= new Set(proddata.projectage.data.map(d => Object.keys(d)).flat());
  proddata.projecttypeproduction.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  proddata.projecttypeproduction.projtypes= new Set(proddata.projecttypeproduction.data.map(d => Object.keys(d)).flat());
  proddata.projectdistribution.ptypes = new Set(proddata.projectdistribution.data.map(d => Object.keys(d)).flat());
  setTimeout(() => {
    prodplot.plot();
    cfprodplot.plot();
    projdistplot.plot();
    projageplot.plot();
  }, 0);
}

onMount(async() => {
  fetchProductionData(maxdays, firstday);
})
</script>

<style>
.instrplot.inactive {
  display: none;
}
</style>

<div class="tabs is-toggle is-centered is-small">
	<ul>
    <li class={tabshow === `prod` ? 'is-active' : ''}>
      <a on:click={showProd}><span>Production</span></a>
    </li>
    {#each instruments as instr}
    <li class={tabshow === `instr_${instr[1]}` ? 'is-active' : '' }>
      <a on:click={e => showInst(instr[1])}><span>{instr[0]}</span></a>
    </li>
    {/each}
	</ul>
</div>
<div class="container">
  <section>
    {#each instruments as instr}
    <div class={`instrplot ${tabshow === `instr_${instr[1]}` ? 'active' : 'inactive'}`} >
      <Instrument bind:this={instrumenttabs[instr[1]]} bind:instrument_id={instr[1]} />
    </div>
    {/each}
    <div class={`instrplot ${tabshow === `prod` ? 'active' : 'inactive'}`} >
      <DateSlider bind:firstday={firstday} maxdays={maxdays}
          on:updatedates={e => fetchProductionData(e.detail.showdays, e.detail.firstday)} />
      <hr>
      <div class="tile is-ancestor">
        <div class="tile">
          <div class="content">
<h5 class="title is-5">Raw file production per instrument</h5>
            <StackedPlot bind:this={prodplot} colorscheme={schemeSet1} data={proddata.fileproduction.data} stackgroups={proddata.fileproduction.instruments} xkey={proddata.fileproduction.xkey} xlab="Date" ylab="Raw files (GB)" />
          </div>
        </div>
        <div class="tile">
          <div class="content">
<h5 class="title is-5">Raw file production per project type</h5>
            <StackedPlot bind:this={cfprodplot} colorscheme={schemeSet1} data={proddata.projecttypeproduction.data} stackgroups={proddata.projecttypeproduction.projtypes} xkey={proddata.projecttypeproduction.xkey} xlab="Date" ylab="Raw files (GB)" />
          </div>
        </div>
      </div>
      <div class="tile is-ancestor">
        <div class="tile">
          <div class="content">
            <h5 class="title is-5">Active project size distribution</h5>
            <GroupedBarPlot bind:this={projdistplot} colorscheme={schemeSet1} data={proddata.projectdistribution.data} groups={proddata.projectdistribution.ptypes} xkey={proddata.projectdistribution.xkey} ylab="# Projects" xlab="Raw files (GB)" />
          </div>
        </div>
        <div class="tile">
          <div class="content">
            <h5 class="title is-5">Project sizes by last-activity date</h5>
            <StackedPlot bind:this={projageplot} colorscheme={schemeSet1} data={proddata.projectage.data} stackgroups={proddata.projectage.ptypes} xkey={proddata.projectage.xkey} xlab="Date" ylab="Project size (B)" />

          </div>
        </div>
      </div>
      
    </div>
	</section>
</div>
