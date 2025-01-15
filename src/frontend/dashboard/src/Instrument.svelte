<script>
import { schemeSet1 } from 'd3-scale-chromatic';
import { createEventDispatcher } from 'svelte';
import * as Plot from '@observablehq/plot';


import DateSlider from './DateSlider.svelte';
import { getJSON } from '../../datasets/src/funcJSON.js'

const dispatch = createEventDispatcher();

let firstday = 0;
let maxdays = 30;

let identplot;
let psmplot;
let fwhmplot;
let pepms1plot;
let ionmobplot;
let scoreplot;
let rtplot;
let perrorplot;
let acquisistionmode;

let qcdata = {
  ident: {data: false, func: linePlot, div: identplot, title: 'Nr of IDs'},
  psms: {data: false, func: linePlot, title: 'Scans and PSMs', div: psmplot},
  fwhm: {data: false, func: linePlot, div: fwhmplot, title: 'Average FWHM'},
  precursorarea: {data: false, func: linePlotWithQuantiles, div: pepms1plot, title: 'Peptide MS1 area'},
  prec_error: {data: false, func: linePlotWithQuantiles, div: perrorplot, title: 'PSM precursor error (ppm)'},
  rt: {data: false, func: linePlotWithQuantiles, div: rtplot, title: 'PSM retention time (min)'},
  score: {data: false, func: linePlotWithQuantiles, div: scoreplot, title: 'PSM scores'},
  ionmob: {data: false, func: linePlotWithQuantiles, div: ionmobplot, title: 'PSM ion mobility'},
}
let plotlist = ['ident', 'psms', 'fwhm', 'precursorarea', 'prec_error', 'rt', 'score', 'ionmob'];
export let instrument_id;

let acqmode = 'ALL';

export async function loadData(maxdays, firstday) {
  const url = new URL(`/dash/longqc/${instrument_id}/${acqmode}/${firstday}/${maxdays}`, document.location);
  const result = await getJSON(url);
  acqmode = result.runtype;
  for (let key in result.data) {
    qcdata[key].data = result.data[key];
  }
  for (let [name, p] of Object.entries(qcdata)) {
    if (p.div) {
      p.div.replaceChildren();
    }
    if (p.data && p.data.length) {
      p.func(p.div, p.data, p.title)
    }
  }
  plotlist = plotlist.filter(x => qcdata[x].data).concat(plotlist.filter(x => !qcdata[x].data))
}


function toggleAcqMode() {
  acqmode = acqmode === 'DIA' ? 'DDA': 'DIA';
  firstday = 0;
  maxdays = 30;
  loadData(firstday, maxdays);
}

function linePlot(plotdiv, data, title) {
//  try {
    let theplot;
    theplot = Plot.plot({
      title: title,
      width: plotdiv.offsetWidth - 20,
      //x: {axis: null},
      y: {tickFormat: 's', type: 'log', grid: true, }, // scientific ticks
      marks: [Plot.line(data, {
        x: d => new Date(d.date),
        y: 'value',
        stroke: 'name',
        //y: (d) => `${d.mod}_${d.cname}`,
        //fx: (d) => fetched.experiments[d.exp],
        //fill: 'seq',
      }),
     //   Plot.tip(fetched.samples, Plot.pointer({
     //     x: (d) => `${d.mod}_${d.cname}`,
     //     fx: (d) => fetched.experiments[d.exp],
     //     maxRadius: 200,
     //     title: (d) => [d.seq, '', formatModseq(d, fetched.modifications),
     //       fetched.experiments[d.exp],
     //       `${fetched.conditions[d.ctype]}: ${d.cname}`, `MS1: ${d.ms1}`, ].join('\n')}))
      ]
    });
    plotdiv?.append(theplot);
//  } catch (error) {
//    console.log('error');
//    //errors.push(`For MS1 plots: ${error}`);
//  }
}


function linePlotWithQuantiles(plotdiv, data, title) {
//  try {
    let theplot;
    theplot = Plot.plot({
      title: title,
      width: plotdiv.offsetWidth - 20,
      //x: {axis: null},
      y: {tickFormat: 's', type: 'log', grid: true, }, // scientific ticks
      marks: [
        Plot.line(data, {
          x: d => new Date(d.date),
          y: 'q2',
          stroke: 'name',
        }),
        Plot.line(data, {
          x: d => new Date(d.date),
          y: 'q1',
          stroke: 'name',
          strokeOpacity: 0.5,
        }),
        Plot.line(data, {
          x: d => new Date(d.date),
          y: 'q3',
          stroke: 'name',
          strokeOpacity: 0.5,
        }),
        Plot.areaY(data, {
          x: d => new Date(d.date),
          y1: 'q1',
          y2: 'q3',
          fill: 'name',
          opacity: 0.2,
        })
     //   Plot.tip(fetched.samples, Plot.pointer({
     //     x: (d) => `${d.mod}_${d.cname}`,
     //     fx: (d) => fetched.experiments[d.exp],
     //     maxRadius: 200,
     //     title: (d) => [d.seq, '', formatModseq(d, fetched.modifications),
     //       fetched.experiments[d.exp],
     //       `${fetched.conditions[d.ctype]}: ${d.cname}`, `MS1: ${d.ms1}`, ].join('\n')}))
      ]
    });
    plotdiv?.append(theplot);
//  } catch (error) {
//    console.log('error');
//    //errors.push(`For MS1 plots: ${error}`);
//  }
}

</script>

<div>
  <DateSlider bind:daysago={firstday} bind:maxdays={maxdays} on:updatedates={e => loadData(e.detail.showdays, e.detail.firstday)} />
  <div class="tabs is-toggle is-centered is-small">
    <ul>
      <li class={acqmode === 'DIA' ? 'is-active' : ''}>
        {#if acqmode === 'DDA'}
        <a on:click={toggleAcqMode}><span>DIA</span></a>
        {:else}
        <a><span>DIA</span></a>
        {/if}
      </li>
      <li class={acqmode === 'DDA' ? 'is-active' : '' }>
        {#if acqmode === 'DIA'}
        <a on:click={toggleAcqMode}><span>DDA</span></a>
        {:else}
        <a><span>DDA</span></a>
        {/if}
      </li>
    </ul>
  </div>
  <hr>
  
  {#each plotlist as pname, index}
  {#if index % 2}
  {:else if qcdata[pname].data}
  <div class="tile is-ancestor">
    <div class="tile" bind:this={qcdata[pname].div}>
    </div>
    {#if qcdata[plotlist[index+1]].data}
    <div class="tile" bind:this={qcdata[plotlist[index+1]].div}> </div>
    {/if}
  </div>
  {/if}
  {/each}
</div>
