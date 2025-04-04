<script>
import { schemeSet1 } from 'd3-scale-chromatic';
import { onMount, createEventDispatcher } from 'svelte';
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
let ioninfplot;
let peaksfwhmplot;
let matchedpeaksplot;
let miscleavplot;
let mcratioplot;

let acquisistionmode;

let seriesmap = {line: {}, box: {}};

let qcdata = {
  ident: {data: false, func: linePlot, div: identplot, title: 'Nr of IDs', ylab: ''},
  psms: {data: false, func: linePlot, title: 'Scans and PSMs', div: psmplot, ylab: ''},
  miscleav: {data: false, func: linePlot, title: 'Missed cleavages', div: miscleavplot, ylab: '#PSMs', denom: 'psms'},
  mcratio: {data: false, func: linePlot, title: 'Missed cleavages ratio', div: mcratioplot, ylab: '#PSMs w mc / total PSMs'},
  PEAKS_FWHM: {data: false, func: linePlot, title: 'peaks/FWHM', div: peaksfwhmplot, ylab: 'Avg # peaks'},
  MATCHED_PEAKS: {data: false, func: linePlotWithQuantiles, title: 'MS2 peaks matched', div: matchedpeaksplot, ylab: '#peaks / MS2'},
  FWHM: {data: false, func: linePlotWithQuantiles, div: fwhmplot, title: 'FWHM', ylab: 'min'},
  PEPMS1AREA: {data: false, func: linePlotWithQuantiles, div: pepms1plot, title: 'Peptide MS1 area', ylab: ''},
  MASSERROR: {data: false, func: linePlotWithQuantiles, div: perrorplot, title: 'PSM precursor error', ylab: 'ppm'},
  RT: {data: false, func: linePlotWithQuantiles, div: rtplot, title: 'PSM retention time', ylab: 'min'},
  SCORE: {data: false, func: linePlotWithQuantiles, div: scoreplot, title: 'PSM scores', ylab: 'Sage score'},
  IONMOB: {data: false, func: linePlotWithQuantiles, div: ionmobplot, title: 'PSM ion mobility', ylab: 'eV'},
  IONINJ: {data: false, func: linePlotWithQuantiles, div: ioninfplot, title: 'Ion injection time', ylab: 'min'},
}

let extrapeps;

let plotlist = ['ident', 'psms', 'miscleav', 'mcratio', 'MASSERROR', 'RT', 'SCORE', 'FWHM', 'PEPMS1AREA', 
  'PEAKS_FWHM', 'MATCHED_PEAKS', 'IONMOB', 'IONINJ'];

export let instrument_id;
let acqmode = 'ALL';


async function refresh(maxdays, firstday) {
  await loadData(maxdays, firstday);
  renderPlots();
}


export function renderPlots() {
  for (let [name, p] of Object.entries(qcdata)) {
    if (p.div) {
      p.div.replaceChildren();
    }
    if (p.data && p.data.length) {
      // Timeout is needed or it will plot too fast or something,
      // and the plots will not be shown
      setTimeout(() => {
        if (p.extradata) {
          p.func(p.div, p.data, p.title, p.ylab, p.extradata);
        } else {
          p.func(p.div, p.data, p.title, p.ylab);
        }
      }, 0);
    }
  }
}


export async function loadData(maxdays, firstday) {
  const url = new URL(`/dash/longqc/${instrument_id}/${acqmode}/${firstday}/${maxdays}`, document.location);
  const result = await getJSON(url);
  acqmode = result.runtype;
  seriesmap = result.seriesmap;
  for (let key in result.data) {
    qcdata[key].data = result.data[key];
    qcdata[key].data.map(d => Object.assign(d, d.date = new Date(d.date)));
    if ('extradata' in result && result.extradata[key]) {
      qcdata[key].extradata = result.extradata[key];
      qcdata[key].extradata.map(d => Object.assign(d, d.date = new Date(d.date)));
    } else {
      qcdata[key].extradata = false;
    }
  }
  extrapeps = result.extradata.peps;
  plotlist = plotlist.filter(x => qcdata[x].data).concat(plotlist.filter(x => !qcdata[x].data))
}


function toggleAcqMode() {
  acqmode = acqmode === 'DIA' ? 'DDA': 'DIA';
  firstday = 0;
  maxdays = 30;
  refresh(maxdays, firstday);
}

function linePlot(plotdiv, data, title, ylabel, denom) {
//  try {
    let theplot;
    theplot = Plot.plot({
      color: {legend: true},
      title: title,
      width: plotdiv.offsetWidth - 20,
      //y: {tickFormat: 's', type: 'log', grid: true, }, // scientific ticks
      marks: [
        Plot.axisY({label: ylabel}),
        Plot.line(data, {
        x: 'date',
        y: 'value',
        stroke: (d) => seriesmap.line[d.key],
      }),
        Plot.tip(data, Plot.pointerX({
          x: 'date',
          y: 'value',
          lineWidth: 100,
          title: (d) => `${d.date}\n
${seriesmap.line[d.key]}: ${d.value}\n
${seriesmap.fns[d.run]}`,
        })),
      ],
    });
    plotdiv?.append(theplot);
//  } catch (error) {
//    //errors.push(`For MS1 plots: ${error}`);
//  }
}


function linePlotWithQuantiles(plotdiv, data, title, ylabel, extralines) {
//  try {
    let theplot;
    let quantile_marks = [
        Plot.line(data, {
          x: 'date',
          y: 'q2',
          stroke: 'lightgreen',
        }),
        Plot.line(data, {
          x: 'date',
          y: 'q1',
          stroke: 'lightgreen',
          strokeOpacity: 0.5,
        }),
        Plot.line(data, {
          x: 'date',
          y: 'q3',
          stroke: 'lightgreen',
          strokeOpacity: 0.5,
        }),
        Plot.areaY(data, {
          x: 'date',
          y1: 'q1',
          y2: 'q3',
          fill: 'lightgreen',
          opacity: 0.2,
        }),
        Plot.tip(data, Plot.pointer({
          x: 'date',
          y: 'q2',
          title: (d) => `${d.date}\n
${seriesmap.box[d.key]}: ${d.q2}\n
${seriesmap.fns[d.run]}`,
        })),
    ];
    // extralines = {data: [{date, pepid, value}...], peps: {pep_id: IAMAPEP, ...}}
    if (extralines) {
      let extraplots = [
        Plot.line(extralines, {
          x: 'date',
          y: 'value',
          stroke: 'pepid',
        }),
        Plot.tip(extralines, Plot.pointer({
          x: 'date',
          y: 'value',
          title: (d) => `${d.date}\n
${extrapeps[d.pepid]}\n
${d.value}\n
${seriesmap.fns[d.run]}`,
          })),
        ];
      quantile_marks.push(...extraplots);
    }

    theplot = Plot.plot({
      title: title,
      subtitle: '0.25/0.75 quantile range',
      width: plotdiv.offsetWidth - 20,
      //x: {axis: null},
      //y: {tickFormat: 's', type: 'log', grid: true, }, // scientific ticks
      marks: [
        Plot.axisY({label: ylabel, tickFormat: (d) => (d > 1e6 ? `${d/1e6}M` : d)}),
        ...quantile_marks,
      ]
    });
    plotdiv?.append(theplot);
//  } catch (error) {
//    console.log('error');
//    //errors.push(`For MS1 plots: ${error}`);
//  }
}

onMount(async() => {
  if (!qcdata.ident.data.length) {
    loadData(maxdays, firstday);
  }
})


</script>

<div>
  <DateSlider bind:daysago={firstday} bind:maxdays={maxdays} on:updatedates={e => refresh(e.detail.showdays, e.detail.firstday)} /> <div class="tabs is-toggle is-centered is-small">
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
    {#if plotlist.length - 1 > index && qcdata[plotlist[index+1]].data}
    <div class="tile"  bind:this={qcdata[plotlist[index+1]].div}> </div>
    {/if}
  </div>
  <hr>
  {/if}
  {/each}
</div>
