<script>
import { schemeSet1 } from 'd3-scale-chromatic';
import { createEventDispatcher } from 'svelte';
import * as Plot from '@observablehq/plot';


import BoxPlot from './BoxPlot.svelte';
import LinePlot from './LinePlot.svelte';
import DateSlider from './DateSlider.svelte';

const dispatch = createEventDispatcher();

export let qcdata;
export let instrument_id;


let identplot;
let psmplot;
let fwhmplot;
let pepms1plot;
let ionmobplot;
let scoreplot;
let rtplot;
let perrorplot;

function reloadData(maxdays, firstday) {
  dispatch('reloaddata', {instrument_id: instrument_id, showdays: maxdays, firstday: firstday});
}

function linePlot(plotdiv, data, title) {
  ////// test this
  plotdiv.replaceChildren();
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
    console.log('done');
//  } catch (error) {
//    console.log('error');
//    //errors.push(`For MS1 plots: ${error}`);
//  }
}


function linePlotWithQuantiles(plotdiv, data, title) {
  plotdiv.replaceChildren();
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
    console.log('done');
//  } catch (error) {
//    console.log('error');
//    //errors.push(`For MS1 plots: ${error}`);
//  }
}



export function parseData() {
  qcdata.psms.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.psms.series = new Set(qcdata.psms.data.map(d => Object.keys(d)).flat());
  qcdata.ident.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.ident.series = new Set(qcdata.ident.data.map(d => Object.keys(d)).flat());
  qcdata.fwhm.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.precursorarea.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.ionmob.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.msgfscore.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.rt.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  qcdata.prec_error.data.map(d => Object.assign(d, d.day = new Date(d.day)));
  setTimeout(() => {
    linePlot(identplot, qcdata.ident.data, 'Nr of IDs');
    linePlot(psmplot, qcdata.psms.data, 'Scans and PSMs');
    // fwhmplot.plot();
    linePlotWithQuantiles(pepms1plot, qcdata.precursorarea.data, 'Peptide MS1 area');
    linePlotWithQuantiles(rtplot, qcdata.rt.data, 'PSM retention time');

    linePlotWithQuantiles(scoreplot, qcdata.msgfscore.data, 'PSM scores');
    linePlotWithQuantiles(perrorplot, qcdata.prec_error.data, 'Precursor error (ppm)');
    //ionmobplot.plot();
  }, 0);

}
        // <LinePlot bind:this={identplot} colorscheme={schemeSet1} data={qcdata.ident.data} series={qcdata.ident.series} xkey={qcdata.ident.xkey} xlab="Date" ylab="Amount PSMs/scans" />

        //<h5 class="title is-5"># PSMs</h5>
        //<LinePlot bind:this={psmplot} colorscheme={schemeSet1} data={qcdata.psms.data} series={qcdata.psms.series} xkey={qcdata.psms.xkey} xlab="Date" ylab="Amount" />
        //<BoxPlot bind:this={pepms1plot} colorscheme={schemeSet1} data={qcdata.precursorarea.data} xkey={qcdata.precursorarea.xkey} xlab="Date" ylab="" />
</script>

<div>
  <DateSlider on:updatedates={e => reloadData(e.detail.showdays, e.detail.firstday)} />
  <hr>
  
  <div class="tile is-ancestor">
    <div class="tile" bind:this={identplot}>
    </div>
    <div class="tile" bind:this={psmplot}>
    </div>
  </div>
  <hr>
  <div class="tile is-ancestor">
    <div class="tile" bind:this={pepms1plot} >
    </div>
    <div class="tile" bind:this={perrorplot} >
    </div>
  </div>
  <hr>
  <div class="tile is-ancestor">
    <div class="tile" bind:this={rtplot}>
    </div>
    <div class="tile" bind:this={scoreplot} >
    </div>
  </div>
  <hr>
  <div class="tile is-ancestor">
    <div class="tile">
        <h5 class="title is-5">Ion mobility</h5>
    </div>
  </div>
</div>
