<script>

import * as Plot from '@observablehq/plot';
import { closeProject } from '../../home/src/util.js'
import { flashtime } from '../../util.js'
import { postJSON, getJSON } from '../../datasets/src/funcJSON.js'
import Table from '../../home/src/Table.svelte';

export let notif;

// Proj table
let loadedProjects = {};
$: {
    if (Object.keys(loadedProjects).length) {
      replot(plot_sort_by)
    }
}
let selectedProjs = false;
let addItem;
const inactive = ['inactive'];

let plots;
let plot_sort_asc = true;
let plot_sort_by = 'owner';

const sortkeys = {start: 'Start date',
  end: 'End date',
  owner: 'User',
  duration: 'Duration',
  stage: 'Current/last stage',
}
  

function showDetails(event) {
  detailsVisible = event.detail.ids;
}

const actionmap = {
  close: closeProject,
}
async function doAction(action, projid) {
  await actionmap[action](projid, loadedProjects, notif);
  loadedProjects = loadedProjects;
  updateNotif();
}

function updateNotif() {
  Object.entries(notif.errors)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg));
  Object.entries(notif.messages)
    .filter(x => x[1])
    .forEach(([msg,v]) => setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg));
  notif = notif;
}

function showError(error) {
  notif.errors[error] = 1;
  setTimeout(function(msg) { notif.errors[error] = 0 } , flashtime, error);
}



async function getProjDetails(projid) {
	const resp = await getJSON(`/show/project/${projid}`);
  return `
    <p><span class="has-text-weight-bold">Storage amount:</span> ${resp.stored_total_xbytes}</p>
    <p><span class="has-text-weight-bold">Owners:</span> ${resp.owners.join(', ')}</p>
    <hr>
    ${Object.entries(resp.nrstoredfiles).map(x => {return `<div>${x[1]} stored files of type ${x[0]}</div>`;}).join('')}
    <div>Instrument(s) used: <b>${resp.instruments.join(', ')}</b></div>
    `;
}
const tablefields = [
  {id: 'name', name: 'Name', type: 'str', multi: false},
  {id: 'ptype', name: 'Type', type: 'str', multi: false},
  {id: 'dsets', name: 'Datasets', type: 'count', count: 'dset_ids', multi: false},
  {id: 'start', name: 'Registered', type: 'str', multi: false},
  {id: 'lastactive', name: 'Last active', type: 'str', multi: false},
  {id: 'actions', name: '', type: 'button', multi: true, confirm: ['close']},
];


async function replot(sortkey) {
  const url = 'dashboard/projects/'
  const active_projid = Object.entries(loadedProjects)
    .map(kv => kv[0]);
  const resp  = await postJSON(url, {proj_ids: active_projid});
  if (!resp.ok) {
    showError(resp.error);
    return;
  }

  let perProject = resp.per_proj.map(x => Object.assign(x, {
    start: new Date(x.start),
    startdset: new Date(x.startdset),
    end: new Date(x.end),
    state: x.open ? 'Open' : 'Closed',
  })); 
  perProject = perProject.map(x => Object.assign(x, {
    // Bin projects start and end by month for aggregate plot
    monthend: (parseInt(`${x.end.getFullYear()}00`) + x.end.getMonth()+1).toString(),
    monthstart: (parseInt(`${x.start.getFullYear()}00`) + x.start.getMonth()+1).toString(),
    monthstartdset: (parseInt(`${x.startdset.getFullYear()}00`) + x.startdset.getMonth()+1).toString(),
  }))
  
  const today = new Date(resp.today);
  const firstdate = new Date(resp.firstdate);

  // Sorting: switch asc / desc when pressing sort button twice
  plot_sort_asc = plot_sort_by === sortkey ? plot_sort_asc === false : plot_sort_asc;
  const sort_order = `${{true: 'a', false: 'de'}[plot_sort_asc]}scending`;
  plot_sort_by = sortkey;
  const bar_to_sort = ['owner', 'end', 'stage'].indexOf(sortkey) > -1 ? 'last' : 'first';
  let individual_proj_plot;
  let aggregate_plot;

  try {
    individual_proj_plot = Plot.plot({
          marginRight: 130,
          marginRight:150,
          axis: null,
          color: {
                 legend: true,
                  opacity: 0.3,
              },
          x: {
                axis: "top",
                grid: true,
              },
          marks: [
                Plot.barX(perProject, {
                        x1: "start",
                        x2: "end",
                        y: "dset",
                        fill: "stage",
                        opacity: 0.3,
                        sort: {
                          y: '-data',
                          reduce: (D) => D.find((d) => d[bar_to_sort])?.[sortkey],
                          order: sort_order,
                        },
                      }),
                Plot.axisY({
                  text: (y) => perProject.find((d) => d[bar_to_sort] && d.dset === y)?.owner,
                  tickSize: 0,
                  anchor: 'right',
                  label: null,
                }),
                Plot.text(perProject, {
                  filter: (d) => d.first,
                        x: firstdate,
                        y: "dset",
                        text: 'proj',
                        textAnchor: "start",
                        dx: 5,
                      }),

                Plot.text(perProject, {
                  filter: (d) => d.first && resp.closedates[d.pid],
                    x: (d) => new Date(resp.closedates[d.pid]),
                    y: "dset",
                    //U+2705
                    text: (d) => '✅',
                    }),
                Plot.text(perProject, {
                  filter: (d) => d.first && !resp.closedates[d.pid],
                    x: (d) => today,
                    y: "dset",
                    //U+23F1
                    text: (d) => '⏳',
                    dx: -10,
                    }),
              ]
      })
  const colorscale = individual_proj_plot.scale('color');

   const xlabels = {duration: 'Days',
     start: 'Month',
     end: 'Month',
     owner: null,
     stage: null,
   }
   const ylabels = {
     duration: 'Datasets (stacked)',
     start: 'Datasets',
     end: 'Datasets',
     owner: 'Datasets (stacked)',
     stage: 'Datasets',
   }
  const bar_to_aggregate = ['owner', 'end', 'start', 'stage'].indexOf(sortkey) > -1 ? 'last' : 'first';
  const agg_plotdata = perProject.filter(d => d[bar_to_aggregate]);
  let sortkey_mod = ['stage', 'owner'].indexOf(sortkey) > -1 ? sortkey : `month${sortkey}`;
  sortkey_mod = sortkey_mod === 'monthstart' ? 'monthstartdset' : sortkey_mod;
  const rotation = ['stage', 'owner'].indexOf(sortkey) > -1 ? 0 : 90;

   if (['start', 'end', 'owner', 'stage'].indexOf(sortkey) > -1) {
     aggregate_plot = Plot.plot({
            marginBottom: 75,
            color: colorscale,
            y: { label: ylabels[sortkey], grid: true },
            x: { axis: null},
            fx: {axis: 'bottom', tickRotate: rotation, label: xlabels[sortkey]},
            marks: [
                  Plot.barY(agg_plotdata,
                    Plot.groupX({y: 'count'}, {fx: sortkey_mod, x: 'state', fill: 'stage', opacity: 0.3}),
                  ),
                  Plot.text(agg_plotdata,
                    Plot.groupX({text: "first", y: "count"}, {
                      fx: sortkey_mod,
                      x: 'state',
                      text: (d) => d.open ? '⏳' : '✅',
                      fontSize: 20,
                      dy: -15,
                    }),
                  ),
            Plot.ruleY([0]),
                ]
     })

   } else if (sortkey === 'duration') {
 // bin plot

     aggregate_plot= Plot.plot({
            color: {
                   legend: true,
                    opacity: 0.3,
                },
            y: { label: 'Datasets', grid: true },
            x: {label: xlabels[sortkey]},
            marks: [
                  Plot.barY(agg_plotdata,
                    Plot.binX({y: 'count'},
                      {x: {thresholds: 20, value: sortkey}, fill: 'state', opacity: 0.3}),
                  ),
                  Plot.ruleY([0]),
            ]
     })
    }

  } catch (error) {
    console.log(error);
    showError(`Some error occurred: ${error}`);
  }
  
  if (plots && individual_proj_plot) {
    // plots is null when you switch tabs to Protocols e.g., so if(plots ...
    while (plots.firstChild) {
      plots?.lastChild?.remove();
    }
    plots?.append(individual_proj_plot);
    plots?.append(aggregate_plot);
  }
}
</script>

<div class="box" id="plots">
  <h4 class="title is-4">Projects</h4>
  <span class="is-size-7">Sort and aggregate on: </span>
  {#each Object.entries(sortkeys) as [key, txt] }
  <button class={`${plot_sort_by === key ? 'is-focused' : ''} button is-small`} on:click={e => replot(key)}>{txt}</button>

  {/each}

  <div style="display: flex" bind:this={plots}></div>
</div>

<Table tab="Dashboard" bind:addItem={addItem}
  bind:notif={notif}
  bind:selected={selectedProjs}
  bind:items={loadedProjects}
  fetchUrl="/show/projects"
  findUrl="/show/projects"
  show_deleted_or_q="type:cf, from:2025, to:20250801, from:202504, active:true/false/yes/no, user:username"
  defaultQ="type:cf active:true"
  getdetails={getProjDetails}
  fixedbuttons={[]}
  fields={tablefields}
  inactive={inactive}
  on:detailview={showDetails}
  allowedActions={Object.keys(actionmap)}
  on:rowAction={e => doAction(e.detail.action, e.detail.id)}
  />

