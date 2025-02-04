<script>

import { onMount } from 'svelte';
import { flashtime, statecolors, helptexts } from '../../util.js'
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import DynamicSelect from '../../datasets/src/DynamicSelect.svelte';
import TokenInstructions from '../../file-inflow/src/TokenInstructions.svelte';

let notif = {errors: {}, messages: {}, links: {}};
let loadingItems = false;
let runButtonActive = true;
let postingAnalysis = false;
let copiedToken = false;

let analysis_id = existing_analysis ? existing_analysis.analysis_id : false;

let wf = false;
let dsets = {};
let external_dsets = {};

let dsetToAdd;
let libfiles = {};
let libfnorder = [];
let fetched_resultfiles = [];
let prev_resultfiles = [];
let resfn_arr = [];
let resultfiles = {}
let resultfnorder = [];
let field_order = [];
let dsnames = initial_dsnames;

let base_analysis = {
  isComplement: false,
  dsets_identical: false,
  selected: false,
  typedname: '',
  resultfiles: [],
}

let adding_analysis = {
  selected: false,
  typedname: '',
}

let added_analyses_order = [];
let added_results = {};
if (existing_analysis && existing_analysis.added_results) {
  added_results = existing_analysis.added_results;
  added_analyses_order = Object.keys(existing_analysis.added_results);
}


function addDataset() {
  dsnames[dsetToAdd] = `Dataset ${dsetToAdd}`;
  if (wf) {
    fetchDatasetDetails([dsetToAdd]);
  } else {
    external_dsets[dsetToAdd] = 1;
  }
}


function removeDataset(dsid) {
  delete(dsnames[dsid]);
  dsnames = dsnames;
  if (dsid in dsets) {
    delete(dsets[dsid]);
    dsets = dsets;
  }
}


function updateResultfiles() {
  /* Adds result files that are fetched, also loads them from
  any resultfiles (prev_resultfiles) that are implicit to the analysis
  since they are from an analysis with the same datasets
  */
  fetched_resultfiles = added_analyses_order.flatMap(x => added_results[x].fns);
  resfn_arr = fetched_resultfiles.concat(base_analysis.resultfiles).concat(prev_resultfiles);
  resultfiles = Object.fromEntries(resfn_arr.map(x=> [x.id, {id: x.id,
    name: `${x.fn} - Result of ${x.ana} (${x.date})`}]));
  resultfnorder = resfn_arr.map(x => x.id);
}


let config = {
  wfid: false,
  wfversion: false,
  analysisname: '',
  editable: true,
  external_results: false,
  external_desc: '',
  jobstate: false,
  upload_token: false,
  flags: [],
  multicheck: [],
  isoquants: {}, // contain both isoquant and sampletable
  fileparams: {},
  inputparams: {},
  multifileparams: {},
}
let matchedFr = {};

function validate() {
  notif = {errors: {}, messages: {}, links: {}};
  const charRe = RegExp('^[a-z0-9_-]+$', 'i');
  if (!config.analysisname) {
    notif.errors['Analysisname must be filled in'] = 1;
  } else if (!charRe.test(config.analysisname)) {
    notif.errors['Analysisname may only contain a-z 0-9 _ -'] = 1;
  }
  if (!config.wfid && !config.external_results) {
    notif.errors['You must select a workflow'] = 1;
  }
  if (!config.wfversion && !config.external_results) {
		notif.errors['You must select a workflow version'] = 1;
	}
  if (!Object.keys(dsets).length && !config.external_results) {
    notif.errors['No datasets are in this analysis, maybe they need some editing'] = 1;
  }
  Object.values(dsets).forEach(ds => {
    if (!('LABELCHECK_ISO' in wf.components) && ds.allfilessamesample && !ds.setname) {
			notif.errors[`Dataset ${ds.proj} - ${ds.exp} - ${ds.run} needs to have a set name`] = 1;
    } else if (!ds.allfilessamesample) {
      if (ds.ft_files[ds.picked_ftype].some(fn => !fn.fields.__sample)) {
			  notif.errors[`File ${fn.name} needs to have a sample name`] = 1;
			}
    } else if (ds.setname && !charRe.test(ds.setname)) {
			notif.errors[`Dataset ${ds.proj} - ${ds.exp} - ${ds.run} needs to have another set name: only a-z 0-9 _ are allowed`] = 1;
		}
	});
  Object.entries(config.isoquants).forEach(([sname, isoq]) => {
    if (('ISOQUANT' in wf.components) && !isoq.report_intensity && !isoq.sweep &&
      !Object.entries(isoq.denoms).filter(([ch, val]) => !isoq.remove[ch]).map(x => x[1]).some(x=>x)) {
      notif.errors[`No denominator, sweep or intensity values are filled in for set ${sname}, or denominator
        is a removed channel`] = 1;
    }
    Object.entries(isoq.samplegroups).forEach(([ch, sgroup]) => {
      if (sgroup && !charRe.test(sgroup)) {
        notif.errors[`Incorrect sample group name for set ${sname}, channel ${ch}, only A-Z a-z 0-9 _ are allowed`] =1; 
      }
    })
  })

  if (!config.external_results) {
    wf.selectparams.forEach(selp => {
      if (!config.inputparams[selp.id]) {
        notif.errors[`Must select a value for ${selp.name}`] = 1;
        }
    });
  }
  return Object.keys(notif.errors).length === 0;
}


async function storeAnalysis() {
  if (!validate()) {
  	return false;
  }
  runButtonActive = false;
  postingAnalysis = true;
  let msg = 'Validated data';
  notif.messages[msg] = 1;
  setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
  let fns = Object.fromEntries(Object.entries(config.fileparams).filter(([k,v]) => v))
  let multifns = Object.fromEntries(Object.entries(config.multifileparams).map(([k, v]) => [k, Object.values(v).filter(x => x)]).filter(([k, v]) => v.length));

  msg = `Using ${Object.keys(dsets).length} dataset(s)`;
  notif.messages[msg] = 1;
  setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
  msg = `${Object.keys(fns).length} other inputfiles found`;
  notif.messages[msg] = 1;
  setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
  let post = {
    analysis_id: analysis_id,
    upload_external: config.external_results,
    external_description: config.external_desc,
    base_analysis: base_analysis,
    dsids: wf ? Object.keys(dsets) : Object.keys(external_dsets),
    dssetnames: Object.fromEntries(Object.entries(dsets)
      .filter(([x,ds]) => ds.allfilessamesample)
      .map(([dsid, ds]) => [dsid, ds.setname])),
    infiles: Object.fromEntries(Object.values(dsets)
      .flatMap(ds => ds.ft_files[ds.picked_ftype]
        .map(fn => [fn.id, {fr: fn.fr}]))),
    fnfields: Object.fromEntries(Object.entries(dsets)
      .filter(([x,ds]) => !ds.allfilessamesample)
      .map(([dsid, ds]) => ds.ft_files[ds.picked_ftype]
        .map(fn => [fn.id, fn.fields]))
      .flat()),
    picked_ftypes: Object.fromEntries(Object.entries(dsets)
      .map(([dsid, ds]) => [dsid, ds.picked_ftype])),
    dsetfields: Object.fromEntries(Object.entries(dsets)
      .map(([dsid, ds]) => [dsid, ds.fields])),
    singlefiles: fns,
    multifiles: multifns,
    components: {
      ISOQUANT: {},
      ISOQUANT_SAMPLETABLE: false,
    },
    wfid: config.wfid,
    nfwfvid: config.wfversion.id || false,
    analysisname: config.analysisname,
    // Most params are {param.id: value, param2.id: value}, but multicheck
    // is checkboxes and is thus {param.id: [value1.id, value2.id]}
    params: {
      flags: Object.fromEntries(config.flags.map(x => [x, true])),
      inputparams: config.inputparams,
      // Multicheck 
      multicheck: config.multicheck.reduce((acc, x) => {
        const xspl = x.split('___');
        acc[xspl[0]].push(xspl[1]);
        return acc},
        Object.fromEntries(config.multicheck.map(x => {
          const xspl = x.split('___');
          return [xspl[0], []]
        }))),
    },
  };
  if (wf && 'ISOQUANT' in wf.components) {
    post.components.ISOQUANT = config.isoquants;
  }

  if (wf && 'ISOQUANT_SAMPLETABLE' in wf.components) {
    // If data is purely labelfree this will be empty []
    const sampletable = Object.entries(config.isoquants).flatMap(([sname, isoq]) => 
      Object.entries(isoq.channels).map(([ch, sample]) => [ch, sname, sample[0], isoq.samplegroups[ch]]).sort((a, b) => {
      return a[0].replace('N', 'A') > b[0].replace('N', 'A')
      })
    );
    post.components.ISOQUANT_SAMPLETABLE = sampletable;
  }
   
  // Post the payload
  if (!Object.entries(notif.errors).filter(([k,v]) => v).length) {
    msg = `Storing analysis for ${config.analysisname}`;
    notif.messages[msg] = 1;
    setTimeout(function(msg) { notif.messages[msg] = 0 } , flashtime, msg);
    const resp = await postJSON('/analysis/store/', post);
    if (resp.error.length) {
      let msg = 'Errors found, please fix and try again';
      notif.errors[msg] = 1;
      resp.error.forEach(msg => {
        notif.errors[msg] = 1;
      });
      if ('files_nods' in resp) {
        // Dsets have been changed while editing analysis
        const files_nodset = new Set(resp.files_nods);
        Object.values(dsets)
          .map(ds => [ds.id, Object.values(ds.ft_files[ds.picked_ftype])
            .filter(x => files_nodset.has(x.id))
          ]).filter(dstuple => dstuple[1].length)
          .forEach(dstuple => {
            dsets[dstuple[0]].changed = true;
        });
        Object.entries(dsets).filter(([dsid, ds]) => resp.ds_newfiles.indexOf(dsid) > -1).forEach(([dsid, ds]) => {
          ds.changed = true;
        });
      }
    } else {
      analysis_id = resp.analysis_id;
      history.pushState({}, '', `/analysis/${analysis_id}/`);
      config.upload_token = resp.token;
    }
  }
  postingAnalysis = false;
  runButtonActive = true;
}


async function runAnalysis() {
  await storeAnalysis();
  if (!Object.entries(notif.errors).filter(([k,v]) => v).length) {
    notif.messages[`Queueing analysis job for ${config.analysisname}`] = 1;
    const post = {
      analysis_id: analysis_id,
    }
    const resp = await postJSON('/analysis/start/', post);
    if (resp.error) {
      notif.errors[resp.error] = 1;
    } else {
      window.location.href = '/#/analyses/';
    }
  }
}


async function freezeAnalysis() {
  await storeAnalysis();
  if (!Object.entries(notif.errors).filter(([k,v]) => v).length) {
    notif.messages[`Freezing analysis ${config.analysisname}`] = 1;
    const post = {
      analysis_id: analysis_id,
    }
    const resp = await postJSON('/analysis/freeze/', post);
    if (resp.error) {
      notif.errors[resp.error] = 1;
    } else {
      config.editable = false;
    }
  }
}


async function unFreezeAnalysis() {
  const post = {
    analysis_id: analysis_id,
  }
  const resp = await postJSON('/analysis/unfreeze/', post);
  if (resp.error) {
    notif.errors[resp.error] = 1;
  } else {
    config.editable = true;
  }
}

async function fetchWorkflow() {
  /* Fetch all info about a chosen workflow. Passes dsids as param
  to get input files that are results of any previous workflow that has
  the same datasets as input
  */
  notif = {errors: {}, messages: {}, links: {}};
  let url = new URL('/analysis/workflow', document.location)
  const params = {dsids: Object.keys(dsnames).join(','), wfvid: config.wfversion.id};
  url.search = new URLSearchParams(params).toString();
  const result = await getJSON(url);
  loadingItems = true;
  if ('error' in result) {
    const msg = `While fetching workflow versions, encountered: ${result.error}`;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
// FIXME cant this be done in backend?
    libfiles = Object.fromEntries(Object.entries(result.wf.libfiles).map(([ft, lf]) => [ft, Object.fromEntries(lf.map(x => [x.id, x]))]));
    libfnorder = Object.fromEntries(Object.entries(result.wf.libfiles).map(([ft, lf]) => [ft, lf.map(x => x.id)]));
    prev_resultfiles = result.wf.prev_resultfiles;
    wf = result.wf;
  }
  if (wf.fileparams.length) {
    Object.keys(wf.fileparams).forEach(k => {
      wf.fileparams[k].component = false;
    });
  }
  if (wf.multifileparams.length) {
    Object.keys(wf.multifileparams).forEach(k => {
      wf.multifileparams[k].components = [];
    });
    config.multifileparams = Object.assign(config.multifileparams, Object.fromEntries(wf.multifileparams.filter(x => !(x.id in config.multifileparams)).map(x => [x.id, {0: ''}])));
  }
  if (wf.selectparams.length) {
    wf.selectparams.forEach(x => {
      config.inputparams[x.id] = false;
    });
  }
  updateResultfiles();
  if (Object.keys(dsnames).length) {
    await fetchDatasetDetails(false);
  }
}


async function fetchDatasetDetails(fetchdsids) {
  let url = new URL(`/analysis/dsets/${config.wfversion.id}/`, document.location)
  const params = {
    dsids: fetchdsids ? fetchdsids.join(',') : Object.keys(dsnames).join(','),
    anid: existing_analysis ? existing_analysis.analysis_id : 0,
  };
  url.search = new URLSearchParams(params).toString();
  const result = await getJSON(url);
  // Only function where we have a list of errors, so not using {error: err1}
  // as in other places here! Instead, {errmsg: [err1, err2, ..]}
  if (result.error) {
    let msg = 'Errors found in datasets, please fix and refresh this page';
    notif.errors[msg] = 1;
    result.errmsg.forEach(msg => {
      notif.errors[msg] = 1;
    });
  } else {
    Object.keys(result.dsets).forEach(x => {
      dsets[x] = result.dsets[x];
      dsets[x].changed = false;
      dsnames[x] = dsets[x].storage;
      });
    Object.keys(dsets).forEach(x => {
      dsets[x].changed = false;
    })
    Object.entries(dsets).filter(x=>x[1].prefrac).forEach(x=>matchFractions(dsets[x[0]]));
    field_order = result.field_order;
  }
}


async function loadAnalysisResults() {
  if (added_analyses_order.indexOf(adding_analysis.selected) > -1) {
    return;
  }
  let url = new URL(`/analysis/resultfiles/load/${adding_analysis.selected}/`, document.location)
  const params = {
    dsids: Object.keys(dsnames).join(','),
    base_ana: base_analysis.selected || '0',
  };
  url.search = new URLSearchParams(params).toString();
  const result = await getJSON(url);
  if ('error' in result) {
    const msg = `While fetching analysis, encountered: ${result.error}`;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    added_analyses_order.push(adding_analysis.selected);
    added_results[adding_analysis.selected] = result;
    updateResultfiles();
  }
}


function removeAnalysisResults(anaid) {
  if (added_analyses_order.indexOf(anaid) === -1) {
    return
  }
  added_analyses_order = added_analyses_order.filter(x => x !== anaid);
  delete(added_results[anaid]);
  added_results = added_results;
  updateResultfiles();
}


async function loadBaseAnalysis() {
  /*
    Load fresh base analysis (resetting its isComplement/runFromPSM also)
    */
  const params = {
    dsids: Object.keys(dsets).join(','),
    added_ana_ids: Object.keys(added_results).join(','),
  }
  let url = new URL(`/analysis/baseanalysis/load/${config.wfversion.id}/${base_analysis.selected}/`, document.location);
  url.search = new URLSearchParams(params).toString();
  const result = await getJSON(url);
  if ('error' in result) {
    const msg = `While fetching base analysis, encountered: ${result.error}`;
    notif.errors[msg] = 1;
    setTimeout(function(msg) { notif.errors[msg] = 0 } , flashtime, msg);
  } else {
    base_analysis.resultfiles = result.resultfiles;
    base_analysis.dsets_identical = result.base_analysis.dsets_identical;
    for (const key of ['runFromPSM', 'isComplement']) {
      base_analysis[key] = false;
    }
    // svelte reactivity, this updates object
    base_analysis = base_analysis;
    let overlapping_setnames = new Set();
    let old_setnames = {};
    for (const dsid in dsets) {
      if (!(dsets[dsid].setname in old_setnames)) {
        old_setnames[dsets[dsid].setname] = new Set();
      }
      if (dsid in result.datasets) {
        const resds = result.datasets[dsid];
        dsets[dsid].setname = resds.setname;
        overlapping_setnames.add(dsets[dsid].setname);
        Object.keys(resds.fields).forEach(f => {
          dsets[dsid].fields[f] = resds.fields[f];
        });
        dsets[dsid].allfilessamesample = resds.allfilessamesample;
        dsets[dsid].picked_ftype = resds.picked_ftype;
        dsets[dsid].ft_files[resds.picked_ftype]
          .filter(x => x.id in resds.files)
          .forEach(x => {
            x.fields = resds.files[x.id].fields;
        });
        if (dsets[dsid].prefrac) {
          matchFractions(dsets[dsid]);
        }
      } else {
        old_setnames[dsets[dsid].setname].add(dsid);
      }
    }
    for (const sname in result.base_analysis.isoquants) {
      if (overlapping_setnames.has(sname)) {
        config.isoquants[sname] = result.base_analysis.isoquants[sname];
      }
    }
    Object.entries(old_setnames).forEach(([setname, dsids]) => {
      if (!dsids.size) {
        delete(config.isoquants[setname]);
      }
    });
    for (const key of ['flags', 'inputparams', 'multicheck', 'fileparams']) {
      config[key] = result.base_analysis[key];
    }
    Object.assign(config.multifileparams, result.base_analysis.multifileparams);
    let base_ana_contains_new_added_ana = false;
    Object.entries(result.base_analysis.added_results).forEach(([aid, base_add_res]) => {
      if (added_analyses_order.indexOf(aid) < 0) {
        base_ana_contains_new_added_ana = true;
        added_analyses_order.push(aid);
        added_results[aid] = base_add_res;
      }
    });
    if (base_ana_contains_new_added_ana) {
      updateResultfiles();
    }
    config = config;
    // svelte reactivity, this updates object so the options of the file params are updated
    // which triggers the select to update its intext via inputdone()
    libfiles = libfiles;
  }
}

function removeMultifile(fparam_id, key) {
  delete(config.multifileparams[fparam_id][key]);
  let newmfp = {}
  Object.keys(config.multifileparams[fparam_id]).forEach((k, ix) => {
    newmfp[ix] = config.multifileparams[fparam_id][k]
  });
  config.multifileparams[fparam_id] = newmfp;
}

function addMultifile(fparam_id) {
  const keyints = Object.keys(config.multifileparams[fparam_id]).map(x => +x);
  const newkey = keyints.length ? Math.max(...keyints) + 1 : 0;
  config.multifileparams[fparam_id][newkey] = '';
}

function getIntextFileName(fnid, files) {
  if (files && fnid in files) {
    return files[fnid].name 
  } else {
    return '';
  }
}

function matchFractions(ds) {
  let allfrs = new Set();
  for (let fn of ds.ft_files[ds.picked_ftype]) {
    const match = fn.name.match(RegExp(ds.fields.__regex));
    if (match) {
      fn.fr = match[1];
      allfrs.add(match[1]);
    } else {
      fn.fr = 'NA';
    }
  }
	matchedFr[ds.id] = allfrs.size;
}

function sortChannels(channels) {
  return Object.entries(channels).sort((a,b) => {
	  return a[0].replace('N', 'A') > b[0].replace('N', 'A')
  }).map(x => {return {ch: x[0], sample: x[1][0], chid: x[1][1]}});
}


function toggleRmCh(setn, ch) {
  if (ch in config.isoquants[setn].remove && config.isoquants[setn].remove[ch]) {
    config.isoquants[setn].remove[ch] = false;
  } else {
    config.isoquants[setn].remove[ch] = true;
  }
}


async function renewToken() {
  const post = {
    analysis_id: analysis_id,
  }
  const resp = await postJSON('/analysis/token/renew/', post);
  if (resp.error) {
    notif.errors[resp.error] = 1;
  } else {
    config.upload_token = resp.token;
  }
}


function copyToken() {
  navigator.clipboard.writeText(config.upload_token.user_token);
  copiedToken = true;
  setTimeout(() => {copiedToken = false;}, 2000);
}


function updateIsoquant(dsid_changed) {
  // Add new set, called when isobaric dataset gets new set name
  // FIXME Ideally handle if isobaric in wf in the dataset setname thing
  if ('ISOQUANT' in wf.components || 'ISOQUANT_SAMPLETABLE' in wf.components) {
    let ds = dsets[dsid_changed];
    const errmsg = `Sample set mixing error! Channels for datasets with setname ${ds.setname} are not identical!`;
    if (ds.qtype.is_isobaric) {
      if (ds.setname && !(ds.setname in config.isoquants)) {
        config.isoquants[ds.setname] = {
          chemistry: ds.qtype.short,
          channels: ds.channels,
          samplegroups: Object.fromEntries(Object.keys(ds.channels).map(x => [x, ''])),
          denoms: Object.fromEntries(Object.keys(ds.channels).map(x => [x, false])),
          remove: Object.fromEntries(Object.keys(ds.channels).map(x => [x, false])),
          report_intensity: false,
          sweep: false,
        };
      } else if (ds.setname && ds.setname in config.isoquants) {
        // Compare dataset channels with existing config.isoquant channels, to see
        // if not e.g TMT16 and TMT10 are mixed
        const dsch = new Set(Object.keys(ds.channels))
        const isoch = Object.keys(config.isoquants[ds.setname].channels);
        if (isoch.length !== dsch.size) {
            notif.errors[errmsg] = 1;
        } else {
          for (const ch of isoch) {
            if (!dsch.has(ch)) {
              notif.errors[errmsg] = 1;
              break;
            }
            ds.channels[ch].map((val, ix) => {
              if (val !== config.isoquants[ds.setname].channels[ch][ix]) {
                notif.errors[errmsg] = 1;
              }
            });
          }
        }
      }
    }

    // Remove old sets from config.isoquants if necessary
    const dset_sets = new Set(Object.values(dsets).filter(ds => ds.setname).map(ds => ds.setname));
    Object.keys(config.isoquants).filter(x => !(dset_sets.has(x))).forEach(x => {
      delete(config.isoquants[x])
    });
    config.isoquants = Object.assign({}, config.isoquants);  // assign so svelte notices (doesnt act on deletion)
  }
}

function clearWorkflow() {
  config.wfid = false;
  config.wfversion = false;
  wf = false;
  dsets = {};
} 


async function populate_analysis_and_fetch_wf() {
  // Only runs in onMount
  if (existing_analysis.wfid) {
    config.wfid = existing_analysis.wfid;
    if (existing_analysis.wfid in allwfs) {
      config.wfversion = allwfs[existing_analysis.wfid].versions.filter(
        x => x.id === existing_analysis.wfversion_id)[0];
    }
    await fetchWorkflow();
  }
  for (const key of ['analysisname',
      'jobstate',
      'flags',
      'inputparams',
      'multicheck',
      'fileparams',
      'isoquants',
      'editable',
      'external_desc',
      'external_results',
      'upload_token']) {
    config[key] = existing_analysis[key];
  }
  Object.assign(config.multifileparams, existing_analysis.multifileparams);
  if (Object.keys(existing_analysis.base_analysis).length) {
    base_analysis = existing_analysis.base_analysis;
  }
  // FIXME now repopulate files with sample names if any
}


onMount(async() => {
  if (ds_errors.length) {
    ds_errors.forEach(x => {
      notif.errors[x] = 1;
    });
  } else {
    if (existing_analysis) {
      runButtonActive = false;
      await populate_analysis_and_fetch_wf();
      // Populate dynamic select components by calling .inputdone()
      // Does not work if it is in populate_analysis, possibly is too fast
      // and values havent made it to selectval in the components yet
      if (wf) {
        Object.keys(wf.fileparams).forEach(x => {
          wf.fileparams[x].component.inputdone();
        });
      }
      // wf.mfp: [{id: 1, components: []}, ]
      // cfg.mfp: {1: {0: 3, 1: 4}, ...}
      Object.entries(config.multifileparams).forEach(([paramid, ix_val]) => {
        let mfp = wf.multifileparams.filter(mfp => mfp.id === parseInt(paramid))[0];
        Object.keys(ix_val).forEach(ix => {
          mfp.components[ix].inputdone();
        });
      });
      runButtonActive = true;
    }
  }
})
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

{#if Object.values(notif.links).some(x => x === 1)}
<div class="notification is-danger is-light errormsg"> 
    {#each Object.entries(notif.links).filter(x => x[1] == 1).map(x=>x[0]) as link}
    <div>Click here: <a target="_blank" href={link}>here</a></div>
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

<div class="content">
  <div class="box">
    {#if analysis_id}
    <span class="icon-text">
      {#if !runButtonActive}
        <span class="has-icon"><i class="fa fa-spinner fa-spin"></i></span> 
      {:else}
        {#if config.editable}
        <a title="Click to freeze" on:click={freezeAnalysis}><span class="icon "><i class="fa fa-lock-open has-text-grey"></i></span></a>
        {:else if config.external_results}
        <a title="Click to edit" on:click={unFreezeAnalysis}><span class="icon "><i class="fa fa-lock has-text-grey"></i></span></a>
        {/if}
        <span class="tags has-addons">
          <span class="tag">Analysis {config.editable ? 'editable' : 'frozen'}</span>
          {#if config.jobstate}
          <span class="tag {statecolors.tag[config.jobstate]}">{helptexts.jobstate[config.jobstate]}</span>
          {:else}
          <span class="tag is-grey-light">No job</span>
          {/if}
        </span>
      {/if}
    </span>
    {/if}
    <h5 class="mt-3 title is-5">Analysis name</h5>
    
    <div class="field">
      <input type="text" class="input" bind:value={config.analysisname} placeholder="Please enter analysis name">
    </div>
  </div>

  <div class="box">
    <label class="checkbox">
      <input type="checkbox" bind:checked={config.external_results} />
      Externally run analysis, upload results but don't run
    </label>

    {#if config.external_results}
      {#if config.wfid}
      <div class="field">
        <button class="button is-small" on:click={clearWorkflow}>Dont use workflow</button>
      </div>
      {/if}
    
    <textarea class="textarea" bind:value={config.external_desc} placeholder="Describe your analysis, relevant info, versions etc, possibly command line invocation used"></textarea>

    {#if config.external_results && config.editable}
      {#if analysis_id}
      <button title="Generate new token and invalidate any old tokens for this analysis" on:click={renewToken} class="button">
        <span class="icon is-small"><i class="fa fa-sync"></i></span>
        <span>New token</span>
      </button>
      {/if}

      {#if config.upload_token}
      <button on:click={copyToken} class="button">
        {#if copiedToken}
        <span class="icon is-small"><i class="fa fa-check has-text-success"></i></span>
        {:else}
        <span class="icon is-small"><i class="fa fa-copy"></i></span>
        <span>Copy token</span>
        {/if}
      </button>
      {/if}

      {#if config.upload_token}
      <div>
        <input class="input" value={config.upload_token.user_token} readonly />
        {#if config.upload_token.expired}
        <span class="has-text-danger">Token has expired</span>
        {:else}
        This token will expire {config.upload_token.expires}
        {/if}
      </div>
      <hr />
      <TokenInstructions filetype="analysis output" />
      {/if}
    {/if}

  {/if}
  </div>

  <div class="box">
    <h5 class="title is-5">Datasets selected</h5>
    <DynamicSelect placeholder='Find dataset to add' bind:selectval={dsetToAdd} on:selectedvalue={addDataset} niceName={x => x.name} fetchUrl="/analysis/find/datasets/" />
    <div class="tags">
    {#each Object.entries(dsnames) as [dsid, name]}
      <span class="tag is-medium is-info">
        {name}
        <button class="delete is-small" on:click={e => removeDataset(dsid)}></button>
      </span>
    {/each}
    {#if !Object.entries(dsnames).length}
    No datasets selected
    {/if}
    </div>
  </div>


  <div class="box">
    <div class="field is-horizontal">
      <div class="field-label is-normal">
        <label class="label">Workflow:</label>
      </div>
      <div class="field-body">
        <div class="field">
          <div class="select">
            <select bind:value={config.wfid} on:change={e => wf = config.wfversion = false}>
              <option disabled value={false}>Select workflow</option>
              {#each wforder as wfid}
              <option value={wfid}>{allwfs[wfid].name} </option>
              {/each}
            </select>
          </div>
        </div>
      </div>
      <div class="field-label is-normal">
        <label class="label">Workflow version:</label>
      </div>
      <div class="field-body">
        <div class="field">
          <div class="select" on:change={fetchWorkflow}>
            <select bind:value={config.wfversion}>
              <option disabled value={false}>Select workflow version</option>
              {#if config.wfid && config.wfid in allwfs}
              {#each allwfs[config.wfid].versions as wfv}
              <option value={wfv}>{wfv.date} -- {wfv.name}</option>
              {/each}
              {/if}
            </select>
          </div>
        </div>
      </div>
    </div>
    {#if config.wfid && !(config.wfid in allwfs)}
    <p class="has-text-danger">Workflow used is not selectable in interface</p>
    {/if}
  </div>

  {#if wf}
  <div class="box">
    <div class="title is-5">Fetch settings/files from a previous analysis</div>
    {#if wf && 'COMPLEMENT_ANALYSIS' in wf.components && base_analysis.selected}
    <div class="checkbox">
      {#if base_analysis.dsets_identical}
      <input type="checkbox" bind:checked={base_analysis.runFromPSM}>
      <label class="checkbox">
        Re-analyze previous analysis from PSM table, skipping identification steps
      </label>
      <a title="For output changes post-PSM table, e.g. for changes in quantification only. Any changes to fractions, input datasets, modifications etc will be ignored. For this to work you may not change set names."><i class="fa fa-question-circle"></i></a>
      {:else}
      <input type="checkbox" bind:checked={base_analysis.isComplement}>
      <label class="checkbox">
        Complement previous analysis with new or re-run sets (with replaced or extra raw data)
      </label>
      <a title="Skips parts of analysis already run, faster output"><i class="fa fa-question-circle"></i></a>
      {/if}
    </div>
    {/if}
    <DynamicSelect bind:intext={base_analysis.typedname} bind:selectval={base_analysis.selected} on:selectedvalue={e => loadBaseAnalysis()} niceName={x => x.name} fetchUrl="/analysis/baseanalysis/show/" />
  </div>
  {/if}

  {#if Object.keys(dsets).length}
 <div class="title is-5">Datasets details</div>
  {/if}
  {#each Object.values(dsets) as ds}
  <div class="box">
    {#if ds.changed}
    <div class="has-text-danger">
      <span>This dataset has changed files while editing  <button on:click={e => fetchDatasetDetails([ds.id])} class="button is-small">Reload dataset</button></span>
    </div>
    {/if}

    {#if ds.dtype.toLowerCase() === 'labelcheck'}
    <span class="has-text-primary">{ds.proj} // Labelcheck // {ds.run} // {ds.qtype.name} // {ds.instruments.join(',')}</span>
    {:else}
        <div class="subtitle is-6 has-text-primary">
          <span>{ds.proj} // {ds.exp} // {ds.run} //</span>
          {#if !ds.prefrac}
          <span>{ds.dtype}</span>
          {:else if ds.hr}
          <span>{ds.hr}</span>
          {:else}
          <span>{ds.prefrac}</span>
          {/if}
			  </div>
		<div class="columns">
		  <div class="column">
        {#if !ds.prefrac && !ds.qtype.is_isobaric}
        <input type="checkbox" bind:checked={ds.allfilessamesample}>
        <label class="checkbox">Same sample in each file in the dataset</label>
        {/if}
        {#if ds.allfilessamesample}
			  <div class="field">
          <input type="text" class="input" placeholder="Name of set" bind:value={ds.setname} on:change={e => updateIsoquant(ds.id)}>
			  </div>
        {/if}
        {#if wf && ds.prefrac && 'PREFRAC' in wf.components}
        <div class="field">
					<label class="label">Regex for fraction detection</label>
          <input type="text" class="input" on:change={e => matchFractions(ds)} bind:value={ds.fields.__regex}>
				</div>
				<span>{matchedFr[ds.id]} fractions matched</span>
        {/if}

        {#if ds.allfilessamesample && field_order.filter(x => !x.startsWith('__')).length}
        <hr>
        <h6 class="title is-6">Workflow specific fields:</h6>
        {#each field_order.filter(x => !x.startsWith('__')) as field}
        <div class="field">
          <label class="label"><code>{field}</code></label>
          <input type="text" class="input" bind:value={ds.fields[field]}>
				</div>
        {/each}
        {/if}
			</div>

			<div class="column">
        <div class="field">
					<label class="label">File type to use</label>
          <div class="select">
            <select bind:value={ds.picked_ftype}>
              {#each Object.keys(ds.ft_files) as ft}
              <option value={ft}>{ft}</option>
              {/each}
              {#each ds.incomplete_files as incomp}
              <option disabled>{incomp}</option>
              {/each}
            </select>
          </div>
        </div>
			  <div class="subtitle is-6">
          {#if (ds.qtype)}
				    <span>{ds.qtype.name} // </span>
          {/if}
		      <span>{ds.nrstoredfiles[0]} {ds.nrstoredfiles[1]} files </span>
				  <span>// {ds.instruments.join(', ')} </span>
			  </div>

			</div>
		</div>
    {#if !ds.allfilessamesample}
    <table class="table is-striped is-narrow">
      <thead>
        <th>File name</th>
        <th>Sample</th>
        {#each field_order.filter(x => !x.startsWith('__')) as field}
        <th><code>{field}</code></th>
        {/each}
      </thead>
      <tbody>
        {#each ds.ft_files[ds.picked_ftype] as fn}
        <tr>
          <td>{fn.name}</td>
          <td>
            <input type="text" class="input" bind:value={fn.fields.__sample} placeholder={fn.dsetsample}>
          </td>
          {#each field_order.filter(x => !x.startsWith('__')) as field}
          <td>
            <div class="field">
              <input type="text" class="input" bind:value={fn.fields[field]}>
				    </div>
          </td>
          {/each}
        </tr>
        {/each}
      </tbody>
    </table>
    {/if}
    {/if}
  </div>
  {/each}

  {#if wf}
  {#if 'ISOQUANT' in wf.components}
  <div class="box">
    <div class="title is-5">Isobaric quantification</div>
    {#if 'LABELCHECK_ISO' in wf.components}
    <span>Labelcheck pipeline does not require individual channel information</span>
    {:else if !(Object.entries(config.isoquants).length)}
    <span>No isobaric sets have been specified</span>
    {:else}
      {#each Object.entries(config.isoquants) as isoq}
      <div class="has-text-primary title is-6">Set: {isoq[0]}</div>

      {#if 'ISOQUANT_SAMPLETABLE' in wf.components}
        {#if !Object.values(isoq[1].denoms).some(x=>x)}
        <div class="field">
          <input type="checkbox" bind:checked={isoq[1].sweep}>
          <label class="checkbox">Use median sweeping (no predefined denominators)
            <span class="icon is-small">
              <a title="Pick median denominator per PSM, only for single-set analyses"><i class="fa fa-question-circle"></i></a>
            </span>
          </label>
        </div>
        {/if}

        {#if !isoq[1].sweep && !Object.values(isoq[1].denoms).some(x=>x)}
        <div class="field">
          <input type="checkbox" bind:checked={isoq[1].report_intensity}>
          <label class="checkbox">Report isobaric ion intensities instead of ratios
            <span class="icon is-small">
              <a title="Reports median intensity rather than fold changes, not for use with DEqMS"><i class="fa fa-question-circle"></i></a>
            </span>
          </label>
        </div>
        {/if}

      {/if}

      <div class="columns">
        <div class="column is-three-quarters">
          <table class="table is-striped is-narrow">
            <thead>
              <tr>
                {#if !('ISOQUANT' in wf.components) || (!isoq[1].sweep && !isoq[1].report_intensity)}
                <th>Denominator</th>
                {:else}
                <th><del>Denominator</del></th>
                {/if}
                <th>Channel</th>
                <th>Sample name</th>
                {#if 'ISOQUANT_SAMPLETABLE' in wf.components}
                  <th>Sample group 
                    <span class="icon is-small">
                      <a title="For DEqMS and PCA plots"><i class="fa fa-question-circle"></i></a>
                    </span>
                    LEAVE EMPTY FOR INTERNAL STANDARDS!
                  </th>
                {/if}
                {#if ('REMOVE_CHANNEL' in wf.components)}
                <th></th>
                {/if}
              </tr>
            </thead>
            <tbody>
              {#each sortChannels(isoq[1].channels) as {ch, sample}}
              <tr>
                <td>
                  {#if !('ISOQUANT' in wf.components) || (!isoq[1].sweep && !isoq[1].report_intensity)}
                  <input type="checkbox" bind:checked={isoq[1].denoms[ch]} />
                  {/if}
                </td>
                <td>{ch}</td>
                <td>{sample}</td>
                {#if 'ISOQUANT_SAMPLETABLE' in wf.components}
                <td>
                  {#if isoq[1].remove[ch]}
                  Empty channel
                  {:else}
                  <input type="text" class="input" bind:value={isoq[1].samplegroups[ch]} placeholder="Sample group or empty (e.g. CTRL, TREAT)">
                  {/if}
                </td>
                {/if}
                <td>
                  {#if ('REMOVE_CHANNEL' in wf.components && !isoq[1].remove[ch])}
                    <button class="button is-small is-rounded is-danger is-outlined" on:click={e => toggleRmCh(isoq[0], ch)}><span>Remove</span><span class="icon is-small"><i class="fas fa-times"></i></span></button>
                  {:else if ('REMOVE_CHANNEL' in wf.components && isoq[1].remove[ch])}
                    <button class="button is-small is-rounded is-success is-outlined" on:click={e => toggleRmCh(isoq[0], ch)}><span>Add</span><span class="icon is-small"><i class="fas fa-times"></i></span></button>
                  {/if}
                </td>
              </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </div>
      {/each}
    {/if}
  </div>
  {/if}

  {#if wf.multicheck.length + wf.numparams.length + wf.flags.length + wf.textparams.length + wf.selectparams.length}
  <div class="box">
    <div class="title is-5">Workflow parameters</div>
    {#each wf.multicheck as {nf, id, name, opts, help}}
    <div class="field">
      <label class="label">{name} 
        {#if help}
          <a title={help}><i class="fa fa-question-circle"></i></a>
        {/if}
        <code>{nf}</code></label> 
      {#each Object.entries(opts) as opt}
      <div>
        <input value={`${id}___${opt[0]}`} bind:group={config.multicheck} type="checkbox">
        <label class="checkbox">{opt[1]}</label>
      </div>
      {/each}
    </div>
    {/each}

    {#each wf.selectparams as {nf, id, name, opts, help}}
    <div class="field">
      <label class="label">{name} 
        {#if help}
          <a title={help}><i class="fa fa-question-circle"></i></a>
        {/if}
        <code>{nf}</code>
      </label> 
      <div class="select">
        <select bind:value={config.inputparams[id]}>
          <option disabled value={false}>-</option>
          {#each Object.entries(opts) as opt}
          <option value={opt[0]}>{opt[1]} </option>
          {/each}
        </select>
      </div>
    </div>
    {/each}

    {#each wf.textparams as {nf, id, name, help}}
    <div class="field">
      <label class="label">{name} 
        {#if help}
          <a title={help}><i class="fa fa-question-circle"></i></a>
        {/if}
        <code>{nf}</code>
      </label> 
      <input type="text" class="input" bind:value={config.inputparams[id]}>
    </div>
    {/each}

    {#each wf.numparams as {nf, id, name, help}}
    <div class="field">
      <label class="label">{name} 
        {#if help}
          <a title={help}><i class="fa fa-question-circle"></i></a>
        {/if}
        <code>{nf}</code></label> 
      <input type="number" class="input" bind:value={config.inputparams[id]}>
    </div>
    {/each}

    <label class="label">Config flags</label>
    {#each wf.flags as {nf, id, name, help}}
    <div>
      <input value={id} bind:group={config.flags} type="checkbox">
      <label class="checkbox">{name}</label>: <code>{nf}</code> 
        {#if help}
          <a title={help}><i class="fa fa-question-circle"></i></a>
        {/if}
    </div>
    {/each}
	</div>
  {/if}


  {#if wf.multifileparams.length + wf.fileparams.length}
  <div class="box">
    <div class="title is-5">Input files</div>
    Pick previous analyses to use results as input if needed:
    <DynamicSelect bind:intext={adding_analysis.typedname} bind:selectval={adding_analysis.selected} on:selectedvalue={e => loadAnalysisResults()} niceName={x => x.name} fetchUrl="/analysis/baseanalysis/show/" />

    <div class="tags">
    {#each added_analyses_order as anaid}
      <span class="tag is-medium is-info">
        {added_results[anaid].analysisname}
        <button class="delete is-small" on:click={e => removeAnalysisResults(anaid)}></button>
      </span>
    {/each}
    </div>

    {#each wf.multifileparams as filep}
      <label class="label">{filep.name} 
          <a on:click={e => addMultifile(filep.id)} title="Add another file"><i class="fa fa-plus-square"></i></a>
          {#if filep.help}
            <a title={filep.help}><i class="fa fa-question-circle"></i></a>
          {/if}
      </label>
      {#each Object.keys(config.multifileparams[filep.id]) as mfpkey}
      <label class="label is-small">
        File nr. {mfpkey} 
        <span class="icon is-small">
          <a on:click={e => removeMultifile(filep.id, mfpkey)} title="Remove this file"><i class="fa fa-trash-alt"></i></a>
        </span>
      </label>
        <div class="field">
          {#if !filep.allow_resultfile}
          <DynamicSelect bind:this={filep.components[mfpkey]} bind:selectval={config.multifileparams[filep.id][mfpkey]} niceName={x => x.name} fixedoptions={libfiles[filep.ftype]} fixedorder={libfnorder[filep.ftype]} />
          {:else}
          <DynamicSelect bind:this={filep.components[mfpkey]} bind:selectval={config.multifileparams[filep.id][mfpkey]} niceName={x => x.name} fixedoptions={Object.assign(resultfiles, libfiles[filep.ftype])} fixedorder={resultfnorder.concat(libfnorder[filep.ftype])} />
          {/if}
        </div>
      {/each}
    {/each}

    {#each wf.fileparams as filep}
    <div class="field">
      <label class="label">
        {filep.name} 
        {#if filep.help}
          <a title={filep.help}><i class="fa fa-question-circle"></i></a>
        {/if}
      </label>
      {#if !filep.allow_resultfile}
      <DynamicSelect bind:this={filep.component} bind:selectval={config.fileparams[filep.id]} niceName={x => x.name} fixedoptions={libfiles[filep.ftype]} fixedorder={libfnorder[filep.ftype]} />
      {:else}
      <DynamicSelect bind:this={filep.component} bind:selectval={config.fileparams[filep.id]} niceName={x => x.name} fixedoptions={Object.assign(resultfiles, libfiles[filep.ftype])} fixedorder={resultfnorder.concat(libfnorder[filep.ftype])} />
      {/if}
    </div>
    {/each}
	</div>
  {/if}
  {/if} 

  {#if runButtonActive && (!existing_analysis || config.editable)}
    {#if wf || config.external_results}
      <a class="button is-primary" on:click={storeAnalysis}>Store analysis</a>
      {#if config.external_results}
        <a class="button is-primary" on:click={freezeAnalysis}>Freeze analysis</a>
      {:else}
        <a class="button is-primary" on:click={runAnalysis}>Store and queue analysis</a>
      {/if}
    {/if}
  {:else if postingAnalysis}
	<a class="button is-primary is-loading"></a>
  {/if}

</div>
