<script>
import { getJSON } from './funcJSON.js';
import { createEventDispatcher } from 'svelte';

const dispatch = createEventDispatcher();

// intext only exported for initial value if no options (e.g. base analysis needs options fetched)
export let intext = '';

export let selectval = '';
export let fixedoptions = {};
export let fixedorder = [];
export let fetchUrl = false;
export let fetchedData;
export let niceName = function(text) { return text; }
export let unknowninput = '';
export let placeholder = 'Filter by typing';

let options = {};
$: {
  // When selectvalue changes (e.g. loaded from base analysis during first page load),
  // call populate the thing
  // Indeed a console.log call is enought to trigger this
  console.log(selectval);
  populateInitial();
}

let optorder = [];
let optorderindex;
$: optorderindex = Object.fromEntries(optorder.map((x, ix) => [x, ix]));

// currently hovered option id
let hoveropt = false;

// toggle dropdown to active
let typing = false;

// only when mouseSelect==false we can be done with input
let mouseSelect = false;

// Fall back initval in case user backs out from selection
const initval = selectval;

function populateInitial() {
  if (selectval && selectval in fixedoptions) {
    intext = niceName(fixedoptions[selectval]);
  }
}

export function inputdone() {
  /* This only does something if mouseSelect is false, but 
  then it is called when the input is received (e.g. esc, enter, mouse selectvalue).
  The intext is then set if there is a slected value, 
  */
  if (!mouseSelect) {
    typing = false;
    // Always run this, do not elseif chain, for when there are 
    // fixedoptions but also an unknown input (eg new value to save)
    // With elseif the unknown input would not be set
    if (selectval && selectval in options) {
      intext = niceName(options[selectval]);
    } else {
      unknowninput = intext;
      selectval = '';
      dispatch('novalue', {});
    } 
    // FIXME should we dispatch illegalvalue when the value is not in fixedoptions
    // and we are not allowed to create new values?
  }
}


function selectvalue(optid) {
  mouseSelect = false;
  selectval = options[optid].id;
  intext = niceName(options[selectval]);
  if (unknowninput !== '__ILLEGAL_PLACEHOLDER__') {
    unknowninput = '';
  }
  dispatch('selectedvalue', {});
  inputdone();
}


async function handleKeyInput(event) {
  // Takes care of key inputs, characters and backspace/delete
  if (event.keyCode === 27) {
    // escape
    mouseSelect = false;
    inputdone();
  } else if (hoveropt && (event.keyCode === 13 || event.keyCode === 9)) {
    // return || tab pressed in hover
    mouseSelect = false;
    selectvalue(hoveropt);
  } else if (!hoveropt && (event.keyCode === 13 || event.keyCode === 9)) {
    // return || tab pressed in new value thing hover
    inputdone();
  } else if (optorder.length && event.keyCode === 40) {
    // down arrow key
    if (hoveropt && optorderindex[hoveropt] + 1 <= optorder.length) {
      // else bottom line, do nothing
      hoveropt = optorder[optorderindex[hoveropt] + 1];
    } else if (!hoveropt) {
      hoveropt = optorder[0];
    }
  } else if (optorder.length && event.keyCode === 38) {
    // up arrow key
    hoveropt = hoveropt && optorderindex[hoveropt] > 0 ? optorder[optorderindex[hoveropt] - 1] : false;
  } else if (!intext.length) {
    // empty input, show all options, reset selectvalue
    hoveropt = false;
    options = Object.fromEntries(Object.entries(fixedoptions));
    optorder = fixedorder.length ? fixedorder : Object.keys(options);

  } else if (event.key.length > 1 && !(event.keyCode === 8 || event.keyCode === 46)) {
    // special key without modification effect (e.g. alt), not backspace/delete
    return

  } else if (intext.length > 2 && fetchUrl) {
    options = await getJSON(`${fetchUrl}?q=${intext}`);
    fetchedData = Object.assign({}, options);
    delete(options.ok);
    optorder = Object.keys(options);
    typing = true;

  } else if (!fetchUrl && fixedoptions && intext.trim().length) {
    // Typing with filter changes the options here
    let searched_opt = Object.entries(fixedoptions);
    for (let word of intext.trim().split(' ')) {
      searched_opt = searched_opt.filter(x => x[1].name.toLowerCase().includes(word.toLowerCase()));
    }
    options = Object.fromEntries(searched_opt);
    const keys = Object.keys(options);
    optorder = fixedorder.length ? fixedorder.filter(x => keys.indexOf(x.toString()) > -1) : keys;
    typing = true;

  } else if (!fetchUrl && fixedoptions) {
    options = Object.fromEntries(Object.entries(fixedoptions));
    optorder = fixedorder.length ? fixedorder : Object.keys(options);
    typing = true;

  } else{
  }
  if (!optorder.length) { hoveropt = false };
}

function starttyping() {
  intext = '';;
  const keys = Object.keys(options);
  //optorder = fixedorder.length ? fixedorder : keys;
  //options = fixedorder.length ? fixedoptions : options;
  optorder = [];
  options = [];
  typing = true;
  placeholder = selectval ? niceName(selectval) : '';
}
 
</script>

<div class="control has-icons-right" tabindex="0">
  <input type="text" on:blur={inputdone} class="input is-narrow" placeholder={placeholder} on:keyup|preventDefault|stopPropagation={handleKeyInput} on:focus={starttyping} bind:value={intext}>
  <span class="icon is-right"><i class="fas fa-chevron-down"></i></span>

  {#if typing}
  <div class="select is-multiple">
    <select multiple on:mousedown={e => mouseSelect = true} >
      {#if !optorder.length}
      <option disabled>Type more or type less...</option>
      {/if}
      {#each optorder as optid} 
      <option selected={optid===hoveropt} value={optid} on:mouseup={e => selectvalue(optid)} on:mousemove={e => hoveropt=optid}>{niceName(options[optid])}</option>
      {/each}
  </select>
  </div>
  {/if}
</div>
