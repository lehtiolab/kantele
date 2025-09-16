<script>

import { createEventDispatcher } from 'svelte';
const dispatch = createEventDispatcher();
import { flashtime, statecolors, helptexts } from '../../util.js'

export let value;
export let field;
export let rowid;
export let help = '';
export let icon;
export let inactive;
export let allowedActions = [];

let confirmReady = false;
let color = statecolors[field.id];
let helptext = helptexts[field.id] ? helptexts[field.id][value] : false;

function setConfirm() {
  confirmReady = true;
  setTimeout(() => { confirmReady = false} , flashtime);
}
</script>

{#if field.type === 'tag'}
  {#if color}
  <span class={`tag ${color[value[0]]}`}>{value[0]} ({value[1]})</span>
  {:else}
  <span class="tag is-info">{value[0]} ({value[1]})</span>
  {/if}

{:else if field.type === 'bool'}
<span class="has-icon">
  {#if value}
  <i class="fa fa-check has-text-success"></i>
  {:else}
  -
  {/if}
</span>

{:else if field.type === 'icon'}
<span title={help} class={`icon is-small is-info`}><i class={`fa fa-${icon}`}></i></span>

{:else if field.type === 'state'}
<a title={helptext}>
  <span class={`icon is-small ${color[value]}`}><i class="fa fa-square"></i></span>
</a>

{:else if field.type === 'button' && allowedActions.indexOf(value) > -1}

  {#if field.confirm && field.confirm.indexOf(value) > -1 && !confirmReady}
  <button on:click={setConfirm} class="button is-small">{value}</button>
  {:else if field.confirm && field.confirm.indexOf(value) > -1}
  <button on:click={e => dispatch('rowAction', {id: rowid, action: value})} class="button is-small is-danger is-light">{value} - Are you sure?</button>
  {:else}
  <button on:click={e => dispatch('rowAction', {id: rowid, action: value})} class="button is-small">{value}</button>
  {/if}

{:else if field.type === 'smallcoloured'}
<div class={`is-size-7 ${color[value.state]}`}>{value.text}</div>

{:else if field.type !== 'button'}
  {#if inactive}
  <del>{value}</del>
  {:else}
  {value}
  {/if}
{/if}
