<script>

import Router from 'svelte-spa-router';
import routes from './routes';
import { onMount } from 'svelte';
import { getJSON } from '../../datasets/src/funcJSON.js'

let messages = false;

function fetchData() {
  fetchMessages();
}

async function fetchMessages() {
  let url = '/messages/';
  const resp = await getJSON(url);
  if (!('error' in resp)) {
    messages = {};
    messages.olddef = resp.olddef;
    messages.purgable_analyses = resp.purgable_analyses;
    messages.old_purgable_analyses = resp.old_purgable_analyses;
  } else { console.log(resp); }
}

function get_purgable_analyses_ids(analyses) {
  return `#/analyses/?ids=${analyses.join(',')}`;
}


onMount(async() => {
  fetchData();
})

</script>

<div class="container is-fluid">

  {#if messages}
  <article class="message is-info"> 
    <div class="message-body">
      <h5 class="title is-5">Admin news</h5>
      {#if messages.purgable_analyses}
      <div>There are <a target="_blank" href={get_purgable_analyses_ids(messages.purgable_analyses)}>{messages.purgable_analyses.length} analyses</a> that can be purged</div>
      {/if}
      {#if messages.old_purgable_analyses}
      <div><a target="_blank" href={get_purgable_analyses_ids(messages.old_purgable_analyses)}>{messages.old_purgable_analyses.length} purgable analyses</a> are older than {messages.olddef}</div>
      {/if}
    </div>
  </article>
  {/if}

  <Router {routes} />
</div>
