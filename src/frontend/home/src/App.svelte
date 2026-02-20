<script>

import Router from 'svelte-spa-router';
import routes from './routes';
import { onMount } from 'svelte';
import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'

let messages = false;
let msgorder = [];
let nrNewMsgs = 0;
let showingMessages = false;


async function fetchMessages() {
  let url = '/messages/';
  const resp = await getJSON(url);
  if (!('error' in resp)) {
    messages = resp.messages;
    msgorder = resp.order;
    nrNewMsgs = resp.nr_new;
  } else { console.log(resp); }
}


function markAllAsRead() {
  markAsRead(Object.keys(messages));
}


function markAsRead(msgids) {
  postJSON('/messages/set/read/', {msgids: msgids});
  msgids.forEach(x => {
    messages[x].shown = true;
  })
}

function markAsReadAndShowLink(msgid) {
  const _ids = messages[msgid].link_id;
  markAsRead([msgid]);
  window.open(`#/${messages[msgid].linkpath}/?ids=${_ids}`, '_blank').focus();
}

function deleteMessage(msgids) {
  postJSON('/messages/set/delete/', {msgids: msgids});
  msgids.forEach(x => {
    msgorder = msgorder.filter(it => it !== x);
    delete(messages[x]);
  })
}

function deleteAllMessages() {
  deleteMessage(Object.keys(messages));
}

onMount(async() => {
  fetchMessages();
  setInterval(fetchMessages, 5000);
})

</script>

<div class="container">

  {#if messages}
  <article class="message is-info"> 
  
    <div class="message-header" on:click={e => showingMessages = showingMessages === false}>
      {#if Object.keys(messages).length}
        Messages
        {#if nrNewMsgs}
        ({nrNewMsgs} new)
        {/if}
        <a>
          {#if !showingMessages}
          Show
          {:else}
          Hide
          {/if}
        </a>
      {:else}
        No messages
      {/if}
    </div>

    {#if showingMessages}
    <div class="message-body">
      {#if nrNewMsgs}
        <button class="button is-small" on:click={markAllAsRead}>Mark all as read</button>
      {/if}
      <button class="button is-small" on:click={deleteAllMessages}>Delete all</button>
      {#each msgorder as msgid}
        <div>
          <a on:click={e => deleteMessage([msgid])}><span class="icon"><i class="fa fa-trash-alt" /></span></a>
          {#if messages[msgid].shown}
          <span class="has-text-info-light">&bull;</span> <span>{messages[msgid].txt} (<a href="#/{messages[msgid].linkpath}/?ids={messages[msgid].link_id}" target="_blank">Show</a> in new tab)</span> 
          {:else}
          <span>&bull;</span> <span on:click={e => markAsRead([msgid])}>{messages[msgid].txt} (<a on:click={e => markAsReadAndShowLink(msgid)}>Show</a> in new tab)</span> 
          {/if}
        </div>
      {/each}

    </div>
    {/if}
  </article>
  {/if}
</div>

<div class="container is-fluid">
  <Router {routes} />
</div>
