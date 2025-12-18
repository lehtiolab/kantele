<script>

export let tabshow; 
export let notif;

export let tabs = ['Projects', 'Analyses', 'Datasets', 'Files', 'Jobs'];

</script>

<style>
.errormsg {
  position: -webkit-sticky;
  position: sticky;
  top: 20px;
}
</style>

{#if Object.values(notif.errors).some(x => x === 1)}
<div class="notification is-danger is-light errormsg"> 
    {#each Object.entries(notif.errors).filter(x => x[1] == 1).map(x=>x[0]) as error}
    <div>{error}</div>
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

<div class="content">
  <div class="tabs is-toggle is-centered is-small">
    <ul>
      {#each tabs as tab}
      <li class={tabshow === tab ? 'is-active' : ''}>
        <a href={`#/${tab.toLowerCase()}`}><span>{tab}</span></a>
      </li>
      {/each}
    </ul>
  </div>
</div>
