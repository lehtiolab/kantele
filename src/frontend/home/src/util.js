import { getJSON, postJSON } from '../../datasets/src/funcJSON.js'
import { flashtime } from '../../util.js'



export let treatItems = async function(url, thing, operationmsg, itemid, notif) {
  const resp = await postJSON(url, {item_id: itemid});
  if (!resp.ok) {
    const msg = `Something went wrong ${operationmsg} ${thing} with id ${itemid}: ${resp.error}`;
    notif.errors[msg] = 1;
  } else {
    const msg = `${thing} with id ${itemid} queued for ${operationmsg}`;
    notif.messages[msg] = 1;
  }
}


export async function closeProject(projid, projects, notif) {
  const callback = false ;// (proj) => {refreshProj(proj.id)};
  await treatItems('/datasets/archive/project/', 'project', 'closing', projid, notif);
  const resp = await getJSON(`/refresh/project/${projid}`);
  if (!resp.ok) {
    const msg = `Something went wrong trying to refresh project ${projid}: ${resp.error}`;
    notif.errors[msg] = 1;
  } else {
    projects[projid] = Object.assign(projects[projid], resp);
  }
}
