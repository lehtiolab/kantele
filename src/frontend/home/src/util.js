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

export let treatItemsValue = async function(url, thing, operationmsg, itemid, values, notif) {
  let postdata = {item_id: itemid};
  if (values) {
    postdata = Object.assign(postdata, values);
  }
  const resp = await postJSON(url, postdata);
  if (!resp.ok) {
    const msg = `Something went wrong ${operationmsg} ${thing} with id ${itemid}: ${resp.error}`;
    notif.errors[msg] = 1;
  } else {
    const msg = `${thing} with id ${itemid} queued for ${operationmsg}`;
    notif.messages[msg] = 1;
  }
}


export async function closeProject(projid, projects, _expire, notif) {
  // _expire is only there to not use, but called by same parent as
  // below setExpiryProject function
  const callback = false ;// (proj) => {refreshProj(proj.id)};
  await treatItemsValue('/datasets/archive/project/', 'project', 'closing', projid, {expires_in_days: false}, notif);
  const resp = await getJSON(`/refresh/project/${projid}`);
  if (!resp.ok) {
    const msg = `Something went wrong trying to refresh project ${projid}: ${resp.error}`;
    notif.errors[msg] = 1;
  } else {
    projects[projid] = Object.assign(projects[projid], resp);
  }
}

export async function setExpiryProject(projid, projects, expire_days, notif) {
  const callback = false ;// (proj) => {refreshProj(proj.id)};
  if (!parseInt(Number(expire_days))) {
    notif.errors['Must fill in a number days after which the project will expire'] = 1;
    return
  }
  await treatItemsValue('/datasets/archive/project/', 'project', 'closing', projid, {expires_in_days: expire_days}, notif);
  const resp = await getJSON(`/refresh/project/${projid}`);
  if (!resp.ok) {
    const msg = `Something went wrong trying to refresh project ${projid}: ${resp.error}`;
    notif.errors[msg] = 1;
  } else {
    projects[projid] = Object.assign(projects[projid], resp);
  }
}
