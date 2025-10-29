export async function parseResponse(response) {
  let jsonresp;
  try {
    jsonresp = await response.json();
  } catch(error) {
    // Non-JSON responses, e.g. HTTP 500 server crash
    return {ok: false, error: 'Server error encountered', status: response.status};
  }
  // Deliver HTTP errors if any
  jsonresp.ok = response.ok;
  jsonresp.status = response.status;
  return jsonresp;
}


export async function getJSON(url) {
  let response;
  try {
    response = await fetch(url);
  } catch {
      return {ok: false, error: 'Kantele encountered a network error', status: false}
  }
  return await parseResponse(response);
}


export async function postJSON(url, postdata) {
  const csrftoken = getCookie('csrftoken');
  let response;
  try {
    response = await fetch(url, {
      method: 'POST', headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrftoken},
      body: JSON.stringify(postdata)
    });
  } catch {
    return {ok: false, error: 'Kantele encountered a network error', status: false}
  }
  return await parseResponse(response);
}


export function getCookie(name) {
  let cookieValue = null;
  if (document.cookie && document.cookie !== '') {
    const cookies = document.cookie.split(';');
    for (let i = 0; i < cookies.length; i++) {
      const cookie = cookies[i].trim();
      // Does this cookie string begin with the name we want?
      if (cookie.substring(0, name.length + 1) === (name + '=')) {
          cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
          break;
      }
    }
  }
  return cookieValue;
}

