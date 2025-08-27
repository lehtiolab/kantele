import requests
import json
from urllib.parse import urljoin, urlparse
from celery import states

from django.urls import reverse

from kantele import settings




def get_session_cookies():
    '''Gets CSRF cookie from login view'''
    session = requests.session()
    init_resp = session.get(urljoin(settings.KANTELEHOST, reverse('login')))
    headers = {'X-CSRFToken': session.cookies['csrftoken'], 'Referer': settings.KANTELEHOST}
    return session, headers


def update_db(url, form=False, json=False, files=False, msg=False, session=False, headers=False):
    if not session:
        session, headers = get_session_cookies()
    try:
        r = False
        if form:
            r = session.post(url=url, data=form, headers=headers)
        elif json:
            r = session.post(url=url, json=json,  headers=headers)
        elif files:
            r = session.post(url=url, files=files, headers=headers)
        r.raise_for_status()
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError) as e:
        # BOOM YOU'RE DEAD
        if not msg:
            msg = 'Could not update database: {}'
        msg = msg.format(e)
        raise RuntimeError(msg)
    else:
        if r:
            return r
        else:
            raise RuntimeError('Something went wrong')


def taskfail_update_db(task_id, msg=False):
    update_db(urljoin(settings.KANTELEHOST, reverse('jobs:settask')), json={'task_id': task_id,
        'client_id': settings.APIKEY, 'msg': msg, 'state': states.FAILURE})
