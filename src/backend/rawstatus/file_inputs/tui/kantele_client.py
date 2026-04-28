import os
from base64 import b64decode
import hashlib
from time import sleep
from datetime import datetime as dt
from urllib.parse import urljoin
import shlex

import requests
import certifi
from requests_toolbelt.multipart.encoder import MultipartEncoder

from textual import on
from textual.worker import get_current_worker

from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, Button, Label, Input, Log
from textual.containers import HorizontalGroup, Horizontal, Vertical, VerticalScroll
from textual.events import Paste, AppBlur
from textual.css.query import NoMatches


class File:
    def __init__(self, fn):
        self.fnid = False
        self.md5 = False
        self.path = fn
        self.fname = os.path.basename(fn)
        self.size = os.path.getsize(fn)
        self.prod_date = os.path.getctime(fn)
        self.description = False
        self.done = False
        self.error = False

    def set_md5(self):
        if not self.md5:
            hash_md5 = hashlib.md5()
            with open(self.path, 'rb') as fp:
                for chunk in iter(lambda: fp.read(4096), b''):
                    hash_md5.update(chunk)
            self.md5 = hash_md5.hexdigest()
            return self.md5
        else:
            # not updated
            return False


class LeftPane(VerticalScroll):
    def __init__(self):
        super().__init__()
        self.filelist = FileList()

    def compose(self) -> ComposeResult:
        yield HorizontalGroup(
                Label('File List', classes='boxtitle'),
                Button('Clear', id='clearfiles'),
                classes="panetitle")
        yield self.filelist

    @on(Button.Pressed, '#clearfiles')
    def clear_list(self, event: Button.Pressed) -> None:
        self.filelist.filelist = {}
        self.filelist.query(Label).remove()


class FileList(VerticalScroll):

    def __init__(self):
        super().__init__()
        self.filelist = {}

    def on_paste(self, event: Paste) -> None:
        for fns in event.text.splitlines():
            for fn in shlex.split(fns, posix=os.name=='posix'):
                # Remove whitespace and trailing quote
                # TODO see if this is needed
                fn_clean = fn.strip().strip("'") 
                if not fn_clean in self.filelist and os.path.isfile(fn_clean):
                    fn = File(fn_clean)
                    self.filelist[fn.fname] = fn
                    self.mount(Label(fn.path))

    def clear_list(self):
        self.filelist = {}
        self.query(Label).remove()


class DescriptionInput(Vertical):
    def __init__(self, fname) -> None:
        super().__init__()
        self.fname = fname

    def compose(self) -> ComposeResult:
        yield Label('Please input description')
        yield Input(placeholder=self.fname)
        yield Button('Continue', id='continuedesc')

    def get_description(self) -> str:
        return self.query_one(Input).value


class RightPane(Vertical):

    def __init__(self):
        super().__init__()
        self.token, self.kantelehost, self.need_desc = False, False, False

    def compose(self) -> ComposeResult:
        self.webtoken = Input(placeholder='Can be obtained on Kantele')
        self.checktoken = Button('Check token', variant='primary', id='checktoken')
        self.start = Button('Upload', variant='success', id='start')
        self.stop = Button('Stop', variant='error', id='stop')
        self.start.disabled, self.stop.disabled = True, True
        yield Label('Upload Controls', classes='boxtitle')
        yield Label('Token')
        yield self.webtoken
        yield HorizontalGroup(self.checktoken, self.start, self.stop)

    def toggle_buttons(self, check, start, stop):
        self.checktoken.disabled, self.start.disabled, self.stop.disabled = check, start, stop

    @on(Button.Pressed, '#start')
    def handle_start(self) -> None:
        self.toggle_buttons(True, True, False)

    @on(Button.Pressed, '#stop')
    def handle_stop(self) -> None:
        self.toggle_buttons(False, False, True)
        try:
            self.query_one(DescriptionInput).remove()
        except NoMatches:
            pass
        # Stop actual upload



class UploadApp(App):
    CSS_PATH = 'kantele_uploads.css'
    BINDINGS = [
            #('d', 'toggle_darklight', 'Toggle dark mode'),
            ]
    
    def __init__(self):
        super().__init__()
        self.theme = 'textual-light'
        self.desc_input = False
        self.uploadworker = False
        # load config w kantele URL etc

    def compose(self) -> ComposeResult:
        self.logtxt = Log('hello')
        self.right = RightPane()
        yield Header()
        yield Footer()
        yield Vertical(
            Horizontal(LeftPane(), self.right),
            Label('Log', classes='boxtitle'),
            self.logtxt,
        )

    def on_mount(self) -> None:
        self.title = 'Kantele Uploads'

    def action_toggle_darklight(self) -> None:
        self.theme = ('textual-dark' if self.theme == 'textual-light' else 'textual-light')

    @on(AppBlur)
    def focus_filelist(self, event) -> None:
        '''Have correct focus when drag/dropping files'''
        self.query_one(FileList).focus()

    @on(Button.Pressed, '#clearfiles')
    @on(Button.Pressed, '#stop')
    def handle_stop_upload(self, event) -> None:
        self.logthis('User requested stopping uploading')
        if self.uploadworker:
            self.uploadworker.cancel()

    def logthis(self, txt):
        self.logtxt.write(f'{dt.strftime(dt.now(), "%Y%m%d %H:%M")} - {txt}\n')

    @on(Button.Pressed, '#checktoken')
    def handle_checktoken(self) -> None:
        try:
            self.token, self.kantelehost, self.need_desc = b64decode(self.right.webtoken.value).decode('utf-8').split('|')
        except ValueError:
            self.logtxt.write_line('====================')
            self.logtxt.write_line('ERROR: Token invalid')
            self.token, self.kantelehost, self.need_desc = False, False, False
            self.right.toggle_buttons(False, True, True)
        else:
            self.logtxt.write_line(f'Ready for upload to {self.kantelehost}')
            self.right.toggle_buttons(True, False, True)
        
    @on(Button.Pressed, '#start')
    def handle_start_upload(self, _event) -> None:
        filelist = self.query_one(FileList).filelist
        try:
            loginresp = requests.get(urljoin(self.kantelehost, 'login/'), verify=certifi.where())
        except requests.exceptions.ConnectionError:
            self.logthis('Connecting error trying to contact Kantele. Try again or contact admin')
            self.handle_stop_upload(False)
            return
        cookies = loginresp.cookies
        # First get all descriptions
        for key, fn in filelist.items():
            if self.need_desc and not fn.description:
                self.desc_input = DescriptionInput(fn.fname)
                self.query_one(RightPane).mount(self.desc_input)
                self.logthis(f'Upload paused, requesting description for {fn.fname}')
                return
        self.uploadworker = self.run_worker(self.do_file_upload(cookies, filelist), exclusive=True, thread=True)

    #@work(exclusive=True, thread=True)
    async def do_file_upload(self, cookies, filelist):
        trf_url = urljoin(self.kantelehost, 'files/transfer/')
        worker = get_current_worker()
        while True:
            for key, fn in filelist.items():
                if fn.done or fn.error:
                    continue
                if not os.path.exists(fn.path):
                    self.call_from_thread(self.logthis, f'File {fn.fname} does not exist, skipping')
                    fn.error = True
                    continue
                if newmd5 := fn.set_md5():
                    self.call_from_thread(self.logthis, f'MD5 of {fn.fname} is {fn.md5}')
                query = query_file(self.kantelehost, 'files/transferstate/', fn.fname, fn.md5, fn.size,
                        fn.prod_date, cookies, self.token, fn.fnid, fn.description)
                if query.get('error', False):
                    self.call_from_thread(self.logthis, f'ERROR: {query["error"]}')
                    continue
                fn.fnid = query['fnid']
                if query['state'] == 'done':
                    self.call_from_thread(self.logthis, f'File {fn.fname} is already uploaded')
                    fn.done = True
    
                elif query['state'] == 'transfer':
                    self.call_from_thread(self.logthis, f'Uploading {fn.path} to {self.kantelehost}')
                    try:
                        resp = transfer_file(trf_url, fn.path, fn.fnid, self.token,
                                cookies, self.kantelehost)
                    except requests.exceptions.ConnectionError:
                        # FIXME wrong exception!
                        self.call_from_thread(self.logthis, 'Could not transfer {fn.fname}')
                    else:
                        if resp.status_code == 500:
                            result = {'error': 'Kantele server error when transferring file, '
                                'please contact administrator'}
                        elif resp.status_code == 413:
                            result = {'error': 'File to transfer too large for Kantele server! '
                                    'Please contact administrator'}
                        else:
                            result = resp.json()
                        if resp.status_code != 200:
                            if 'problem' in result:
                                if result['problem'] == 'NOT_REGISTERED':
                                    # Re-register in next round
                                    fn.md5 = False
                                elif result['problem'] == 'ALREADY_EXISTS':
                                    fn.done = True
                                elif result['problem'] == 'NO_RSYNC':
                                    fn.error = True
                                elif result['problem'] == 'RSYNC_PENDING':
                                    # Do nothing, but print to user
                                    pass
                                elif result['problem'] == 'MULTIPLE_ENTRIES':
                                    fn.error = True
                                elif result['problem'] == 'DUPLICATE_EXISTS':
                                    fn.error = True
                                self.call_from_thread(self.logthis, result['error'])
                            else:
                                self.call_from_thread(self.logthis, f'{result.get("error")} - Error trying to upload file, contact admin')
                                fn.error = True
                        else:
                            self.call_from_thread(self.logthis, f'Step 1 successful for transfer of file {fn.fname}, waiting for step 2')
                elif query['state'] == 'wait':
                    self.call_from_thread(self.logthis, f'Waiting for step 2 for {fn.fname} (move to storage)')
                else:
                    self.call_from_thread(self.logthis, f'Unknown reply, {str(query)}')

            if worker.is_cancelled or all(fn.done or fn.error for fn in filelist.values()):
                self.call_from_thread(self.logthis, 'Upload canceled')
                break
            sleep(5)

    @on(Button.Pressed, '#continuedesc')
    def handle_input_description(self, _event) -> None:
        filelist = self.query_one(FileList).filelist
        filelist[self.desc_input.fname].description = self.desc_input.get_description()
        self.desc_input.remove() 
        self.logthis('Received description')
        self.handle_start_upload(False)


def get_csrf(cookies, referer_host):
    return {'X-CSRFToken': cookies['csrftoken'], 'Referer': referer_host}


def query_file(host, url, fn, fn_md5, size, date, cookies, token, fnid, desc):
    url = urljoin(host, url)
    postdata = {'fn': fn,
                'token': token,
                'md5': fn_md5,
                'size': size,
                'date': date,
                'fnid': fnid,
                'desc': desc,
                }
    try:
        resp = requests.post(url=url, cookies=cookies, headers=get_csrf(cookies, host), 
                json=postdata, verify=certifi.where())
    except requests.exceptions.ConnectionError:
        error = 'Cannot connect to kantele server trying to query file {fn}, will try later'
    else:
        if resp.status_code != 500:
            resp_j = resp.json()
        if resp.status_code == 200:
            #(f'File {fn} has ID {resp_j["fn_id"]}, instruction: {resp_j["transferstate"]}')
            return {'fnid': resp_j['fn_id'], 'state': resp_j['transferstate']}
        elif resp.status_code == 403:
            error = (f'Permission denied querying server for file, server replied '
                    f' with {resp_j["error"]}')
        elif resp.status_code != 500:
            error = f'Problem querying for file, server replied with {resp_j["error"]}'
        else:
            error = (f'Could not register file, error code {resp.status_code}, '
                    'likely problem is on server, please check with admin')
        return {'error': f'{resp.status_code}: error'}


def transfer_file(url, fpath, fn_id, token, cookies, host):
    # use fpath/basename instead of fname, to get the
    # zipped file name if needed, instead of the normal fn
    filename = os.path.basename(fpath)
    stddata = {'fn_id': f'{fn_id}', 'token': token, 'filename': filename}
    with open(fpath, 'rb') as fp:
        stddata['file'] = (filename, fp)
        # MultipartEncoder from requests_toolbelt can stream large files, unlike requests (2024)
        mpedata = MultipartEncoder(fields=stddata)
        headers = get_csrf(cookies, host)
        headers['Content-Type'] = mpedata.content_type
        return requests.post(url, cookies=cookies, data=mpedata, headers=headers, verify=certifi.where())


if __name__ == '__main__':
    app = UploadApp()
    app.run()
