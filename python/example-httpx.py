
import io
import httpx
from http import HTTPStatus
from base import HTTPIO

class HTTPXIO(HTTPIO):
    def _map_io_error(self, resp:httpx.Response):
        error_str = f'{resp.status_code} {resp.reason_phrase}: {self.name}'
        if resp.status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError(error_str)
        elif resp.status_code == HTTPStatus.FORBIDDEN:
            raise PermissionError(error_str)

    def _range_header(self, length:int = -1):
        point = self.tell()
        if length == -1:
            return {'Range': f'bytes={point}-'}
        else:
            # -1 for range start at 0
            ## https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Range#single_byte_ranges_and_cors-safelisted_requests
            return {'Range': f'bytes={point}-{point+length-1}'}

    def __init__(self, url:str, client:httpx.Client|None = None) -> None:
        self.__client = client or httpx.Client()
        resp          = self.__client.head(url) # head file for infomation
        self.__url    = resp.url                 # get url after follow redirects
        super().__init__(str(resp.url))
        error_str = f'{resp.status_code} {resp.reason_phrase}: {self.name}'
        self._map_io_error(resp)
        if resp.status_code != 200:
            resp.raise_for_status()
            raise RuntimeError(error_str)
        try:    self.length = int(resp.headers.get('Content-Length'))
        except: self.length = -1

    def read(self, length: int = -1) -> bytes:
        '''
        main read function
        '''
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        resp = self.__client.get(
            url     = self.__url,
            headers = self._range_header(length)
        )
        if resp.status_code == HTTPStatus.RANGE_NOT_SATISFIABLE:
            return b''
        self._map_io_error(resp)
        l = len(resp.content)
        self.seek(l, io.SEEK_CUR)
        if length > 0 and l < length:
            self.length = self.tell()
        return resp.content

IO_BUFFER = 1 * 1024** 2 # 1MiB

import random
import zipfile

client = httpx.Client(http2=True)

with HTTPXIO('http://localhost:8080/test.zip', client) as hf:
    with zipfile.ZipFile(hf) as f:
        zlist = f.infolist()
        random.shuffle(zlist)
        for zf in zlist:
            with f.open(zf) as i:
                print('zip', 'unbuffed', zf.filename, len(i.read()))
    hf.seek(0,0)
    bf = io.BufferedReader(hf, IO_BUFFER)
    with zipfile.ZipFile(bf) as f:
        zlist = f.infolist()
        random.shuffle(zlist)
        for zf in zlist:
            with f.open(zf) as i:
                print('zip', 'buffed', zf.filename, len(i.read()))

import tarfile
with HTTPXIO('http://localhost:8080/test.tar.gz', client) as hf:
    with tarfile.open(fileobj=hf, mode='r:gz') as f:
        for file in f:
            i = f.extractfile(file) 
            print('tar.gz', 'unbuffed', file.name, len(i.read()) if i is not None else 0)
    hf.seek(0,0)
    bf = io.BufferedReader(hf, IO_BUFFER)
    with tarfile.open(fileobj=bf, mode='r:gz') as f:
        for file in f:
            i = f.extractfile(file) 
            print('tar.gz', 'buffed', file.name, len(i.read()) if i is not None else 0)