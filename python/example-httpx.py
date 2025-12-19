
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

    def __init__(self, url:str, client:httpx.Client|None = None) -> None:
        self.__can_close = client is None
        self.__client    = client or httpx.Client()
        resp             = self.__client.head(url) # head file for infomation
        self.__url       = resp.url                 # get url after follow redirects
        super().__init__(str(resp.url))
        error_str = f'{resp.status_code} {resp.reason_phrase}: {self.name}'
        self._map_io_error(resp)
        if resp.status_code != 200:
            resp.raise_for_status()
            raise RuntimeError(error_str)
        try:    self.length = int(resp.headers.get('Content-Length'))
        except: self.length 
        
    def close(self):
        if self.__can_close:
            self.__client.close()
        return super().close()

    def http_range_read(self, start, end = -1):
        get_range = (start, end if end > 0 else '')
        with self.__client.stream(method = 'GET', url = self.__url, headers = {'Range': 'bytes=%s-%s' % get_range}) as resp:
            if resp.status_code == HTTPStatus.RANGE_NOT_SATISFIABLE:
                return b'', 0
            elif resp.status_code == HTTPStatus.PARTIAL_CONTENT:
                data = resp.read()
                return data, len(data)
            else:
                raise EOFError

# IO_BUFFER = 1 * 1024** 2 # 1MiB

# import random
# import zipfile
# import io

# client = httpx.Client(http2=True)

# with HTTPXIO('http://localhost:8080/test.zip', client) as hf:
#     with zipfile.ZipFile(hf) as f:
#         zlist = f.infolist()
#         random.shuffle(zlist)
#         for zf in zlist:
#             with f.open(zf) as i:
#                 print('zip', 'unbuffed', zf.filename, len(i.read()))
#     hf.seek(0,0)
#     bf = io.BufferedReader(hf, IO_BUFFER)
#     with zipfile.ZipFile(bf) as f:
#         zlist = f.infolist()
#         random.shuffle(zlist)
#         for zf in zlist:
#             with f.open(zf) as i:
#                 print('zip', 'buffed', zf.filename, len(i.read()))

# import tarfile
# with HTTPXIO('http://localhost:8080/test.tar.gz', client) as hf:
#     with tarfile.open(fileobj=hf, mode='r:gz') as f:
#         for file in f:
#             i = f.extractfile(file) 
#             print('tar.gz', 'unbuffed', file.name, len(i.read()) if i is not None else 0)
#     hf.seek(0,0)
#     bf = io.BufferedReader(hf, IO_BUFFER)
#     with tarfile.open(fileobj=bf, mode='r:gz') as f:
#         for file in f:
#             i = f.extractfile(file) 
#             print('tar.gz', 'buffed', file.name, len(i.read()) if i is not None else 0)