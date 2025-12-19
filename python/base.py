
import io
import errno
from abc import abstractmethod

class HTTPIO(io.RawIOBase):
    def __init__(self, name:str) -> None:
        self.__name   = name
        self.__point  = 0
        self.__length = -1
        self.__closed = False

    # 继承对象获取文件长度用
    @property
    def length(self):          return self.__length
    # 继承对象设置文件长度用
    @length.setter
    def length(self, val:int): self.__length = val
    @property
    def closed(self):          return self.__closed
    # 模式只支持读
    @property
    def mode(self):            return 'r'
    @property
    def name(self):            return self.__name

    # 虚拟关闭，并不处理任何逻辑
    def close(self) -> None: self.__closed = True

    def readable(self): return not self.closed

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == io.SEEK_CUR:        # 当前位置增加
            resp = self.__point + offset
            if resp < 0: resp = 0        # 加到负数就归零
            self.__point = resp
            return resp
        elif whence == io.SEEK_SET:      # 起始位置加
            if offset < 0:               # 不能小于0
                raise OSError(errno.EINVAL, 'Invalid argument')
            self.__point = offset
            return self.__point
        elif whence == io.SEEK_END:      # 结尾位置加
            if self.__length < 0:        # 流文件不能从末尾偏移
                raise OSError(errno.EINVAL, 'Invalid argument')
            resp = self.__length + offset
            if resp < 0:                 # 不能小于0
                raise OSError(errno.EINVAL, 'Invalid argument')
            self.__point = resp
            return resp
        return self.__point

    # 流文件回报不支持seek
    def seekable(self) -> bool:
        return self.__length > 0

    @abstractmethod
    def http_range_read(self, start:int, end:int = -1) -> tuple[bytes, int]:
        raise NotImplemented

    def read(self, n: int = -1) -> bytes:
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        if n == 0: # 如果读0，那就直接返回空
            return b''
        end = n
        if n > 0:  # 如果有限制长度
            end = self.__point + n - 1 # 设置结尾为 偏移量+长度-1
        data, length = self.http_range_read(self.__point, end)
        self.__point += length
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def tell(self) -> int:
        return self.__point

    # 不支持写
    def writable(self) -> bool:
        return False
