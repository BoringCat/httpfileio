package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
)

type HttpFile struct {
	closed bool
	point  int64
	length int64
	url    string
	client *http.Client
}

func NewHttpFile(url string, client *http.Client) (hf *HttpFile, err error) {
	if client == nil {
		client = &http.Client{}
	}
	var resp *http.Response
	if resp, err = client.Head(url); err != nil {
		return nil, err
	}
	return &HttpFile{
		point:  0,
		length: resp.ContentLength,
		url:    resp.Request.URL.String(),
		client: client,
	}, nil
}

func (hf *HttpFile) Close() error {
	if hf.closed {
		return fmt.Errorf("close %s: file already closed", hf.url)
	}
	hf.closed = true
	hf.client.CloseIdleConnections()
	return nil
}

func (hf *HttpFile) read(p []byte, start int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	} else if hf.length > 0 && start >= hf.length {
		return 0, io.EOF
	}
	req, err := http.NewRequest(http.MethodGet, hf.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, start+int64(len(p))-1))
	resp, err := hf.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body.Read(p)
	case http.StatusRequestedRangeNotSatisfiable:
		return 0, io.EOF
	default:
		return 0, io.ErrUnexpectedEOF
	}
}

func (hf *HttpFile) Read(p []byte) (n int, err error) {
	n, err = hf.read(p, hf.point)
	hf.point += int64(n)
	return n, err
}

func (hf *HttpFile) ReadAt(p []byte, off int64) (n int, err error) {
	return hf.read(p, off)
}

func (hf *HttpFile) Size() int64 {
	return hf.length
}

func (hf *HttpFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		if hf.point+offset < 0 {
			return 0, fmt.Errorf("seek %s: invalid argument", hf.url)
		}
		hf.point += offset
	case io.SeekEnd:
		if hf.length <= 0 || hf.length+offset < 0 {
			return 0, fmt.Errorf("seek %s: invalid argument", hf.url)
		}
		hf.point = hf.length + offset
	case io.SeekStart:
		if offset < 0 {
			return 0, fmt.Errorf("seek %s: invalid argument", hf.url)
		}
		hf.point = offset
	}
	return hf.point, nil
}

func zipfile() {
	fd, err := NewHttpFile("http://localhost:8080/test.zip", nil)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	zf, err := zip.NewReader(fd, fd.Size())
	if err != nil {
		panic(err)
	}
	for _, file := range zf.File {
		fmt.Println(file.Name)
	}
}

func tarfile() {
	fd, err := NewHttpFile("http://localhost:8080/test.tar.gz", nil)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	gz, err := gzip.NewReader(fd)
	if err != nil {
		panic(err)
	}
	tf := tar.NewReader(gz)
	for file, err := tf.Next(); err == nil; file, err = tf.Next() {
		fmt.Println(file.Name)
	}
}

func main() {
	zipfile()
	tarfile()
}
