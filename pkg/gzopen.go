package liftover

import (
	"io"
	"os"
	"compress/gzip"
	"regexp"
)

type GzReader struct {
	rc io.ReadCloser
	gzr *gzip.Reader
}

func (g *GzReader) Read(p []byte) (n int, err error) {
	return g.gzr.Read(p)
}

func (g *GzReader) Close() error {
	g.gzr.Close()
	err := g.rc.Close()
	return err
}

func GzOptOpen(path string) (io.ReadCloser, error) {
	gzre := regexp.MustCompile(`\.gz$`)
	if !gzre.MatchString(path) {
		return os.Open(path)
	}

	var gzreader GzReader

	var err error
	gzreader.rc, err = os.Open(path)
	if err != nil {
		return nil, err
	}

	gzreader.gzr, err = gzip.NewReader(gzreader.rc)
	if err != nil {
		gzreader.rc.Close()
		return nil, err
	}

	return &gzreader, nil
}
