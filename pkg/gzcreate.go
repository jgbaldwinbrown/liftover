package liftover

import (
	"io"
	"os"
	"compress/gzip"
	"regexp"
)

type GzWriter struct {
	wc io.WriteCloser
	gzw *gzip.Writer
}

func (g *GzWriter) Write(p []byte) (n int, err error) {
	return g.gzw.Write(p)
}

func (g *GzWriter) Close() error {
	g.gzw.Close()
	err := g.wc.Close()
	return err
}

func GzOptCreate(path string) (io.WriteCloser, error) {
	gzre := regexp.MustCompile(`\.gz$`)
	if !gzre.MatchString(path) {
		return os.Create(path)
	}

	var gzwriter GzWriter

	var err error
	gzwriter.wc, err = os.Create(path)
	if err != nil {
		return nil, err
	}

	gzwriter.gzw = gzip.NewWriter(gzwriter.wc)

	return &gzwriter, nil
}
