package liftover

import (
	"io"
	"os"
	"compress/gzip"
	"regexp"
	"bufio"
)

type GzWriter struct {
	wc io.WriteCloser
	bw *bufio.Writer
	gzw *gzip.Writer
}

func (g *GzWriter) Write(p []byte) (n int, err error) {
	return g.gzw.Write(p)
}

func (g *GzWriter) Close() error {
	g.gzw.Close()
	g.bw.Flush()
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
	gzwriter.bw = bufio.NewWriter(gzwriter.wc)
	gzwriter.gzw = gzip.NewWriter(gzwriter.bw)

	return &gzwriter, nil
}

func GzWrapWriter(w io.WriteCloser) *GzWriter {
	g := new(GzWriter)
	g.wc = w
	g.bw = bufio.NewWriter(g.wc)
	g.gzw = gzip.NewWriter(g.bw)
	return g
}
