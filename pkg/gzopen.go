package liftover

import (
	"fmt"
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
	// fmt.Println("opening path:", path)
	// cmd := exec.Command("cat", path)
	// cmd.Stdout = os.Stdout
	// cmd.Run()
	// fmt.Println("end path contents")
	gzre := regexp.MustCompile(`\.gz$`)
	if !gzre.MatchString(path) {
		return os.Open(path)
	}

	var gzreader GzReader

	var err error
	gzreader.rc, err = os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("GzOptOpen: %w", err)
	}

	gzreader.gzr, err = gzip.NewReader(gzreader.rc)
	if err != nil {
		gzreader.rc.Close()
		return nil, fmt.Errorf("GzOptOpen: %w", err)
	}

	return &gzreader, nil
}
