package liftover

import (
	"fmt"
	"io"
	"os"
)

func CloseAny[T any](ts ...T) {
	for _, t := range ts {
		a := any(t)
		if c, ok := a.(io.Closer); ok {
			c.Close()
		}
	}
}

func RemoveAll(files ...*os.File) {
	for _, f := range files {
		os.Remove(f.Name())
	}
}

func CreateTemps(dirs []string, prefixes []string) ([]*os.File, error) {
	var files []*os.File

	if len(dirs) != len(prefixes) {
		return nil, fmt.Errorf("GzCreateTemps: len(dirs) %v != len(prefixes) %v", len(dirs), len(prefixes))
	}

	for i:=0; i<len(dirs); i++ {
		file, err := os.CreateTemp(dirs[i], prefixes[i])
		if err != nil {
			CloseAny(files...)
			RemoveAll(files...)
			return nil, fmt.Errorf("CreateTemps: %w", err)
		}
		files = append(files, file)
	}

	return files, nil
}
