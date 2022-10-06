package main

import (
	"io"
	"bufio"
	"regexp"
	"fmt"
	"os"
	"os/exec"
	"flag"
)

//	liftOver testspots2.bed over/3L.chain conversions.bed unmapped.txt

type Flags struct {
	Inpath string
	Outpath string
	Unmappedpath string
	Chainpath string
	LineName string
}

func GetFlags() Flags {
	var f Flags
	flag.StringVar(&f.Inpath, "i", "", "Input .bed file (required)")
	flag.StringVar(&f.Outpath, "o", "stdout", "Output .bed file (default = stdout)")
	flag.StringVar(&f.Unmappedpath, "u", "unmapped.txt", "Unmapped output file (default = unmapped.txt)")
	flag.StringVar(&f.Chainpath, "c", "", ".chain file to use for liftover (required).")
	flag.StringVar(&f.LineName, "l", "", "Name of line in chromomomes of input file to lift over (required).")
	flag.Parse()
	if f.Inpath == "" {
		panic(fmt.Errorf("input file path is required"))
	}
	if f.Chainpath == "" {
		panic(fmt.Errorf(".chain file path is required"))
	}
	if f.LineName == "" {
		panic(fmt.Errorf("line name is required"))
	}
	return f
}

func ExecLiftOver(inpath, outpath, unmappedpath, chainpath string) error {
	cmd := exec.Command("liftOver", inpath, chainpath, outpath, unmappedpath)
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return err
}

func CleanInput(in io.Reader, out io.Writer, linename string) error {
	re, err := regexp.Compile(`^([^	_]*)_` + linename)
	if err != nil {
		return err
	}

	s := bufio.NewScanner(in)
	for s.Scan() {
		if re.MatchString(s.Text()) {
			subs := re.FindStringSubmatch(s.Text())
			io.WriteString(out, re.ReplaceAllString(s.Text(), subs[1]) + "\n")
		}
	}
	return nil
}

func UncleanBed(in, clean io.Reader, out io.Writer, linename string) error {
	uncleanre, err := regexp.Compile(`^[^	_]*_` + linename)
	if err != nil {
		return err
	}

	cleanre := regexp.MustCompile(`^[^	]*`)

	ins := bufio.NewScanner(in)
	ins.Buffer([]byte{}, 1e12)
	for ins.Scan() {
		if !uncleanre.MatchString(ins.Text()) {
			io.WriteString(out, ins.Text() + "\n")
		}
	}

	cleans := bufio.NewScanner(clean)
	cleans.Buffer([]byte{}, 1e12)
	for cleans.Scan() {
		chr := cleanre.FindString(cleans.Text())
		uncleaned := cleanre.ReplaceAllString(cleans.Text(), chr + "_" + linename)
		io.WriteString(out, uncleaned + "\n")
	}

	return nil
}

func LiftOver(inpath string, out io.Writer, unmappedpath, chainpath, linename string) error {
	in, err := os.Open(inpath)
	if err != nil {
		return err
	}
	defer in.Close()

	inclean, err := os.CreateTemp("./", "inclean_*.bed")
	if err != nil {
		return err
	}
	err = CleanInput(in, inclean, linename)
	inclean.Close()
	defer os.Remove(inclean.Name())
	if err != nil {
		return err
	}

	outclean, err := os.CreateTemp("./", "outclean_*.bed")
	if err != nil {
		return err
	}
	outclean.Close()
	defer os.Remove(outclean.Name())

	err = ExecLiftOver(inclean.Name(), outclean.Name(), unmappedpath, chainpath)
	if err != nil {
		return err
	}

	outclean, err = os.Open(outclean.Name())
	if err != nil {
		return err
	}
	defer outclean.Close()

	in, err = os.Open(inpath)
	if err != nil {
		return err
	}
	defer in.Close()

	err = UncleanBed(in, outclean, out, linename)
	if err != nil {
		return err
	}

	return nil
}

func LiftOverFull(f Flags) error {
	out := os.Stdout

	if f.Outpath != "stdout" {
		var err error
		fmt.Println("using hardcoded outpath")
		out, err = os.Create(f.Outpath)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	err := LiftOver(f.Inpath, out, f.Unmappedpath, f.Chainpath, f.LineName)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	f := GetFlags()
	err := LiftOverFull(f)
	if err != nil {
		panic(err)
	}
}
