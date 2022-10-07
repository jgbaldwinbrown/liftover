package liftover

import (
	"strings"
	"github.com/jgbaldwinbrown/lscan/pkg"
	"strconv"
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
	TabDel string
	Chrcol int
	Bpcol1 int
	Bpcols []int
}

func GetFlags() Flags {
	var f Flags
	flag.StringVar(&f.Inpath, "i", "", "Input .bed file (required)")
	flag.StringVar(&f.Outpath, "o", "stdout", "Output .bed file (default = stdout)")
	flag.StringVar(&f.Unmappedpath, "u", "unmapped.txt", "Unmapped output file (default = unmapped.txt)")
	flag.StringVar(&f.Chainpath, "c", "", ".chain file to use for liftover (required).")
	flag.StringVar(&f.LineName, "l", "", "Name of line in chromomomes of input file to lift over (required).")
	flag.StringVar(&f.TabDel, "t", "", "comma-separated list of chromosome column, basepair start column, and optional basepair end column (to convert tab-delimited files")
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

	if f.TabDel != "" {
		ncom := strings.Count(f.TabDel, ",")
		if ncom < 1 || ncom > 2 {
			panic(fmt.Errorf("wrong number of tokens %v in TabDel", ncom+1))
		}
		nparsed := 0
		var err error
		if ncom == 1 {
			f.Bpcols = make([]int, 1)
			nparsed, err = fmt.Sscanf(f.TabDel, "%d,%d", &f.Chrcol, &f.Bpcols[0])
		} else {
			f.Bpcols = make([]int, 1)
			nparsed, err = fmt.Sscanf(f.TabDel, "%d,%d,%d", &f.Chrcol, &f.Bpcols[0], &f.Bpcols[1])
		}
		if nparsed != ncom+1 || err != nil {
			panic(fmt.Errorf("Could not parse tabdel %v", f.TabDel))
		}
	}
	return f
}

func ExecLiftOver(inpath, outpath, unmappedpath, chainpath string) error {
	cmd := exec.Command("liftOver", "-bedPlus=3", inpath, chainpath, outpath, unmappedpath)
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("ExecLiftOver: %w", err)
	}
	return nil
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
	in, err := GzOptOpen(inpath)
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

	in, err = GzOptOpen(inpath)
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

func ExtractBedLine(line []string, bed io.Writer, chrcol int, bpcols []int, linenum int) error {
	bp0, err := strconv.ParseInt(line[bpcols[0]], 0, 64)
	if err != nil {
		return err
	}
	if len(bpcols) == 2 {
		fmt.Fprintf(bed, "%v\t%v\t%v\t%v\n", line[chrcol], bp0-1, line[bpcols[1]], linenum)
	} else {
		fmt.Fprintf(bed, "%v\t%v\t%v\t%v\n", line[chrcol], bp0-1, bp0, linenum)
	}

	return nil
}

func ExtractBed(tab io.Reader, bed io.Writer, chrcol int, bpcols []int) error {
	if len(bpcols) < 1 || len(bpcols) > 2 {
		return fmt.Errorf("len(bpcols) %v too large or small", len(bpcols))
	}

	s := bufio.NewScanner(tab)
	s.Buffer([]byte{}, 1e12)
	var line []string
	split := lscan.ByByte('\t')
	commentre := regexp.MustCompile(`^#`)
	for linenum:=0; s.Scan(); linenum++ {
		if !commentre.MatchString(s.Text()) {
			line = lscan.SplitByFunc(line, s.Text(), split)
			err := ExtractBedLine(line, bed, chrcol, bpcols, linenum)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func MapBedLine(m map[int][]string, l []string) error {
	if len(l) != 4 {
		return fmt.Errorf("len(line) %v != 4", len(l))
	}

	lnum, err := strconv.ParseInt(l[3], 0, 64)
	if err != nil {
		return err
	}

	m[int(lnum)] = []string{l[0], l[1], l[2]}
	return nil
}

func BedMap(bed io.Reader) (map[int][]string, error) {
	s := bufio.NewScanner(bed)
	s.Buffer([]byte{}, 1e12)
	var line []string
	split := lscan.ByByte('\t')
	m := map[int][]string{}

	for s.Scan() {
		line = lscan.SplitByFunc(line, s.Text(), split)
		err := MapBedLine(m, line)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func ReturnBed(inpath string, bed io.Reader, tab io.Writer, chrcol int, bpcols []int) error {
	changemap, err := BedMap(bed)
	if err != nil {
		return err
	}

	in, err := GzOptOpen(inpath)
	if err != nil {
		return err
	}
	defer in.Close()

	if len(bpcols) < 1 || len(bpcols) > 2 {
		return fmt.Errorf("len(bpcols) %v too large or small", len(bpcols))
	}

	s := bufio.NewScanner(in)
	s.Buffer([]byte{}, 1e12)
	var line []string
	split := lscan.ByByte('\t')
	commentre := regexp.MustCompile(`^#`)
	for lnum:=0; s.Scan(); lnum++ {
		if commentre.MatchString(s.Text()) {
			fmt.Fprintf(tab, "%s\n", s.Text())
			continue
		}

		if bedline, ok := changemap[lnum]; ok {
			line = lscan.SplitByFunc(line, s.Text(), split)
			line[chrcol] = bedline[0]
			bp0, err := strconv.ParseInt(bedline[1], 0, 64)
			if err != nil {
				return err
			}
			line[bpcols[0]] = fmt.Sprintf("%d", bp0+1)
			if len(bpcols) == 2 {
				line[bpcols[1]] = bedline[2]
			}
			fmt.Fprintf(tab, "%s\n", strings.Join(line, "\t"))
		} else {
			fmt.Fprintf(tab, "%s\n", s.Text())
		}
	}
	return nil
}

func LiftTabDel(inpath string, out io.Writer, unmappedpath, chainpath, linename string, chrcol int, bpcols []int) error {
	inbed, err := os.CreateTemp("./", "inbed_*.bed")
	if err != nil {
		return err
	}
	defer os.Remove(inbed.Name())

	intab, err := GzOptOpen(inpath)
	if err != nil {
		return err
	}
	defer intab.Close()

	err = ExtractBed(intab, inbed, chrcol, bpcols)
	inbed.Close()
	if err != nil {
		return err
	}

	outbed, err := os.CreateTemp("./", "outbed_*.bed")
	if err != nil {
		return err
	}
	defer os.Remove(outbed.Name())

	err = LiftOver(inbed.Name(), outbed, unmappedpath, chainpath, linename)
	outbed.Close()
	if err != nil {
		return err
	}

	outbed, err = os.Open(outbed.Name())
	if err != nil {
		return err
	}

	err = ReturnBed(inpath, outbed, out, chrcol, bpcols)
	if err != nil {
		return err
	}

	return nil
}

func LiftOverFull(f Flags) error {
	var out io.WriteCloser = os.Stdout

	if f.Outpath != "stdout" {
		var err error
		fmt.Println("using hardcoded outpath")
		out, err = GzOptCreate(f.Outpath)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	var err error
	if f.TabDel != "" {
		err = LiftTabDel(f.Inpath, out, f.Unmappedpath, f.Chainpath, f.LineName, f.Chrcol, f.Bpcols)
	} else {
		err = LiftOver(f.Inpath, out, f.Unmappedpath, f.Chainpath, f.LineName)
	}

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
