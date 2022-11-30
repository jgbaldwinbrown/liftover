package liftover

import (
	"sync"
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
	Tmpdir string
	Threads int
	Chunksize int
}

func GetFlags() Flags {
	var f Flags
	flag.StringVar(&f.Inpath, "i", "", "Input .bed file (required)")
	flag.StringVar(&f.Outpath, "o", "stdout", "Output .bed file (default = stdout)")
	flag.StringVar(&f.Unmappedpath, "u", "unmapped.txt", "Unmapped output file (default = unmapped.txt)")
	flag.StringVar(&f.Chainpath, "c", "", ".chain file to use for liftover (required).")
	flag.StringVar(&f.LineName, "l", "", "Name of line in chromomomes of input file to lift over (required).")
	flag.StringVar(&f.TabDel, "t", "", "comma-separated list of chromosome column, basepair start column, and optional basepair end column (to convert tab-delimited files")
	flag.StringVar(&f.Tmpdir, "T", "./", "Directory in which to store temporary files; current dir (./) by default")
	flag.IntVar(&f.Threads, "j", 1, "Jobs (threads) to use (default 1).")
	flag.IntVar(&f.Chunksize, "C", -1, "Number of lines per parallel chunk (default all lines in 1 chunk).")
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
			f.Bpcols = make([]int, 2)
			nparsed, err = fmt.Sscanf(f.TabDel, "%d,%d,%d", &f.Chrcol, &f.Bpcols[0], &f.Bpcols[1])
		}
		if nparsed != ncom+1 || err != nil {
			panic(fmt.Errorf("Could not parse tabdel %v; err: %w", f.TabDel, err))
		}
	}
	return f
}

func ExecLiftOver(in io.Reader, out io.Writer, unmappedpath, chainpath string) error {
	cmd := exec.Command("liftOver", "-bedPlus=3", "stdin", chainpath, "stdout", unmappedpath)
	cmd.Stdout = out
	cmd.Stdin = in
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
		return fmt.Errorf("CleanInput: %w", err)
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
		return fmt.Errorf("UncleanBed: %w, err")
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

func LiftOver(inpath string, out io.Writer, unmappedpath, chainpath, linename, tmpdir string) error {
	in, err := GzOptOpen(inpath)
	if err != nil {
		return fmt.Errorf("LiftOver: inpath opening error %w", err)
	}
	defer in.Close()

	temps, err := CreateTemps([]string{tmpdir, tmpdir}, []string{"inclean_*.bed.gz", "outclean_*.bed.gz"})
	if err != nil {
		return fmt.Errorf("LiftOver: createtemps %w", err)
	}
	defer RemoveAll(temps...)

	inclean := temps[0]
	gzinclean := GzWrapWriter(inclean)
	err = CleanInput(in, gzinclean, linename)
	gzinclean.Close()
	if err != nil {
		return fmt.Errorf("LiftOver: cleaninput %w", err)
	}

	gzinclean_r, err := GzOptOpen(inclean.Name())
	if err != nil {
		return fmt.Errorf("LiftOver: opening inclean %w", err)
	}
	defer gzinclean_r.Close()

	outclean := temps[1]
	gzoutclean := GzWrapWriter(outclean)

	err = ExecLiftOver(gzinclean_r, gzoutclean, unmappedpath, chainpath)
	if err != nil {
		return fmt.Errorf("LiftOver: execliftover %w", err)
	}
	gzoutclean.Close()

	gzoutclean_r, err := GzOptOpen(outclean.Name())
	if err != nil {
		return fmt.Errorf("creating gzoutclean_r: %w", err)
	}
	defer gzoutclean_r.Close()

	in, err = GzOptOpen(inpath)
	if err != nil {
		return fmt.Errorf("creating in: %w", err)
	}
	defer in.Close()

	err = UncleanBed(in, gzoutclean_r, out, linename)
	if err != nil {
		return fmt.Errorf("UncleanBed: %w", err)
	}

	return nil
}

func ExtractBedLine(line []string, bed io.Writer, chrcol int, bpcols []int, linenum int) error {
	bp0, err := strconv.ParseInt(line[bpcols[0]], 0, 64)
	if err != nil {
		return fmt.Errorf("ExtractBedLine: in strconv.ParseInt: %w", err)
	}
	if line[chrcol] != "!" {
		if len(bpcols) == 2 {
			fmt.Fprintf(bed, "%v\t%v\t%v\t%v\n", line[chrcol], bp0-1, line[bpcols[1]], linenum)
		} else {
			fmt.Fprintf(bed, "%v\t%v\t%v\t%v\n", line[chrcol], bp0-1, bp0, linenum)
		}
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
				return fmt.Errorf("ExtractBed: in ExtractBedLine: %w", err)
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
		return fmt.Errorf("MapBedLine: in ParseInt: %w", err)
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
			return nil, fmt.Errorf("BedMap: in MapBedLine %w", err)
		}
	}
	return m, nil
}

func ReturnBed(inpath string, bed io.Reader, tab io.Writer, chrcol int, bpcols []int) error {
	changemap, err := BedMap(bed)
	if err != nil {
		return fmt.Errorf("ReturnBed: in BedMap %w", err)
	}

	in, err := GzOptOpen(inpath)
	if err != nil {
		return fmt.Errorf("ReturnBed: in GzOptOpen %w", err)
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
				return fmt.Errorf("ReturnBed: in ParseInt %w", err)
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

func LiftTabDel(inpath string, out io.Writer, unmappedpath, chainpath, linename string, chrcol int, bpcols []int, tmpdir string) error {
	temps, err := CreateTemps([]string{tmpdir, tmpdir}, []string{"inbed_*.bed.gz", "outbed_*.bed.gz"})
	if err != nil {
		return fmt.Errorf("LiftTabDel: in CreateTemps %w", err)
	}
	defer RemoveAll(temps...)
	inbed := temps[0]
	outbed := temps[1]

	intab, err := GzOptOpen(inpath)
	if err != nil {
		return fmt.Errorf("LiftTabDel: in GzOptOpen %w", err)
	}
	defer intab.Close()

	gzinbed := GzWrapWriter(inbed)
	err = ExtractBed(intab, gzinbed, chrcol, bpcols)
	gzinbed.Close()
	if err != nil {
		return fmt.Errorf("LiftTabDel: in ExtractBed %w", err)
	}

	gzoutbed := GzWrapWriter(outbed)
	err = LiftOver(inbed.Name(), gzoutbed, unmappedpath, chainpath, linename, tmpdir)
	gzoutbed.Close()
	if err != nil {
		return fmt.Errorf("LiftTabDel: in LiftOver %w", err)
	}

	outbed_r, err := GzOptOpen(outbed.Name())
	if err != nil {
		return fmt.Errorf("GzOptOpen: %w", err)
	}

	err = ReturnBed(inpath, outbed_r, out, chrcol, bpcols)
	if err != nil {
		return fmt.Errorf("ReturnBed: %w", err)
	}

	return nil
}

type Errors []error

func (es Errors) Error() string {
	var b strings.Builder
	for _, e := range es {
		fmt.Fprintf(&b, "%v\n", e)
	}
	return b.String()
}

type ParallelArgs struct {
	Inpath string
	Out io.WriteCloser
	Unmappedpath string
}

func CombineOuts(paths []string, out io.Writer) error {
	for _, path := range paths {
		r, err := GzOptOpen(path)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return fmt.Errorf("CombineOuts file opening: %w", err)
		}
		defer r.Close()
		io.Copy(out, r)
	}
	return nil
}

func CombineOutsToPath(paths []string, outpath string) error {
	out, err := GzOptCreate(outpath)
	if err != nil {
		return fmt.Errorf("CombineOutsToPath outfile opening: %w", err)
	}
	defer out.Close()

	return CombineOuts(paths, out)
}

type ParallelIoSet struct {
	In *os.File
	Inwriter io.WriteCloser
	Out *os.File
	Outwriter io.WriteCloser
	Unmapped *os.File
}

func CreateParallelIoSet(tmpdir string) (ParallelIoSet, error) {
	temps, err := CreateTemps(
		[]string{tmpdir, tmpdir, tmpdir},
		[]string{
			"inpartial_*.bed.gz",
			"outpartial_*.bed.gz",
			"unmappedpartial_*.bed",
		},
	)
	if err != nil {
		return ParallelIoSet{}, fmt.Errorf("CreateParallelIoSet: %w", err)
	}

	temps[2].Close()
	gzin := GzWrapWriter(temps[0])
	gzout := GzWrapWriter(temps[1])

	return ParallelIoSet{
		temps[0],
		gzin,
		temps[1],
		gzout,
		temps[2],
	}, nil
}

func ParallelRun(fullinpath string, fullout io.Writer, fullunmappedpath string, threads, chunksize int, tmpdir string, f func(inpath string, out io.Writer, unmappedpath string) error) error {
	var wg sync.WaitGroup
	var errwg sync.WaitGroup
	jobs := make(chan ParallelArgs, threads * 8)
	errchan := make(chan error, threads * 8)
	var errs Errors

	errwg.Add(1)
	go func() {
		for err := range errchan {
			if err != nil {
				errs = append(errs, fmt.Errorf("Error handler: %w", err))
			}
		}
		errwg.Done()
	}()

	wg.Add(threads)
	for i:=0; i<threads; i++ {
		go func() {
			for job := range jobs {
				errchan <- f(job.Inpath, job.Out, job.Unmappedpath)
				job.Out.Close()
			}
			wg.Done()
		}()
	}

	r, err := GzOptOpen(fullinpath)
	if err != nil {
		close(jobs)
		wg.Wait()
		close(errchan)
		errwg.Wait()
		errs = append(errs, err)
		return fmt.Errorf("ParallelRun: %w", errs)
	}
	defer r.Close()

	s := bufio.NewScanner(r)
	s.Buffer([]byte{}, 1e12)

	var outpartials []string
	var unmappedpartials []string
	started := false

	var ioset ParallelIoSet
	for i:=0; s.Scan(); i++ {

		if i % chunksize == 0 {
			if started {
				ioset.Inwriter.Close()
				jobs <- ParallelArgs{ioset.In.Name(), ioset.Outwriter, ioset.Unmapped.Name()}

			}
			ioset, err = CreateParallelIoSet(tmpdir)
			defer func(ioset ParallelIoSet) {
				ioset.Outwriter.Close()
				os.Remove(ioset.In.Name())
				os.Remove(ioset.Out.Name())
				os.Remove(ioset.Unmapped.Name())
			}(ioset)

			started = true

			if err != nil {
				close(jobs)
				wg.Wait()
				close(errchan)
				errwg.Wait()
				errs = append(errs, err)
				return fmt.Errorf("ParallelRun: %w", errs)
			}


			outpartials = append(outpartials, ioset.Out.Name())
			unmappedpartials = append(unmappedpartials, ioset.Unmapped.Name())
		}


		fmt.Fprintln(ioset.Inwriter, s.Text())
	}
	if started {
		ioset.Inwriter.Close()
		jobs <- ParallelArgs{ioset.In.Name(), ioset.Outwriter, ioset.Unmapped.Name()}

	}

	close(jobs)
	wg.Wait()
	close(errchan)
	errwg.Wait()

	err = CombineOuts(outpartials, fullout)
	if err != nil {
		errs = append(errs, fmt.Errorf("CombineOuts: %w", err))
	}

	err = CombineOutsToPath(unmappedpartials, fullunmappedpath)
	if err != nil {
		errs = append(errs, fmt.Errorf("CombineOutsToPath: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("ParallelRun: %w", errs)
	}
	return nil
}

func LiftOverFull(f Flags) error {
	var out io.WriteCloser = os.Stdout

	if f.Outpath != "stdout" {
		var err error
		out, err = GzOptCreate(f.Outpath)
		if err != nil {
			return fmt.Errorf("LiftOverFull: %w", err)
		}
		defer out.Close()
	}

	var err error

	if f.Threads < 2 || f.Chunksize < 1 {
		if f.TabDel != "" {
			err = LiftTabDel(f.Inpath, out, f.Unmappedpath, f.Chainpath, f.LineName, f.Chrcol, f.Bpcols, f.Tmpdir)
		} else {
			err = LiftOver(f.Inpath, out, f.Unmappedpath, f.Chainpath, f.LineName, f.Tmpdir)
		}
	} else {
		if f.TabDel != "" {
			err = ParallelRun(f.Inpath, out, f.Unmappedpath, f.Threads, f.Chunksize, f.Tmpdir, func(inpath string, outarg io.Writer, unmappedpath string) error {
				return LiftTabDel(inpath, outarg, unmappedpath, f.Chainpath, f.LineName, f.Chrcol, f.Bpcols, f.Tmpdir)
			})
		} else {
			err = ParallelRun(f.Inpath, out, f.Unmappedpath, f.Threads, f.Chunksize, f.Tmpdir, func(inpath string, outarg io.Writer, unmappedpath string) error {
				return LiftOver(inpath, outarg, unmappedpath, f.Chainpath, f.LineName, f.Tmpdir)
			})
		}
	}

	if err != nil {
		return fmt.Errorf("LiftOverFull: %w", err)
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
