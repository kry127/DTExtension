package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// note, that variables are pointers
var srcFileFlag = flag.String("src", "", "CSV with line timestamp on source")
var dstFileFlag = flag.String("dst", "", "CSV with line timestamp on destination")
var isAirbyteFlag = flag.Bool("ab", false, "Is Airbyte transfer")
var isStrictFlag = flag.Bool("strict", false, "Strict intervals")
var outputFmtFlag = flag.String("ofmt", "", "'maxlag' for generating CSV with max lag graphics, default -- average lag")
var experimentPrefixFlag = flag.String("prefix", "", "Experiment prefix flag adds additional value to CSV identifying experiment")

const (
	OUTPUT_FMT_MAX_LAG = "maxlag"
)


func writeMaxLag(writer io.Writer, lags map[time.Time]time.Duration) error {
	w := csv.NewWriter(writer)
	timestamps := []time.Time{}
	for ts := range lags {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})
	for _, ts := range timestamps {
		lag := lags[ts]
		var row []string
		if experimentPrefixFlag != nil && *experimentPrefixFlag != ""{
			row = append(row, *experimentPrefixFlag)
		}
		row = append(row, fmt.Sprintf("%f", ts.Sub(timestamps[0]).Seconds()))
		row = append(row, fmt.Sprintf("%f", lag.Seconds()))
		err := w.Write(row)
		if err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file " + filePath, err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for " + filePath, err)
	}
	return records
}

func asTriggerFormat(csv [][]string) (map[int]time.Time, error) {
	result := map[int]time.Time{}
	for i, row := range csv {
		key, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, fmt.Errorf("unable to parse key int at position #%v: %w", i, err)
		}
		timeAsString := row[1]
		layout := "2006-01-02 15:04:05.000000"
		ts, err := time.Parse(layout, timeAsString)
		if err != nil {
			return nil, fmt.Errorf("unable to parse time of trigger format at position #%v: %w", i, err)
		}
		result[key] = ts
	}
	return result, nil
}


func asAirbyteFormat(csv [][]string) (map[int]time.Time, error) {
	result := map[int]time.Time{}
	for i, row := range csv {
		key, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, fmt.Errorf("unable to parse key int at position #%v: %w", i, err)
		}
		timeAsString := row[1]
		timeAsString = strings.TrimSuffix(timeAsString, "+00")
		layout := "2006-01-02 15:04:05.000"
		ts, err := time.Parse(layout, timeAsString)
		if err != nil {
			return nil, fmt.Errorf("unable to parse time of trigger format '%s' at position #%v: %w", timeAsString, i, err)
		}
		result[key] = ts
	}
	return result, nil
}

type UploadInterval struct {
	from time.Time
	to time.Time
}

func (u *UploadInterval) Duration() time.Duration {
	return u.to.Sub(u.from)
}

func (u *UploadInterval) IsImaginary() bool {
	// this can be because of trigger on target hits earlier than on source
	return u.to.Before(u.from)
}

func NewUploadInterval(from, to time.Time) (*UploadInterval, error) {
	if isStrictFlag != nil && *isStrictFlag {
		if to.Before(from) {
			return nil, fmt.Errorf("invalid interval: from='%v' should appear before to='%v'", from, to)
		}
	}
	return &UploadInterval{from: from, to: to}, nil
}

// use if you would not like to consider not-transmitted files
func rightJoin(file1, file2 string, isAirbyte bool) (map[int]*UploadInterval, error) {
	var ts2 map[int]time.Time
	var err error
	csv2 := readCsvFile(file2)
	if isAirbyte {
		ts2, err = asAirbyteFormat(csv2)
	} else {
		ts2, err = asTriggerFormat(csv2)
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing destination timestamps (isAirbyte=%v): %w", isAirbyte, err)
	}
	ts1, err := asTriggerFormat(readCsvFile(file1))
	if err != nil {
		return nil, fmt.Errorf("error parsing source timestamps: %w", err)
	}

	result := map[int]*UploadInterval{}
	for key, to := range ts2 {
		from, ok := ts1[key]
		if !ok {
			log.Printf("cannot find key '%v' that should present in left csv file", key)
			continue
		}
		intv, err := NewUploadInterval(from, to)
		if err != nil {
			return nil, fmt.Errorf("cannot make new interval: %w", err)
		}
		result[key] = intv
	}

	return result, nil
}

func computeAverageLag(intervals map[int]*UploadInterval) float64 {
	cumulative := float64(0)
	for key, intv := range intervals {
		diff := intv.to.Sub(intv.from)
		if diff.Seconds() > 3000000000 {
			log.Fatalf("OH MY GOD: key = %v, from=%v, to=%v", key, intv.from, intv.to)
		}
		cumulative += diff.Seconds()
	}
	return cumulative / float64(len(intervals))
}

func computeMaxLag(intervals map[int]*UploadInterval) map[time.Time]time.Duration {
	// should iterate them with sorted 'from' coord
	intervs := []UploadInterval{}
	for _, interval := range intervals {
		if interval == nil {
			continue
		}
		if interval.IsImaginary() {
			// skip imaginaries, or algorithm will loop forever
			continue
		}
		intervs = append(intervs, *interval)
	}

	sort.Slice(intervs, func(i, j int) bool {
		return intervs[i].from.Before(intervs[j].from)
	})

	result := map[time.Time]time.Duration{}
	stack := []UploadInterval{}
	for _, intv := range intervs {
		for {
			if len(stack) == 0 {
				// stack is empty: (always) compute 'from' value and put single interval into stack
				stack = append(stack, intv)
				result[intv.from] = 0
			}
			if stack[0].to.Before(intv.from) {
				// on interval extraction we should write actual timestamp (to prevent overwriting)
				result[stack[0].to.Add(-time.Nanosecond)] = stack[0].Duration()
				// also make jump below to next stack value. This value can't be negative
				if len(stack) > 1 {
					result[stack[0].to] = stack[0].to.Sub(stack[1].from)
				}
				// deletion is correct if we do not put nested intervals and always computing value for 'from'
				stack = stack[1:]
				// continue stack traversing with new stack tip
				continue
			}
			if intv.from.Before(stack[0].from) {
				panic("impossible: should have been sorted by start time")
			}
			// here we know that value 'intv.from' in our stack tip interval
			result[intv.from] = intv.from.Sub(stack[0].from)
			// search for some stack
			i := 0
			for ;i < len(stack); i++ {
				if intv.to.Before(stack[i].to) {
					break
				}
			}
			if i < len(stack) {
				// the most earliest stack interval that can cover currtent 'to' value
				result[intv.to] = intv.to.Sub(stack[i].from)
				// now we know, what is the value of function for both intervals, so don't put interval in stack
				break
			} else {
				// the intv.to value would be computed when extracting from stack
				//result[intv.to] =  intv.Duration()
				// should add to stack as value possibly covering some other interval
				stack = append(stack, intv)
				break
			}
		}
	}
	// exhaust stack
	for len(stack) > 0 {
		result[stack[0].to.Add(-time.Nanosecond)] = stack[0].Duration()
		// also make jump below to next stack value. This value can't be negative
		if len(stack) > 1 {
			result[stack[0].to] = stack[0].to.Sub(stack[1].from)
		}
		stack = stack[1:]
	}
	// result is ready!
	return result
}

func main() {
	flag.Parse()

	if srcFileFlag == nil {
		log.Fatalf("'src' flag should be specified")
	}
	srcFile := *srcFileFlag

	if dstFileFlag == nil {
		log.Fatalf("'dst' flag should be specified")
	}
	dstFile := *dstFileFlag

	isAirbyte := false
	if isAirbyteFlag != nil {
		isAirbyte = *isAirbyteFlag
	}

	outputFmt := ""
	if outputFmtFlag != nil {
		outputFmt = *outputFmtFlag
	}

	uploadIntervals, err := rightJoin(srcFile, dstFile, isAirbyte)
	fmt.Fprintf(os.Stderr, "Overall intervals: %d\n", len(uploadIntervals))
	if isStrictFlag != nil && !*isStrictFlag {
		imaginaryCount := 0
		for _, ui := range uploadIntervals {
			if ui == nil {
				continue
			}
			if ui.IsImaginary() {
				imaginaryCount++
			}
		}
		if imaginaryCount > 0 {
			fmt.Fprintf(os.Stderr, "Imaginary interval count: %d\n", imaginaryCount)
		}
	}
	switch outputFmt {
	case OUTPUT_FMT_MAX_LAG:
		maxLag := computeMaxLag(uploadIntervals)
		err := writeMaxLag(os.Stdout, maxLag)
		if err != nil {
			log.Fatalf("cannot write CSV to output: %v", err)
		}
	default:
		if err != nil {
			log.Fatalf("cannot parse files: %w", err)
		}

		fmt.Println(computeAverageLag(uploadIntervals))
	}
	// flush stdout
	os.Stdout.Sync()
}