package main

import (
	// "bytes"
	// "encoding/binary"
	"flag"
	"fmt"
	"log"
	// "net/http"
	// _ "net/http/pprof"
	"os"
	// "regexp"
	"runtime"
	"strconv"
	// "strings"
	// "sync"
	// "sync/atomic"
	"crypto/md5"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/davecgh/go-spew/spew"
	"time"
	// asl "github.com/aerospike/aerospike-client-go/logger"
	// ast "github.com/aerospike/aerospike-client-go/types"
)

// type TStats struct {
// 	Exit       bool
// 	W, R       int // write and read counts
// 	WE, RE     int // write and read errors
// 	WTO, RTO   int // write and read timeouts
// 	WMin, WMax int64
// 	RMin, RMax int64
// 	WLat, RLat int64
// 	Wn, Rn     []int64
// }

// var countReportChan chan *TStats

var host = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")
var namespace = flag.String("n", "test", "Aerospike namespace.")
var set = flag.String("s", "testset", "Aerospike set name.")

// var keyCount = flag.Int("k", 1000000, "Key/record count or key/record range.")

// var user = flag.String("U", "", "User name.")
// var password = flag.String("P", "", "User password.")

// var binDef = flag.String("o", "I", "Bin object specification.\n\tI\t: Read/write integer bin.\n\tB:200\t: Read/write byte array bin of length 200.\n\tS:50\t: Read/write string bin of length 50.")
// var concurrency = flag.Int("c", 32, "Number of goroutines to generate load.")
// var workloadDef = flag.String("w", "I:100", "Desired workload.\n\tI:60\t: Linear 'insert' workload initializing 60% of the keys.\n\tRU:80\t: Random read/update workload with 80% reads and 20% writes.")
// var latency = flag.String("L", "", "Latency <columns>,<shift>.\n\tShow transaction latency percentages using elapsed time ranges.\n\t<columns> Number of elapsed time ranges.\n\t<shift>   Power of 2 multiple between each range starting at column 3.")
// var throughput = flag.Int64("g", 0, "Throttle transactions per second to a maximum value.\n\tIf tps is zero, do not throttle throughput.")
// var timeout = flag.Int("T", 0, "Read/Write timeout in milliseconds.")
// var maxRetries = flag.Int("maxRetries", 2, "Maximum number of retries before aborting the current transaction.")
// var connQueueSize = flag.Int("queueSize", 4096, "Maximum number of connections to pool.")

// var randBinData = flag.Bool("R", false, "Use dynamically generated random bin values instead of default static fixed bin values.")
// var useMarshalling = flag.Bool("M", false, "Use marshaling a struct instead of simple key/value operations")
// var debugMode = flag.Bool("d", false, "Run benchmarks in debug mode.")
// var profileMode = flag.Bool("profile", false, "Run benchmarks with profiler active on port 6060.")
var showUsage = flag.Bool("u", false, "Show usage information.")

// parsed data
// var binDataType string
// var binDataSize int
// var workloadType string
// var workloadPercent int
// var latBase, latCols int

// group mutex to wait for all load generating go routines to finish
// var wg sync.WaitGroup

// // throughput counter
// var currThroughput int64
// var lastReport int64

func main() {
	// use all cpus in the system for concurrency
	spew.Dump("here we go...")
	log.Printf("Setting number of CPUs to use: %d", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())
	readFlags()

	seedDB(1000, 3)
}

func seedDB(cidCount int, didsPerCid int) {
	client, err := as.NewClient("10.150.73.10", 3000)
	panicOnError(err)

	var WritePolicy = as.NewWritePolicy(0, 0)
	WritePolicy.Timeout = 10000 * time.Millisecond
	WritePolicy.SocketTimeout = 10000 * time.Millisecond

	log.Printf("Seeding the database with %v CIDs, using %v Devices per CID", cidCount, didsPerCid)
	begin := time.Now()
	for c := 0; c < cidCount; c++ {
		cid := makeUuidFromString("cid" + strconv.Itoa(c))

		var dids []string
		for d := 0; d < didsPerCid; d++ {
			did := makeUuidFromString(strconv.Itoa(c) + "+" + strconv.Itoa(d))
			dids = append(dids, did)
			key, err := as.NewKey("cid", "devices", "DID:"+did)
			panicOnError(err)

			bins := as.BinMap{"CID": cid}

			err = client.Put(WritePolicy, key, bins)
			panicOnError(err)
		}

		// Write CID
		key, err := as.NewKey("cid", "devices", "CID:"+cid)
		panicOnError(err)
		bins := as.BinMap{"Devices": dids}
		err = client.Put(WritePolicy, key, bins)
		panicOnError(err)
	}
	end := time.Now()

	log.Println("Seeded " + fmt.Sprintf("%v", cidCount) + " CIDs in: " + fmt.Sprintf("%v", end.Sub(begin)))
}

func readFlags() {
	flag.Parse()

	if *showUsage {
		flag.Usage()
		os.Exit(0)
	}
}

func makeUuidFromString(str string) string {
	data := []byte(str)
	md5 := fmt.Sprintf("%x", md5.Sum(data))
	return md5[:8] + "-" + md5[8:12] + "-" + md5[12:16] + "-" + md5[16:20] + "-" + md5[20:32]
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func incrAvg(avg float64, inc float64) float64 {
	avg = avg + inc
	return avg / 2
}

func average(xs []float64) float64 {
	total := 0.0
	for _, v := range xs {
		total += v
	}
	return total / float64(len(xs))
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
