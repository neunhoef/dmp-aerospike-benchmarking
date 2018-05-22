package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
	// "github.com/davecgh/go-spew/spew"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type TStats struct {
	exit         bool
	readCount    int // write and read counts
	readError    int // write and read errors
	readTimeout  int // write and read timeouts
	readLat      int64
	greaterThan1 int
	greaterThan2 int
	greaterThan4 int
}

var reportChan chan *TStats

var host = flag.String("h", "10.150.73.10", "Aerospike server hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server port number.")
var connQueueSize = flag.Int("queueSize", 4096, "Maximum number of connections to pool.")
var namespace = flag.String("n", "test", "Aerospike namespace.")
var set = flag.String("s", "aerospike", "Aerospike set name.")
var benchMode = flag.String("m", "query", "query/seed. Seed to insert records, query to benchmark")
var keyCount = flag.Int("k", 100000, "How many CID users to insert in Seed mode, or the range UUIDs to query that have already been seeded.")
var didsPerCid = flag.Int("d", 3, " How many devices per CID to insert in Seed mode or to randomly select while benchmarking.")
var concurrency = flag.Int("c", 32, "Number of goroutines for querying.")
var timeLimit = flag.Int("t", 60, "Number of seconds to run benchmark.")
var reportInterval = flag.Int("i", 10, "Print a status report every x seconds. Should be < Time Limit")
var showUsage = flag.Bool("u", false, "Show usage information.")

var startExecutionTime time.Time

func main() {
	log.Println("[ Aerospike Benchmark ]")
	clientPolicy := as.NewClientPolicy()
	// cache lots  connections
	clientPolicy.ConnectionQueueSize = *connQueueSize
	// clientPolicy.User = *user
	// clientPolicy.Password = *password
	clientPolicy.Timeout = 10 * time.Second
	client, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	panicOnError(err)
	log.Println("Nodes Found:", client.GetNodeNames())
	log.Println("-------------------------------------------------------------------------------------")

	readFlags()

	if *benchMode == "seed" {
		seedDB(client, *keyCount, *didsPerCid)
		os.Exit(0)
	}

	readPolicy := as.NewPolicy()

	reportChan = make(chan *TStats, 4*(*concurrency))

	startExecutionTime = time.Now()

	for i := 0; i < *concurrency; i++ {
		go runQueryBenchmark(client, readPolicy)
	}

	var intervalReqCount, intervalErrCount, intervalTOCount, intervalOneMs, intervalTwoMs, intervalFourMs int
	var totalReqCount, totalErrCount, totalTOCount, totalOneMs, totalTwoMs, totalFourMs int
	var intervalMinLat, intervalMaxLat int64
	var totalMinLat, totalMaxLat int64
	lastReportTime := time.Now()

	for {
		select {
		case stats := <-reportChan:
			intervalReqCount += stats.readCount
			intervalErrCount += stats.readError
			intervalTOCount += stats.readTimeout
			intervalMinLat = min(intervalMinLat, stats.readLat)
			intervalMaxLat = max(intervalMaxLat, stats.readLat)
			intervalOneMs += stats.greaterThan1
			intervalTwoMs += stats.greaterThan2
			intervalFourMs += stats.greaterThan4

			if time.Now().Sub(lastReportTime) >= (time.Duration(*reportInterval) * time.Second) {

				log.Println("QPS: " + fmt.Sprintf("%v", (math.Round(float64(intervalReqCount)/float64(*reportInterval)))) +
					" | " + "Min: " + fmt.Sprintf("%v", intervalMinLat) +
					"\xC2\xB5s | Max: " + fmt.Sprintf("%v", intervalMaxLat) +
					"\xC2\xB5s | >1ms: " + fmt.Sprintf("%v", (math.Round(float64(intervalOneMs)/float64(intervalReqCount)/0.0001)/100)) +
					"% | >2ms: " + fmt.Sprintf("%v", (math.Round(float64(intervalTwoMs)/float64(intervalReqCount)/0.0001)/100)) +
					"% | >4ms: " + fmt.Sprintf("%v", (math.Round(float64(intervalFourMs)/float64(intervalReqCount)/0.0001)/100)) +
					"% | Timeouts: " + fmt.Sprintf("%v", intervalTOCount) + " (" + fmt.Sprintf("%v", (math.Round(float64(intervalTOCount)/float64(intervalReqCount)/0.0001)/100)) + "%)" +
					"| Errors: " + fmt.Sprintf("%v", intervalErrCount) + " (" + fmt.Sprintf("%v", (math.Round(float64(intervalErrCount)/float64(intervalReqCount)/0.0001)/100)) + "%)")
				lastReportTime = time.Now()

				// reset interval counters, add to total.
				totalReqCount += intervalReqCount
				totalErrCount += intervalErrCount
				totalTOCount += intervalTOCount
				totalMinLat = min(totalMinLat, intervalMinLat)
				totalMaxLat = max(totalMaxLat, intervalMaxLat)
				totalOneMs += intervalOneMs
				totalTwoMs += intervalTwoMs
				totalFourMs += intervalFourMs

				intervalReqCount = 0
				intervalErrCount = 0
				intervalTOCount = 0
				intervalMinLat = 0
				intervalMaxLat = 0
				intervalOneMs = 0
				intervalTwoMs = 0
				intervalFourMs = 0
			}
			if stats.exit {
				log.Println("[ Summary ]")
				log.Println("-------------------------------------------------------------------------------------")
				log.Println("QPS: " + fmt.Sprintf("%v", (math.Round(float64(totalReqCount)/float64(*timeLimit)))) + " | " + "Min: " + fmt.Sprintf("%v", totalMinLat) +
					"\xC2\xB5s | Max: " + fmt.Sprintf("%v", totalMaxLat) +
					"\xC2\xB5s | >1ms: " + fmt.Sprintf("%v", (math.Round(float64(totalOneMs)/float64(totalReqCount)/0.0001)/100)) +
					"% | >2ms: " + fmt.Sprintf("%v", (math.Round(float64(totalTwoMs)/float64(totalReqCount)/0.0001)/100)) +
					"% | >4ms: " + fmt.Sprintf("%v", (math.Round(float64(totalFourMs)/float64(totalReqCount)/0.0001)/100)) +
					"% | Timeouts: " + fmt.Sprintf("%v", totalTOCount) + " (" + fmt.Sprintf("%v", (math.Round(float64(totalTOCount)/float64(totalReqCount)/0.0001)/100)) + "%)" +
					"| Errors: " + fmt.Sprintf("%v", totalErrCount) + " (" + fmt.Sprintf("%v", (math.Round(float64(totalErrCount)/float64(totalReqCount)/0.0001)/100)) + "%)")

				return
			}
		}
	}

}

func runQueryBenchmark(client *as.Client, policy *as.BasePolicy) {
	for {
		if (time.Now().Sub(startExecutionTime) / time.Second) > time.Duration(*timeLimit) {
			// Times up, send exit signal
			reportChan <- &TStats{true, 0, 0, 0, 0, 0, 0, 0}
			return
		}

		did := getRandomDid(*keyCount, *didsPerCid)

		begin := time.Now()

		key, err := as.NewKey(*namespace, *set, "DID:"+did)
		if err != nil {
			panicOnError(err)
		}
		record, err := client.Get(policy, key)
		rLat := int64(time.Now().Sub(begin) / time.Microsecond)

		recordStats(rLat, err)
		if err != nil {
			continue
		}

		read_cid := record.Bins["CID"].(string)

		type Data struct {
			Devices []string `json:"devices" as:"Devices"`
		}
		rec := &Data{}

		begin = time.Now()
		key, err = as.NewKey(*namespace, *set, "CID:"+read_cid)
		err = client.GetObject(nil, key, rec)

		rLat = int64(time.Now().Sub(begin) / time.Microsecond)

		recordStats(rLat, err)

		begin = time.Now()
		var batch_keys []*as.Key
		for _, device_item := range rec.Devices {
			item_key, _ := as.NewKey(*namespace, *set, "DID:"+device_item)
			batch_keys = append(batch_keys, item_key)
		}

		_, err = client.BatchGet(nil, batch_keys, "Sources")
		rLat = int64(time.Now().Sub(begin) / time.Microsecond)

		recordStats(rLat, err)
	}
}

func seedDB(client *as.Client, cidCount int, didsPerCid int) {

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
			key, err := as.NewKey(*namespace, *set, "DID:"+did)
			panicOnError(err)

			bins := as.BinMap{"CID": cid}

			err = client.Put(WritePolicy, key, bins)
			panicOnError(err)
		}

		// Write CID
		key, err := as.NewKey(*namespace, *set, "CID:"+cid)
		panicOnError(err)
		bins := as.BinMap{"Devices": dids}
		err = client.Put(WritePolicy, key, bins)
		panicOnError(err)
	}

	log.Println("Seeded " + fmt.Sprintf("%v", cidCount) + " CIDs in: " + fmt.Sprintf("%v", time.Now().Sub(begin)))
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
	if (a < b && a > 0) || b == 0 {
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

func getRandomDid(cidMax int, didMax int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	randCid := rand.Intn(cidMax)
	randDid := rand.Intn(didMax)
	return makeUuidFromString(strconv.Itoa(randCid) + "+" + strconv.Itoa(randDid))
}

func recordStats(rLat int64, err error) {
	var toErr, errErr int

	if err != nil {
		if ae, ok := err.(ast.AerospikeError); ok && ae.ResultCode() == ast.TIMEOUT {
			toErr = 1 // timeout
		} else {
			errErr = 1 // error
		}
	}

	if rLat > 4000 {
		reportChan <- &TStats{false, 1, errErr, toErr, rLat, 1, 1, 1}
	} else if rLat > 2000 {
		reportChan <- &TStats{false, 1, errErr, toErr, rLat, 1, 1, 0}
	} else if rLat > 1000 {
		reportChan <- &TStats{false, 1, errErr, toErr, rLat, 1, 0, 0}
	} else {
		reportChan <- &TStats{false, 1, errErr, toErr, rLat, 0, 0, 0}
	}

}
