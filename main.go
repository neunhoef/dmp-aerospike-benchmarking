package main

import (
	"fmt"
	. "github.com/aerospike/aerospike-client-go"
	// "github.com/davecgh/go-spew/spew"
	// "github.com/satori/go.uuid"
	"log"
	"math"
	// "reflect"
	"time"
)

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func Round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	_div := math.Copysign(div, val)
	_roundOn := math.Copysign(roundOn, val)
	if _div >= _roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

func main() {
	// define a client to connect to
	client, err := NewClient("10.150.73.10", 3000)
	panicOnError(err)

	var WritePolicy = NewWritePolicy(0, 0)
	WritePolicy.Timeout = 10000 * time.Millisecond
	WritePolicy.SocketTimeout = 10000 * time.Millisecond
	// return
	// Policy{
	//     Priority:            Priority.DEFAULT,
	//     Timeout:             10000 * time.Millisecond, // no timeout
	//     MaxRetries:          2,
	//     SleepBetweenRetries: 500 * time.Millisecond,
	//     SleepMultiplier:     1.5
	//  	}

	dids := []string{"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
		"44444444-4444-4444-4444-444444444444",
		"55555555-5555-5555-5555-555555555555"}

	cid := "88888888-8888-8888-8888-888888888888"
	for _, did := range dids {
		key, err := NewKey("cid", "devices", "DID:"+did)
		panicOnError(err)

		bins := BinMap{
			"CID":     cid,
			"Sources": []interface{}{"Liveramp", "An Other Source"},
		}

		err = client.Put(WritePolicy, key, bins)
		panicOnError(err)
	}

	total_begin := time.Now()
	key, err := NewKey("cid", "devices", "CID:"+cid)
	panicOnError(err)

	bins := BinMap{"Devices": dids}

	err = client.Put(WritePolicy, key, bins)
	panicOnError(err)

	begin := time.Now()
	policy := NewPolicy()

	key, err = NewKey("cid", "devices", "DID:"+dids[0])
	record, err := client.Get(policy, key)
	panicOnError(err)
	end := time.Now()
	log.Println("Query CID from DID in: " + fmt.Sprintf("%v", end.Sub(begin)))

	read_cid := record.Bins["CID"].(string)

	type Data struct {
		Devices []string `json:"devices" as:"Devices"`
	}
	rec := &Data{}

	begin = time.Now()
	key, err = NewKey("cid", "devices", "CID:"+read_cid)
	err = client.GetObject(nil, key, rec)
	panicOnError(err)
	end = time.Now()

	log.Println("Query Device list from CID in: " + fmt.Sprintf("%v", end.Sub(begin)))

	begin = time.Now()
	var batch_keys []*Key
	for _, device_item := range rec.Devices {
		item_key, _ := NewKey("cid", "devices", "DID:"+device_item)
		batch_keys = append(batch_keys, item_key)
	}

	_, err = client.BatchGet(nil, batch_keys, "Sources")
	panicOnError(err)
	end = time.Now()
	log.Println("Batch Query DIDs in Device list: " + fmt.Sprintf("%v", end.Sub(begin)))

	total_end := time.Now()
	log.Println("Total time for read requests: " + fmt.Sprintf("%v", total_end.Sub(total_begin)))

}
