# Aerospike Benchmark Tool
This is development repo to benchmark aerospike performance is sufficient for IDM.

## Usage
```
  -c int
        Number of goroutines for querying. (default 32)
  -d int
        How many devices per CID to insert in Seed mode. (default 3)
  -h string
        Aerospike server seed hostnames or IP addresses (default "10.150.73.10")
  -i int
        Print a status report every x seconds. Should be < Time Limit (default 10)
  -k int
        How many CID users to insert in Seed mode, or the range UUIDs to query that have already been seeded. (default 100000)
  -m string
        query/seed. Seed to insert records, query to benchmark (default "query")
  -n string
        Aerospike namespace. (default "cid")
  -p int
        Aerospike server seed hostname or IP address port number. (default 3000)
  -s string
        Aerospike set name. (default "devices")
  -t int
        Number of seconds to run benchmark. (default 60)
  -u    Show usage information.
```
  
## Example
Run a 24 hour test, with interval reports every hour:
```bash
./dmp-aerospike-benchmarking -h localhost -i 3600 -t 86400 
```
