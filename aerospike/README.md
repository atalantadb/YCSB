<!--
Copyright (c) 2015-2026 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on Aerospike. 

### 1. Start Aerospike

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:aerospike-binding -am clean package

### 4. Provide Aerospike Connection Parameters

The following connection parameters are available.

  * `as.host` - The Aerospike cluster to connect to (default: `localhost`)
  * `as.port` - The port to connect to (default: `3000`)
  * `as.user` - The user to connect as (no default)
  * `as.password` - The password for the user (no default)
  * `as.namespace` - The namespace to be used for the benchmark (default: `ycsb`).
    It takes precedence over the `namespace` alias when both are supplied.
    The namespace must already exist on the server.
  * `namespace` - Alias used when `as.namespace` is absent.
  * `table` - The Aerospike set used by CoreWorkload (default: `usertable`).
    Each binding operation uses its `table` argument as the set name;
    `as.set` is not used.
  * `as.timeout` - Default socket and total command timeout for both single-record
    and batch operations, in milliseconds (default: `10000`).
  * `as.single.socketTimeout`, `as.single.totalTimeout` - Single-record timeout
    overrides. Each property takes precedence over `as.timeout` for that setting.
  * `as.batch.socketTimeout`, `as.batch.totalTimeout` - Batch timeout overrides.
    Each property takes precedence over `as.timeout` for that setting.
  * `as.timeoutDelay` - Time to drain timed-out sockets, in milliseconds
    (default: `3000`).
  * `as.read.maxRetries` - Single-record read retries (default: `1`).
    Single-record writes have retries disabled.
  * `as.batch.read.maxRetries`, `as.batch.write.maxRetries` - Batch read/write
    retries (default: `0` for both).
  * `as.maxConnsPerNode` - Maximum connections per node (default: `500`).
  * `as.connPoolsPerNode` - Connection pools per node (default: `4`).
  * `batchsize` - Positive number of records per binding operation (default: `1`).

Timeout precedence is: the corresponding single/batch override, then `as.timeout`,
then `10000` milliseconds. For example, `as.timeout=5000` and
`as.batch.totalTimeout=20000` set all socket timeouts and the single-record total
timeout to `5000`, and the batch total timeout to `20000`. Aerospike caps a socket
timeout at the total timeout when the latter is nonzero.

Add them to the workload or set them with the shell command, as in:

    ./bin/ycsb load aerospike -s -P workloads/workloada -p as.timeout=5000 >outputLoad.txt

### 5. Load Data and Run Tests

Load the data:

    ./bin/ycsb load aerospike -s -P workloads/workloada >outputLoad.txt

Run the workload test:

    ./bin/ycsb run aerospike -s -P workloads/workloada >outputRun.txt

### 6. Batch Operations

With `batchsize=1`, the binding uses ordinary single-record get, put, and delete
commands. Keys are unchanged, so data loaded by the upstream single-record
binding remains accessible.

With `batchsize=N` greater than `1`, each logical YCSB key expands into `N`
physical keys: `key_0`, `key_1`, ..., `key_(N-1)`. Insert, update, and delete use
Aerospike batch writes; read uses a batch get. The client groups the keys by
cluster node, so one binding call can produce multiple network requests.
Batch writes require Aerospike Java client 6.0+ and Aerospike Server 6.0+.

Insert and update generate field values once per YCSB operation and write the
same bytes to every record in that batch. A batch read checks that all requested
records exist and returns the fields of `key_0` in the YCSB result map. Other
records' field values are not compared or returned. Inserts use `CREATE_ONLY`,
so existing keys cause errors; updates use `UPDATE_ONLY`, so missing keys cause
errors.

For CoreWorkload load, `recordcount` counts logical keys. A successful load with
`recordcount=100000` and `batchsize=10` writes `1000000` physical records, including
ten copies of each generated field set. The thread count does not multiply the
record count. In load mode, `insertcount` overrides the number of inserts when
specified; `operationcount` and operation proportions do not control the load.

YCSB counts one binding call as one operation. With batching, reported throughput
is batches per second and latency is the time for the entire batch. When all
operations succeed without retries, multiply throughput by `batchsize` to obtain
physical record operations per second.

Batches are not atomic across records. Some records can succeed while others
fail, and successful changes are not rolled back. The binding returns
`Status.ERROR` if any record fails or is missing on read. CoreWorkload load stops
the affected loader thread after exhausting its insertion retries (disabled by
default). Retrying a partly successful insert can encounter `CREATE_ONLY` errors
for records that were already written.

Use the same namespace, table, and batch size for load and subsequent runs. A
dataset loaded with batching uses suffixed keys and cannot be accessed with the
single-record key layout. Scan is not implemented.

For example, load 1000 logical keys (10000 physical records), then read them:

```bash
./bin/ycsb load aerospike -s -P workloads/workload \
  -p as.namespace=ycsb -p table=t1 -p recordcount=1000 -p batchsize=10
./bin/ycsb run aerospike -s -P workloads/workload \
  -p as.namespace=ycsb -p table=t1 -p recordcount=1000 -p batchsize=10 \
  -p operationcount=1000 -p readproportion=1 -p updateproportion=0
```
