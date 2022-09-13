# mysqlsct

 mysqlsct is a benchmark tool to test strict consistency read, supports MySQL MGR,PolarDB,Aurora,TiDB,etc

## Get the source codes
```shell
git clone https://github.com/sunashe/mysqlsct.git
```

## Compile mysqlsct

```shell
yum install mariadb-devel.x86_64 -y
cmake .
make
```

## Usage 

```shell
mysqlsct \
--host-rw=127.0.0.1 \
--host-ro=127.0.0.1 \
--port-rw=3407 \
--port-ro=3408 \
--user=sunashe \
--password=**** \
--iterations=100000 \
--table-cnt=1 \
--table-size=1000 \
--concurrency=1 \
--database=sct \
--sc-gap-us=0 \
--report-interval=2
```

e.g. 
```shell
--host-rw=127.0.0.1 \
--host-ro=127.0.0.1 \
--port-rw=3407 \
--port-ro=3408 \
--user=sunashe \
--password=**** \
--iterations=100000 \
--table-cnt=1 \
--table-size=1000 \
--concurrency=1 \
--database=sct \
--sc-gap-us=0 \
--report-interval=2

Input parameters: 
host-rw: 127.0.0.1
host-ro: 127.0.0.1
database: sct
port-rw: 3407
port-ro: 3407
user: sunashe
password: ****
iterations: 100000
table-cnt: 1
table-size: 1000
sc-gap-us: 0
report-interval: 2
detail-log: 0
concurrency: 1
###########################################
Strict consistency tps: 1896, failed qps: 0
Strict consistency tps: 2511, failed qps: 0
Strict consistency tps: 2624, failed qps: 0
Strict consistency tps: 2574, failed qps: 0
Strict consistency tps: 2607, failed qps: 0
Strict consistency tps: 2545, failed qps: 0
...
Strict consistency tps: 2529, failed qps: 0
Strict consistency tps: 2515, failed qps: 0
Strict consistency tps: 2171, failed qps: 0
Test strict consistency cnt: 100000, failed cnt: 0
```

parameter specification
```shell
$ ./mysqlsct --help
Usage: mysqlsct [OPTIONS]
-?      --help          Display this help and exit.
-v      --version       Output version information and exit.
-h      --host-rw       mysql RW node host.
-H      --host-ro       mysql RO node host.
-D      --database      mysql database.
-P      --port-rw       mysql RW node port.
-o      --port-ro       mysql RO node port.
-u      --user  mysql user.
-p      --password      mysql password.
-i      --iterations    number of times to run the tests.
-T      --table-cnt     number of tables to test.
-t      --table-size    table size.
-s      --sc-gap-us     time(us) to sleep after write.
-r      --report-interval       periodically report intermediate statistics with a specified interval in seconds.
-k      --detail-log    print detail error log.
-c      --concurrency   number of threads to use.
```
