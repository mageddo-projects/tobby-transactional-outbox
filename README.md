# tobby-transactional-outbox
Tobby - A transactional outbox implementation

![Tobby](https://i.imgur.com/SOomiFq.png)

## Getting Started

### Vanilla

### Spring

##### Configure the deps
```bash
implementation 'com.mageddo.tobby-transactional-outbox:spring:1.4.0'
```

##### Configure the database tables
You need 2 new tables, see migration [configuration directory][2] and choose one compatible with your DB

##### Configure Spring

```java
@SpringBootApplication
@EnableTobbyTransactionalOutbox
public class App {
  public static void main(String[] args){
    SpringApplication.run(App.class, args);
  }
}
```

Not just inject the old and good `KafkaTemplate` and use it

```
@Autowired 
KafkaTemplate kafkaTemplate;

KafkaTemplate.send(...);
```

## Requirements
* Java 8+
* Kafka 0.11+
* A relational database with JDBC driver, the following were tested
   * Postgres
   * MySQL
   * Oracle
   * SQL Server
   * HSQLDB
   * H2

## Performance Tests

Tests made on Postgres 10 using a producer with 50 java threads producing messages while the replicator was
replicating at the same time
using **DELETE** idempotency strategy (which is the faster one) achieved a result of **8,300/s** 
(eight thousand per second) messages committed by
the producer to the database, also the replicator read the records, send and committed to the Kafka broker.

In other words, you will be able to process around  500 thousand messages per minute or 30 million messages per hour
using Tobby having the Transactional Outbox guarantee,
these numbers should increase considerably at a decent production environment hardware.

You can reproduce this test right now by execute the main classes at [this package][1].

### Specs used at the test
Below the computer used to perform the tests, all the required software ran at the same machine, 
CPU and memory usage were pretty small (below 20%), so a computer with less CPU and RAM with a better SSD should
show better results. 

#### Processor
```
Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
```

#### Memory
4 RAM slots with the following specs
```
Total Width: 64 bits
Data Width: 64 bits
Size: 8192 MB
Form Factor: DIMM
Set: None
Locator: ChannelB-DIMM1
Bank Locator: BANK 3
Type: DDR4
Type Detail: Synchronous
Speed: 2666 MT/s
Manufacturer: Kingston
Serial Number: 0D2BE622
Asset Tag: 9876543210
Part Number: KHX2666C16/8G
Rank: 1
Configured Clock Speed: 2666 MT/s
Minimum Voltage: 1.2 V
Maximum Voltage: 1.2 V
Configured Voltage: 1.2 V
```

#### SSD
```
product: WDC WDS240G2G0A-
vendor: Western Digital
bus info: scsi@4:1.4.0
serial: 182394807269
size: 223GiB (240GB)
capabilities: partitioned partitioned:dos
configuration: ansiversion=5 logicalsectorsize=512 sectorsize=512 signature=b088b822
```


Read speed test
```
$ sudo hdparm -Tt /dev/sde
Timing cached reads:   35110 MB in  1.99 seconds = 17624.25 MB/sec
Timing buffered disk reads: 1248 MB in  3.00 seconds = 415.90 MB/sec
```

Write speed test

```
10240+0 records in
10240+0 records out
83886080 bytes (84 MB, 80 MiB) copied, 0.523091 s, 160 MB/s
```

[1]: https://github.com/mageddo-projects/tobby-transactional-outbox/tree/f1ad98e/src/test/java/apps
[2]: src/main/resources/com/mageddo/tobby/db
