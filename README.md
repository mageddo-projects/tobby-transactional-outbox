# tobby-transactional-outbox
Tobby - A transactional outbox implementation

![Tobby](https://i.imgur.com/SOomiFq.png)

## Performance Tests

Tests made on Postgres 10 using a producer with 50 java threads producing messages while the replicator was
replicating at the same time
using **DELETE** idempotency strategy (which is the faster one) achieved a result of 8,300 k/s messages committed by
the producer to the database, also the replicator read the records and committed to the Kafka broker, in other
words you will be able to process around  500 thousand messages per minute or 30 million messages per hour using
Tobby while your have the Transactional Outbox guarantee,
these numbers should increase considerably at a decent production environment hardware.

### Specs
Below the computer used to perform the tests, all the required software ran at the same machine, 
CPU and memory usage were pretty small (below 20%), then computer will less CPU and RAM with a better SSD should
show better results. 

#### Processor
```
Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
```

#### Memory
4 RAM slots with te following specs
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
bus info: scsi@4:0.0.0
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
