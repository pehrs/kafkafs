kafkafs - Kafka Producer FUSE File System
==============================================

Copyright (c) 2014, [Matti Pehrs](http://pehrs.com)

[https://github.com/pehrs/kafkafs](https://github.com/pehrs/kafkafs)

kafkafs is a [FUSE](http://fuse.sourceforge.net/) file system that produces events into [Apache Kafka](http://kafka.apache.org/).

# Usage

## Requirements

    The GNU toolchain
    GNU make
    librdkafka-dev
    libfuse-dev

## Building

    make

## Try KafkaFS

Start the KafkaFS:

    > mkdir -p /mnt/kafkafs
    > ./kafkafs -o debug -o brokers=localhost:9092 /mnt/kafkafs

Then use syslog to see the progress:

    > tail -f /var/log/syslog

Witht the `-o debug` option the KafkaFS will print debug statements to syslog so you can follow the progress.

Now, lets send some messages to the topic 'sample':

    > echo "First Message" > /mnt/kafkafs/topic/sample
    > echo "Second Message" > /mnt/kafkafs/topic/sample

Whatever name is used after the /topic dirname will be used as the topic target.

To see some status of the KafkaFS just cat the kafka-status file in the root.

    > cat /mnt/kafkafs/kafka-status
    No msgs: 700293
    Msgs/sec: 1002.73662



