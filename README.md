# kafkaProducerPerformanceTest
perf/stress test for kafka 0.8.1 producer

## conf description

we support kafka 0.8.1 producer old config, if you want to specify kafka 
config, please modify conf.json, the producer conf seen in the link: 
https://kafka.apache.org/081/documentation.html#producerconfigs

there are some useful config not included in the kafka standard need to explain

we can specify the partitions in conf.json for perf test, the partition support two types:

interval:
>"partitions": ["1-6"]

it means, we want to do perf test on partition [1,2,3,4,5,6]

single point
>"partitions": ["1"]

we can combine it together in the conf.json:

>"partitions": ["1-6", "9"]

By the way, we can specify the topics in the conf.json:
>"topics": "test"

if you have multiple topics, Put them together with commas
>"topics": "test1, test2"
                   


## Build

we can generate the jar with command: mvn install

if you want to run test, please run: mvn test

## Usage
```
java -jar kafkaProducerPerformanceTest-1.0-SNAPSHOT.jar [option]

 -d,--debug <arg>          show debug info
 
 -k,--kafka <arg>          whether send to kafka, else eventserver
 
 -n,--num-message <arg>    number of message
 
 -s,--size-message <arg>   size of message
 
 -t,--threads <arg>        thread number
```
