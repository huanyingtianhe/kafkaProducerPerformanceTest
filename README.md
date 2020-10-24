# kafkaProducerPerformanceTest
perf/stress test for kafka 0.8.1 producer

we support 0.8.1 kafka producer old config, if you want to add config, please modify conf.json with json format

we can specify the partitions in conf.json for perf test, just have fun!


usage: java -jar kafkaProducerPerformanceTest-1.0-SNAPSHOT.jar [option]

 -d,--debug <arg>          show debug info
 
 -k,--kafka <arg>          whether send to kafka, else eventserver
 
 -n,--num-message <arg>    number of message
 
 -s,--size-message <arg>   size of message
 
 -t,--threads <arg>        thread number

