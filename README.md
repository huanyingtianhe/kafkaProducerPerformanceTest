# kafkaProducerPerformanceTest
perf/stress test for kafka 0.8.1 producer

the 0.8.1 kafka client only support sync write, so we do not support async write in the test.

we can specify the partitions in conf.json for perf test, have fun!
