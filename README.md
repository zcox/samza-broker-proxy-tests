# samza-broker-proxy-tests
Test showing BrokerProxy.stop blocks and won't stop its thread

Usage:

```
./sbt test
```

After a bajillion log lines, the test will hang for approx 60 seconds, then it will show a failure.
