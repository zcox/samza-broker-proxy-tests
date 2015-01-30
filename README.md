# samza-broker-proxy-tests
Test showing that Logback's AsyncAppender causes BrokerProxy.stop to block forever.

Usage:

```
./sbt test
```

If AsyncAppender is used in logback.xml, then tests will fail.

We do not yet know why AsyncAppender interferes with BrokerProxy.stop, but this test makes it easy to observe. Current hypothesis is that the `thread.interrupt` is swallowed somewhere in AsyncAppender.

When you use AsyncAppender, the BrokerProxy.stop blocking issue causes problems both with 1) Samza container will not shut down, and 2) key-value stores will not restore from changelog on startup, causing the task to not start.
