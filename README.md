# Spark service
Simple example to show how to integrate spark ml pipeline 
into scala (or java) application with a millisecond latency

Example:
```scala
    // Start spark query
    val config = SparkRunnerConfig("testQuery", Some(1), "1 hour", Map.empty)
    val sparkRunner = new SparkRunner(config)
    sparkRunner.start()

    // Now we can send requests
    val features = Features("SomeString")
    val reply: CompletableFuture[Double] = SparkService.request(features)

    val result = reply.get()
    result shouldEqual 10.0
```