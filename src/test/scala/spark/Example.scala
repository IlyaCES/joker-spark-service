package spark

import org.scalatest.{FlatSpec, Matchers}

import java.util.concurrent.CompletableFuture

class Example extends FlatSpec with Matchers {

  "Example" should "compute length of a string" in {
    // Start spark query
    val config = SparkRunnerConfig("testQuery", Some(1), "1 hour", Map.empty)
    val sparkRunner = new SparkRunner(config)
    sparkRunner.start()

    // Now we can send requests
    val features = Features("SomeString")
    val reply: CompletableFuture[Double] = SparkService.request(features)

    val result = reply.get()
    result shouldEqual 10.0
  }
}
