package spark

import org.scalatest.{FlatSpec, Matchers}

class SparkServiceTest extends FlatSpec with Matchers {

  private val testFeatures = Features("SomeString")
  private val expectedReplyValue = "SomeString".length

  "SparkService" should "process request" in {
    val reply = SparkService.request(testFeatures)

    emulateRequestProcessing()

    reply.get() shouldEqual expectedReplyValue
  }

  "SparkService" should "timeout request if request was not processed in given time" in {
    val reply = SparkService.request(testFeatures, timeoutMs = 40)
    SparkService.getNextRequestBlocking
    Thread.sleep(50)

    reply.isCompletedExceptionally should be (true)
  }

  "SparkService" should "not timeout request if request was processed in given time" in {
    val reply = SparkService.request(testFeatures, timeoutMs = 100)

    Thread.sleep(50)
    emulateRequestProcessing()

    reply.get() shouldEqual expectedReplyValue
  }

  private def emulateRequestProcessing(): Unit = {
    val request = SparkService.getNextRequestBlocking
    SparkService.reply(expectedReplyValue, request.requestId, request.timestamp)
  }
}
