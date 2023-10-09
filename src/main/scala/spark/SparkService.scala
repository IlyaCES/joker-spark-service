package spark

import io.micrometer.core.instrument.{Counter, Gauge, Metrics, Timer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent._


case class Request(features: Features, requestId: String, timestamp: Long) {
  def toInternalRow: InternalRow = {
    val featuresLength = features.features.length
    val featuresWithIdAndTimestamp = new Array[Any](featuresLength + 2)
    features.features.copyToArray(featuresWithIdAndTimestamp)
    featuresWithIdAndTimestamp(featuresLength) = UTF8String.fromString(requestId)
    featuresWithIdAndTimestamp(featuresLength + 1) = timestamp

    InternalRow.fromSeq(featuresWithIdAndTimestamp)
  }
}


object SparkService {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val requestQueueCapacity = 3000
  private val requestsQueue = new LinkedBlockingQueue[Request](requestQueueCapacity)
  private val replyMap = new ConcurrentHashMap[String, CompletableFuture[Double]]()
  private val timeoutQueue = new DelayQueue[IdWithTimeout]()

  private val requestsCounter = Counter.builder("SparkService.requestsCounter").register(Metrics.globalRegistry)
  private val requestsQueueLimitExceededCounter = Counter.builder("SparkService.requestsQueueLimitExceededCounter").register(Metrics.globalRegistry)
  private val timeoutCounter = Counter.builder("SparkService.timeoutCounter").register(Metrics.globalRegistry)
  private val replyFutureNotFoundCounter = Counter.builder("SparkService.replyFutureNotFoundCounter").register(Metrics.globalRegistry)
  private val timeoutTimer = Timer.builder("SparkService.timeoutTimer").register(Metrics.globalRegistry)
  private val successTimer = Timer.builder("SparkService.successTimer").register(Metrics.globalRegistry)

  Gauge.builder(
    "SparkService.requestQueueGauge",
    requestsQueue,
    (queue: LinkedBlockingQueue[Request]) => queue.size()
  ).register(Metrics.globalRegistry)

  Gauge.builder(
    "SparkService.replyMapGauge",
    replyMap,
    (map: ConcurrentHashMap[String, CompletableFuture[Double]]) => map.size()
  ).register(Metrics.globalRegistry)

  // Task to remove request that were not processed by spark in given time
  private val expiredRequestsCleanerTask = new Runnable {
    override def run(): Unit = {
      logger.info("Start cleaner task for expired requests")
      while (true) {
        val expired = timeoutQueue.take()
        Option(replyMap.remove(expired.id)).foreach(resultFuture => {
          val timeout = expired.timeoutMs
          val timeoutException = new TimeoutException(s"Spark failed to process request in $timeout ms")
          resultFuture.completeExceptionally(timeoutException)
          logger.error(s"Spark failed to process request in $timeout ms")
          timeoutCounter.increment()
        })
      }
    }
  }
  new Thread(expiredRequestsCleanerTask).start()

  /** Task to heat up spark. Spark usually starts very slow initially, but then builds up speed. */
  private val heatingTask = new Runnable {
    override def run(): Unit = {
      logger.info("Start spark heating with 50 requests")
      val features = Array.fill(50)(Features.defaultFeatures)
      features.foreach(features => {
        request(features).get()
      })
      logger.info("End spark heating")
    }
  }

  /** Request processing by spark pipeline.
   *
   * @param features features to process
   * @param timeoutMs if spark fails to process request in given time, return future is completed exceptionally.
   *                  if this param equals 0, then there is no timeout. Default is 0.
   * @return future that will be completed with spark processing result, or completed exceptionally in case of timeout.
   */
  def request(features: Features, timeoutMs: Int = 0): CompletableFuture[Double] = {
    val request = buildRequest(features)
    val resultFuture = new CompletableFuture[Double]()
    requestsCounter.increment()

    val added = requestsQueue.offer(request)
    if (added) {
      replyMap.put(request.requestId, resultFuture)

      if (timeoutMs != 0) {
        withTimeout(timeoutMs, request)
      }
    } else {
      requestsQueueLimitExceededCounter.increment()
      resultFuture.completeExceptionally(new IllegalStateException("Requests queue if full"))
    }

    resultFuture
  }

  private[spark] def getNextRequestBlocking: Request = requestsQueue.take()

  /** Method to reply to request. Called by spark stream.
   *
   * @param result spark processing result
   * @param requestId identifier of request
   * @param timestamp timestamp of request
   */
  private[spark] def reply(result: Double, requestId: String, timestamp: Long): Unit = {
    logger.trace(s"Replying with $result to requestId=$requestId")
    val elapsedTime = System.nanoTime() - timestamp
    Option(replyMap.remove(requestId)) match {
      case Some(resultFuture) =>
        resultFuture.complete(result)
        logSuccess(elapsedTime)
      case None =>
        logFailure(requestId, elapsedTime)
    }
  }

  def heating(): Unit = {
    new Thread(heatingTask).start()
  }

  private def logSuccess(elapsedTime: Long): Unit = {
    successTimer.record(elapsedTime, TimeUnit.NANOSECONDS)
    logger.trace("Completed resultFuture and removed it from resultMap")
    logger.trace(s"Elapsed time ${TimeUnit.NANOSECONDS.toMillis(elapsedTime)} ms")
  }

  private def logFailure(requestId: String, elapsedTime: Long): Unit = {
    replyFutureNotFoundCounter.increment()
    timeoutTimer.record(elapsedTime, TimeUnit.NANOSECONDS)
    logger.error(s"resultFuture for requestId=$requestId is not found, probably because of timeout")
  }

  private def withTimeout(timeoutMs: Int, request: Request): Unit = {
    val idWithTimeout = IdWithTimeout(request.requestId, request.timestamp, timeoutMs)
    timeoutQueue.put(idWithTimeout)
  }

  private def buildRequest(features: Features): Request = {
    val uuid = UUID.randomUUID().toString
    val timestamp = System.nanoTime()
    Request(features, uuid, timestamp)
  }
}

/** Class used for requests timeout logic */
case class IdWithTimeout(id: String, timestamp: Long, timeoutMs: Long) extends Delayed {
  private val expireTimeMs = timestamp + TimeUnit.MILLISECONDS.toNanos(timeoutMs)

  override def getDelay(unit: TimeUnit): Long = {
    val delay = expireTimeMs - System.nanoTime()
    unit.convert(delay, TimeUnit.NANOSECONDS)
  }

  override def compareTo(delayed: Delayed): Int = {
    (expireTimeMs - delayed.asInstanceOf[IdWithTimeout].expireTimeMs).toInt
  }
}
