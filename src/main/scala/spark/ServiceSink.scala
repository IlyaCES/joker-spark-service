package spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ForeachWriter, Row}

class ServiceSink extends ForeachWriter[Row] with Logging {
  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(value: Row): Unit = {
    val result = value.getAs[Int]("fooLength")
    val resultFuture = value.getAs[String]("requestId")
    val timestamp = value.getAs[Long]("timestamp(latency)")
    SparkService.reply(result, resultFuture, timestamp)
  }

  override def close(errorOrNull: Throwable): Unit = {}
}

