package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
 * @param queryName spark session and query name
 * @param parallelism number of cores to run spark. If none uses number == all available cores.
 *                    From testing, using larger number doesn't necessarily end up in spark running better.
 *                    Using 1 is good default option.
 * @param sparkTriggerInterval interval for spark save and report progress. During that time spark stops running query,
 *                             so it is really hurts performance. If using [[ServiceSource]] should be set large, since there is no commits.
 * @param sparkOptions options for spark, see available options at https://spark.apache.org/docs/latest/configuration.html
 */
case class SparkRunnerConfig(queryName: String,
                             parallelism: Option[Int],
                             sparkTriggerInterval: String,
                             sparkOptions: Map[String, String])

class SparkRunner(config: SparkRunnerConfig) {

  private val logger = LoggerFactory.getLogger(this.getClass.getName + s".${config.queryName}")

  private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  private lazy val spark = {
    logger.info(s"Spark runner config $config")
    val sparkConfig = new SparkConf().setAll(config.sparkOptions)

    logger.info(s"Initialize spark session with config ${sparkConfig.getAll.mkString("(", ", ", ")")}")

    val parallelism = config.parallelism.map(_.toString).getOrElse("*")
    val session = SparkSession.builder
      .master(s"local[$parallelism]")
      .appName(config.queryName)
      .config(sparkConfig)
      .getOrCreate()

    logger.info(s"Spark session config ${session.conf.getAll}")
    session
  }

  def start(): Unit = Future {
    logger.info(s"Starting spark query ${config.queryName}")
    val stream = spark.readStream
      .format(classOf[ServiceSourceProvider].getName)
      .load()

    val res = stream.withColumn("fooLength", length(col("foo")))

    res.writeStream
      .foreach(new ServiceSink)
      .trigger(Trigger.Continuous(config.sparkTriggerInterval))
      .start()
      .awaitTermination()
  }
}
