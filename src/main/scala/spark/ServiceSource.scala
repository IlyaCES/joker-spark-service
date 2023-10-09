package spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._


class ServiceSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val numPartitions = options.getInt("numPartitions", SparkSession.active.sparkContext.defaultParallelism)

    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$numPartitions'. The option 'numPartitions' must be > 0")
    }
    logInfo(s"Configured numPartitions for ServiceSourceProvider is $numPartitions")

    new ServiceSourceTable(numPartitions, getSchema)
  }

  private def getSchema: StructType = {
    val fields = Features.schemaFields
    val requestIdField = StructField("requestId", StringType, nullable = false)
    val timestampField = StructField("timestamp(latency)", LongType, nullable = false)
    val schema = StructType(fields :+ requestIdField :+ timestampField)
    logInfo(s"Created schema $schema")
    schema
  }

  override def shortName(): String = "ServiceSourceProvider"

}

class ServiceSourceTable(numPartitions: Int, schema: StructType) extends Table with SupportsRead with Logging {

  override def name(): String = {
    s"ServiceSourceStream"
  }

  override def schema(): StructType = schema

  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.CONTINUOUS_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      ???
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream =
      new ServiceContinuousStream(numPartitions)
  }
}

class ServiceContinuousStream(numPartitions: Int) extends ContinuousStream {
  override def planInputPartitions(offset: Offset): Array[InputPartition] = {
    Array.fill(numPartitions)(NoPartition)
  }

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = new ServiceStreamContinuousReaderFactory

  override def mergeOffsets(partitionOffsets: Array[PartitionOffset]): Offset = {
    NoStreamOffset
  }

  override def initialOffset(): Offset = NoStreamOffset

  override def deserializeOffset(s: String): Offset = NoStreamOffset

  override def commit(offset: Offset): Unit = {}

  override def stop(): Unit = {}
}

class ServiceStreamContinuousReaderFactory extends ContinuousPartitionReaderFactory {
  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    new ServiceStreamContinuousPartitionReader
  }
}

class ServiceStreamContinuousPartitionReader extends ContinuousPartitionReader[InternalRow] with Logging {
  override def getOffset: PartitionOffset = NoPartitionOffset

  private var currentRow: InternalRow = null

  override def next(): Boolean = {
    val request = SparkService.getNextRequestBlocking

    currentRow = request.toInternalRow
    true
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}

object NoPartitionOffset extends PartitionOffset

object NoStreamOffset extends Offset {
  override def json(): String = ""
}

object NoPartition extends InputPartition
