package spark

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


class Features private(val features: Array[Any])

object Features {

  def apply(feature: String): Features = {
    new Features(Array(UTF8String.fromString(feature)))
  }

  def schemaFields: Array[StructField] = {
    DataType.fromJson(schemaJson).asInstanceOf[StructType].fields
  }

  val defaultFeatures: Features = new Features(Array("testString"))

  private val schemaJson = """{"type":"struct","fields":[{"name":"foo","type":"string","nullable":true,"metadata":{}}]}"""
}
