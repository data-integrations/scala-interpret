package co.cask.plugin.interpret

import co.cask.cdap.api.data.format.StructuredRecord
import org.apache.spark.rdd.RDD

/**
 * Transforms one rdd into another
 */
trait RDDTransform {

  def transform(input: RDD[StructuredRecord]): RDD[StructuredRecord]
}
