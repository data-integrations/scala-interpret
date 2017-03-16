package co.cask.plugin.interpret

import co.cask.cdap.api.annotation.{Description, Name, Plugin}
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.Schema
import co.cask.cdap.etl.api.batch.{SparkCompute, SparkExecutionPluginContext}
import co.cask.cdap.etl.api.{PipelineConfigurer, StageConfigurer}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.repl.SparkIMain

import scala.reflect.runtime
import scala.reflect.runtime.universe._
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.reflect.ToolBox

/**
 * SparkCompute plugin that runs configurable scala code.
 */
@Plugin(`type` = SparkCompute.PLUGIN_TYPE)
@Name("ScalaInterpreter3")
@Description("Runs configurable scala code to transform one RDD into another.")
class ScalaInterpreterCompute3(config: ScalaConfig) extends SparkCompute[StructuredRecord, StructuredRecord]
  with Serializable {

  @transient val rddType = typeOf[RDD[StructuredRecord]]
  @transient var methodTree: Tree = null
  var compiledCode: () => Any = null

  override def configurePipeline(pipelineConfigurer: PipelineConfigurer) {
    val stageConfigurer: StageConfigurer = pipelineConfigurer.getStageConfigurer
    val inputSchema: Schema = stageConfigurer.getInputSchema
    var outputSchema: Schema = config.parseSchema
    outputSchema = if (outputSchema == null) inputSchema else outputSchema
    stageConfigurer.setOutputSchema(outputSchema)
  }

  @throws(classOf[Exception])
  override def initialize(context: SparkExecutionPluginContext): Unit = {
  }

  @throws(classOf[Exception])
  def transform(sparkExecutionPluginContext: SparkExecutionPluginContext,
                javaRDD: JavaRDD[StructuredRecord]): JavaRDD[StructuredRecord] = {
    //javaRDD.rdd.filter(record => true).toJavaRDD()
    // this line is the equivalent of prefixing the code with
    // val rdd = javaRDD.rdd

    val settings = new GenericRunnerSettings( println _ )
    settings.usejavacp.value = true
    val interpreter = new SparkIMain(settings)
    interpreter.addImports("co.cask.cdap.api.data.format.StructuredRecord")
    interpreter.bind("input", "org.apache.spark.rdd.RDD[co.cask.cdap.api.data.format.StructuredRecord]", javaRDD.rdd)
    interpreter.interpret(config.getMethod)
    javaRDD
  }
}
