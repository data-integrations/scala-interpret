package co.cask.plugin.interpret

import co.cask.cdap.api.annotation.{Description, Name, Plugin}
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.Schema
import co.cask.cdap.etl.api.batch.{SparkCompute, SparkExecutionPluginContext}
import co.cask.cdap.etl.api.{PipelineConfigurer, StageConfigurer}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._
import scala.tools.nsc.Settings
import scala.tools.nsc.interactive.Global
import scala.tools.reflect.ToolBox

/**
 * SparkCompute plugin that runs configurable scala code.
 */
@Plugin(`type` = SparkCompute.PLUGIN_TYPE)
@Name("ScalaInterpreter2")
@Description("Runs configurable scala code to transform one RDD into another.")
class ScalaInterpreterCompute2(config: ScalaConfig)
  extends SparkCompute[StructuredRecord, StructuredRecord] with Serializable {
  @transient val rddType = typeOf[RDD[StructuredRecord]]
  @transient val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
  val outline =
    "import co.cask.cdap.api.data.format.StructuredRecord\n" +
    "import co.cask.plugin.interpret._\n" +
    "import org.apache.spark.rdd.RDD\n" +
    "\n" +
    "\n" +
    "class ScalaTransform extends RDDTransform with Serializable {\n" +
    "\n" +
    "  override def transform(input: RDD[StructuredRecord]): RDD[StructuredRecord] = {\n" +
    "    %s\n" +
    "  }\n" +
    "}\n" +
    "scala.reflect.classTag[ScalaTransform].runtimeClass"
  var transform: RDDTransform = null

  override def configurePipeline(pipelineConfigurer: PipelineConfigurer) {
    val stageConfigurer: StageConfigurer = pipelineConfigurer.getStageConfigurer
    val inputSchema: Schema = stageConfigurer.getInputSchema
    var outputSchema: Schema = config.parseSchema
    outputSchema = if (outputSchema == null) inputSchema else outputSchema
    stageConfigurer.setOutputSchema(outputSchema)
  }

  @throws(classOf[Exception])
  override def initialize(context: SparkExecutionPluginContext): Unit = {
    val classStr = String.format(outline, config.getMethod)
    val classTree = toolbox.parse(classStr)
    val clazz = toolbox.eval(classTree).asInstanceOf[java.lang.Class[RDDTransform]]
    transform = clazz.newInstance()
    val jars = context.getSparkContext.sc.jars
    val compiler = new Global(new Settings(), null)
  }

  @throws(classOf[Exception])
  def transform(sparkExecutionPluginContext: SparkExecutionPluginContext,
                javaRDD: JavaRDD[StructuredRecord]): JavaRDD[StructuredRecord] = {
    transform.transform(javaRDD.rdd).toJavaRDD()
  }
}
