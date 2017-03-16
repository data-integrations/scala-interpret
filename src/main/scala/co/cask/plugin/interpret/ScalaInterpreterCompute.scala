package co.cask.plugin.interpret

import co.cask.cdap.api.annotation.{Description, Name, Plugin}
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.Schema
import co.cask.cdap.etl.api.batch.{SparkCompute, SparkExecutionPluginContext}
import co.cask.cdap.etl.api.{PipelineConfigurer, StageConfigurer}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.reflect.runtime
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

/**
 * SparkCompute plugin that runs configurable scala code.
 */
@Plugin(`type` = SparkCompute.PLUGIN_TYPE)
@Name("ScalaInterpreter")
@Description("Runs configurable scala code to transform one RDD into another.")
class ScalaInterpreterCompute(config: ScalaConfig) extends SparkCompute[StructuredRecord, StructuredRecord]
  with Serializable {

  @transient val rddType = typeOf[RDD[StructuredRecord]]
  @transient var toolbox: ToolBox[runtime.universe.type] = null
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
    toolbox = runtimeMirror(Thread.currentThread().getContextClassLoader).mkToolBox()
    methodTree = toolbox.parse(config.getMethod)
    val build = runtime.universe.build
    val subVar = build.setTypeSignature(build.newFreeTerm("__dumb__",
      context.getSparkContext.emptyRDD.rdd), rddType)
    // this line is the equivalent of prefixing the code with
    // val rdd = context.getSparkContext.emptyRDD.rdd
    val fullTree = Block(List(ValDef(Modifiers(), newTermName("rdd"), TypeTree(), Ident(subVar))), methodTree)
    val returnType = toolbox.typeCheck(fullTree)
    // todo: figure out why returnType.tpe.equals(rddType) is false
    if (!returnType.tpe.toString.equals(rddType.toString)) {
      throw new IllegalArgumentException(
        "method must return an RDD[StructuredRecord] but it returns " + returnType.tpe.toString)
    }
  }

  @throws(classOf[Exception])
  def transform(sparkExecutionPluginContext: SparkExecutionPluginContext,
                javaRDD: JavaRDD[StructuredRecord]): JavaRDD[StructuredRecord] = {
    //javaRDD.rdd.filter(record => true).toJavaRDD()
    // this line is the equivalent of prefixing the code with
    // val rdd = javaRDD.rdd
    val subVar = build.setTypeSignature(build.newFreeTerm("__dumb__", javaRDD.rdd), rddType)
    val fullTree = Block(List(ValDef(Modifiers(), newTermName("rdd"), TypeTree(), Ident(subVar))), methodTree)
    compiledCode = toolbox.compile(fullTree)
    compiledCode.apply().asInstanceOf[RDD[StructuredRecord]].toJavaRDD()
  }
}
