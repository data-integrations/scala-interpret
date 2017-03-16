package co.cask.plugin.interpret

import org.apache.spark.repl.SparkIMain

import scala.tools.nsc.GenericRunnerSettings

/**
 * Interprets Spark code.
 */
class SparkInterpreter {
  val settings = new GenericRunnerSettings( println _ )
  settings.usejavacp.value = true
  val interpreter = new SparkIMain(settings)
  interpreter.addImports("org.apache.spark._")
  interpreter.addImports("org.apache.spark.rdd._")
  interpreter.interpret("val sc = new SparkContext()")

  def interpret(code: String): Unit = {
    interpreter.interpret(code)
  }

  def parse(code: String): Unit = {
    interpreter.parse(code)
  }
}
