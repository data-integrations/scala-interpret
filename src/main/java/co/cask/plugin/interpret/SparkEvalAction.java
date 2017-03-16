package co.cask.plugin.interpret;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;

/**
 * Runs some arbitrary spark code
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SparkEval")
@Description("Evaluates and runs custom Spark code. Only spark classes already on the classpath are available.")
public class SparkEvalAction extends Action {
  private final Conf conf;

  public SparkEvalAction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    SparkInterpreter interpreter = new SparkInterpreter();
    try {
      interpreter.parse(conf.code);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse code: " + e.getMessage(), e);
    }
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    SparkInterpreter interpreter = new SparkInterpreter();
    interpreter.interpret(conf.code);
  }

  /**
   * Config for the plugin
   */
  public static class Conf extends PluginConfig {
    private String code;
  }
}
