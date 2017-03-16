package co.cask.plugin.interpret;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Config properties for the plugin.
 */
public class ScalaConfig extends PluginConfig {
  @Description("The method to run, written as scala code.")
  private String method;

  @Nullable
  @Description("The schema of the output. If none is given, the input schema is used as the output schema.")
  private String schema;

  @Nullable
  public Schema parseSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not parse schema. Cause: " + e.getMessage(), e);
    }
  }

  public String getMethod() {
    return method;
  }
}
