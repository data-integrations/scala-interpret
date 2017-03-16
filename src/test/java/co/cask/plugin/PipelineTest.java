/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.plugin.interpret.ScalaInterpreterCompute;
import co.cask.plugin.interpret.SparkEvalAction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.Logging;
import org.apache.spark.repl.SparkIMain;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      SparkEvalAction.class,
                      ScalaInterpreterCompute.class,
                      SparkIMain.class,
                      Logging.class);
  }

  @Test
  public void testSparkEvalAction() throws Exception {
    File testDir = TMP_FOLDER.newFolder("sparkEvalTest");

    // write to some file
    File input = new File(testDir, "poems.txt");
    PrintWriter printWriter = new PrintWriter(input.getAbsolutePath());
    printWriter.println("haikus are easy");
    printWriter.println("but sometimes they dont make sense");
    printWriter.println("refridgerator");
    printWriter.println();
    printWriter.println("this is a poem");
    printWriter.println("it is not a good poem");
    printWriter.close();

    File output = new File(testDir, "poems_wordcount.txt");

    String code = String.format("val textFile = sc.textFile(\"%s\")\n" +
                                  "textFile.saveAsTextFile(\"%s\")",
                                input.getAbsolutePath(), output.getAbsolutePath());

    Map<String, String> properties = ImmutableMap.of("code", code);
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("taybull")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("otherTaybull")))
      .addStage(new ETLStage("spark", new ETLPlugin("SparkEval", Action.PLUGIN_TYPE, properties, null)))
      .addConnection("source", "sink")
      .addConnection("spark", "source")
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("computeTest");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, config));

    // run the workflow
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    // check output
    try (BufferedReader reader = new BufferedReader(new FileReader(output))) {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }
    }
  }

  // this fails because none of the compute plugins work
  @Test
  public void testScalaInterpreterCompute() throws Exception {
    String inputName = "scalaComputeTestInput";
    String outputName = "scalaComputeTestOutput";

    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("email", Schema.of(Schema.Type.STRING)));
    Map<String, String> properties = new HashMap<>();
    properties.put("schema", schema.toString());
    properties.put("method", "val output = input.filter(record => { " +
      "  val id = record.get(\"id\").asInstanceOf[Long]\n" +
      "  id < 3L" +
      "})");

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputName)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputName)))
      .addStage(new ETLStage("compute", new ETLPlugin("ScalaInterpreter3", SparkCompute.PLUGIN_TYPE,
                                                      properties, null)))
      .addConnection("source", "compute")
      .addConnection("compute", "sink")
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("computeTest");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, config));

    // write the input
    DataSetManager<Table> inputManager = getDataset(inputName);
    Set<StructuredRecord> inputRecords = ImmutableSet.of(
      StructuredRecord.builder(schema).set("id", 1L).set("name", "Samuel").set("email", "sam@abc.com").build(),
      StructuredRecord.builder(schema).set("id", 2L).set("name", "L").set("email", "l@abc.com").build(),
      StructuredRecord.builder(schema).set("id", 3L).set("name", "Jackson").set("email", "jax@xyz.com").build()
    );
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(schema).set("id", 1L).set("name", "Samuel").set("email", "sam@abc.com").build(),
      StructuredRecord.builder(schema).set("id", 2L).set("name", "L").set("email", "l@abc.com").build()
    );
    Set<StructuredRecord> outputRecords = new HashSet<>();
    outputRecords.addAll(MockSink.readOutput(outputManager));
    Assert.assertEquals(expected, outputRecords);
  }
}
