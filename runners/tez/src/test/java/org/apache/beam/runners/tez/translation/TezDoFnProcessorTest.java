package org.apache.beam.runners.tez.translation;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the TezDoFnProcessor that wraps beam DoFns.
 */
public class TezDoFnProcessorTest {

  private static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
  private static final String INPUT_LOCATION = "src/test/resources/test_input.txt";
  private static final DAG dag = DAG.create("TestDag");
  private static TezClient client;

  @Before
  public void setUp(){
    TezConfiguration config = new TezConfiguration();
    config.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    config.set("fs.default.name", "file:///");
    config.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    config.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG");
    client = TezClient.create("TezClient", config);
  }

  @Test
  public void testDoFn() throws Exception {
    String expected = FileUtils.readFileToString(new File(INPUT_LOCATION));
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expected.split(TOKENIZER_PATTERN)));

    DoFn<?,?> doFn = new TestWordsFn();
    String doFnInstance;
    doFnInstance = TranslatorUtil.toString(doFn);

    Configuration config = new Configuration();
    config.set("OUTPUT_TAG", new TupleTag<>().getId());
    config.set("DO_FN_INSTANCE", doFnInstance);
    UserPayload payload = TezUtils.createUserPayloadFromConf(config);

    Vertex vertex = Vertex.create("TestVertex", ProcessorDescriptor
        .create(TezDoFnProcessor.class.getName()).setUserPayload(payload));
    vertex.addDataSource("TestInput" , MRInput.createConfigBuilder(new Configuration(),
        TextInputFormat.class, INPUT_LOCATION).build());

    dag.addVertex(vertex);
    client.start();
    client.submitDAG(dag);
    while (client.getAppMasterStatus() != TezAppMasterStatus.SHUTDOWN){}

    Assert.assertEquals(expectedSet, TestWordsFn.RESULTS);
  }

  private static class TestWordsFn extends DoFn<String, String> {
    private static final Set<String> RESULTS = Collections.synchronizedSet(new HashSet<>());

    public TestWordsFn(){
      RESULTS.clear();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      // Split the line into words.
      String[] words = c.element().split(TOKENIZER_PATTERN);
      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          RESULTS.add(word);
        }
      }
    }
  }

}
