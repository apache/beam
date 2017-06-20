package org.apache.beam.runners.tez.translation;

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.tez.TezPipelineOptions;
import org.apache.beam.runners.tez.TezRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the ParDoTranslator class
 */
public class ParDoTranslatorTest {

  private static final String DO_FN_INSTANCE_TAG = "DO_FN_INSTANCE";
  private static final String TEST_TAG = "TestName";
  private static TransformHierarchy hierarchy;
  private static PValue pvalue;
  private static DAG dag;
  private static TranslationContext context;
  private static ParDoTranslator translator;

  @Test
  public void testParDoTranslation() throws Exception {
    MultiOutput parDo = ParDo.of(new TestDoFn()).withOutputTags(new TupleTag<>(), TupleTagList.of(new TupleTag<String>()));
    Node node = hierarchy.pushNode(TEST_TAG, pvalue, parDo);
    hierarchy.setOutput(pvalue);
    context.setCurrentTransform(node);
    translator.translate(parDo, context);
    context.populateDAG(dag);
    Vertex vertex = Iterables.getOnlyElement(dag.getVertices());
    Configuration config = TezUtils.createConfFromUserPayload(vertex.getProcessorDescriptor().getUserPayload());
    String doFnString = config.get(DO_FN_INSTANCE_TAG);
    DoFn doFn = (DoFn) TranslatorUtil.fromString(doFnString);

    Assert.assertEquals(vertex.getProcessorDescriptor().getClassName(), TezDoFnProcessor.class.getName());
    Assert.assertEquals(doFn.getClass(), TestDoFn.class);
  }

  @Before
  public void setupTest(){
    dag = DAG.create(TEST_TAG);
    translator = new ParDoTranslator();
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(TezRunner.class);
    TezPipelineOptions tezOptions = PipelineOptionsValidator.validate(TezPipelineOptions.class, options);
    context = new TranslationContext(tezOptions, new TezConfiguration());
    hierarchy = new TransformHierarchy(Pipeline.create());
    PValue innerValue = new PValueBase() {
      @Override
      public String getName() {return null;}
    };
    pvalue = new PValue() {
      @Override
      public String getName() {return null;}

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        Map<TupleTag<?>, PValue> map = new HashMap<>();
        map.put(new TupleTag<>(), innerValue);
        return map;
      }

      @Override
      public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {}

      @Override
      public Pipeline getPipeline() {return null;}

      @Override
      public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {}
    };
  }

  private static class TestDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      //Test DoFn
    }
  }
}
