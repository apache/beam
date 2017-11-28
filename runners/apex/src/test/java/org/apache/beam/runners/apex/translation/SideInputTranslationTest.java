package org.apache.beam.runners.apex.translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.OperatorMeta;
import com.datatorrent.stram.engine.PortContext;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.runners.apex.translation.utils.CoderAdapterStreamCodec;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;

/**
 * Test that view overrides are applied by checking the corresponding
 * side input coders. Unlike runner validation these don't run the pipeline,
 * they only check translation.
 */
public class SideInputTranslationTest implements java.io.Serializable {

  //@Test
  public void testMapAsEntrySetSideInput() {
    ApexPipelineOptions options =
        PipelineOptionsFactory.as(ApexPipelineOptions.class);
    options.setApplicationName("GroupByKey");
    options.setRunner(TestApexRunner.class);
    Pipeline pipeline = Pipeline.create(options);


    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.<String, Integer>asMap());

    org.apache.beam.sdk.values.PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of(2 /* size */))
            .apply(
                "OutputSideInputs",
                ParDo.of(new DoFn<Integer, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertEquals((int) c.element(), c.sideInput(view).size());
                    assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                    for (Entry<String, Integer> entry : c.sideInput(view).entrySet()) {
                      c.output(KV.of(entry.getKey(), entry.getValue()));
                    }
                  }
                }).withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 1), KV.of("b", 3));

    pipeline.run();
  }

  private final transient ApexPipelineOptions options = PipelineOptionsFactory.create()
      .as(ApexPipelineOptions.class);

  @Test
  public void testListSideInputTranslation() throws Exception {

    Pipeline p = Pipeline.create();
    final PCollectionView<List<Integer>> view =
        p.apply("CreateSideInput", Create.of(11, 13, 17, 23)).apply(View.<Integer>asList());
    ListCoder<?> expectedCoder = ListCoder.of(KvCoder.of(VoidCoder.of(), VarIntCoder.of()));
    assertSideInputTranslation(view, expectedCoder);

  }

  @Test
  public void testMapSideInputTranslation() throws Exception {

    Pipeline p = Pipeline.create();
    final PCollectionView<Map<String, Integer>> view =
        p.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.<String, Integer>asMap());
    ListCoder<?> expectedCoder = ListCoder.of(KvCoder.of(VoidCoder.of(),
        KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));
    assertSideInputTranslation(view, expectedCoder);
  }

  @Test
  public void testMultimapSideInputTranslation() throws Exception {

    Pipeline p = Pipeline.create();
    final PCollectionView<Map<String, Iterable<Integer>>> view =
        p.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
            .apply(View.<String, Integer>asMultimap());
    ListCoder<?> expectedCoder = ListCoder.of(KvCoder.of(VoidCoder.of(),
        KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));
    assertSideInputTranslation(view, expectedCoder);
  }

  private void assertSideInputTranslation(PCollectionView<?> view, Coder<?> expectedSideInputCoder)
      throws Exception {

    Pipeline p = view.getPipeline();
    p.apply("CreateMainInput", Create.of(1))
        .apply("OutputSideInputs",
            ParDo.of(new DoFn<Integer, KV<String, Integer>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
              }
            }).withSideInputs(view));

    DAG dag = TestApexRunner.translate(p, options);

    OperatorMeta om = dag.getOperatorMeta("OutputSideInputs/ParMultiDo(Anonymous)");
    assertNotNull(om);
    assertEquals(2, om.getInputStreams().size());

    String fieldName = "field=sideInput1";
    Map.Entry<DAG.InputPortMeta, DAG.StreamMeta> sideInput = null;
    for (Map.Entry<DAG.InputPortMeta, DAG.StreamMeta> input : om.getInputStreams().entrySet()) {
      CoderAdapterStreamCodec sc = (CoderAdapterStreamCodec) input.getKey().getAttributes()
          .get(PortContext.STREAM_CODEC);
      if (input.toString().contains(fieldName)) {
        sideInput = input;
      }
      System.out.println(sc.getCoder());
    }
    assertNotNull("could not find stream for: " + fieldName, sideInput);

    CoderAdapterStreamCodec sc = (CoderAdapterStreamCodec) sideInput.getKey().getAttributes()
        .get(PortContext.STREAM_CODEC);
    @SuppressWarnings("rawtypes")
    StructuredCoder<?> coder = (StructuredCoder) sc.getCoder();
    assertTrue(coder.getComponents().get(0) instanceof FullWindowedValueCoder);
    @SuppressWarnings("rawtypes")
    FullWindowedValueCoder<?> fwvc = (FullWindowedValueCoder) coder.getComponents().get(0);
    assertEquals(expectedSideInputCoder, fwvc.getValueCoder());

  }

}
