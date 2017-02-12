package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.spark.ReuseSparkContext;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


/**
 * A test suite that tests tracking of the streaming sources created an
 * {@link org.apache.beam.runners.spark.translation.streaming.UnboundedDataset}.
 */
public class TrackStreamingSourcesTest {

  @Rule
  public ReuseSparkContext reuseContext = ReuseSparkContext.yes();

  private static final transient SparkPipelineOptions options =
      PipelineOptionsFactory.create().as(SparkPipelineOptions.class);

  @Before
  public void before() {
    UnboundedDataset.resetQueuedStreamIds();
    StreamingSourceTracker.numAssertions = 0;
  }

  @Test
  public void testTrackSingle() {
    options.setRunner(SparkRunner.class);
    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc,
        new Duration(options.getBatchIntervalMillis()));

    Pipeline p = Pipeline.create(options);

    CreateStream.QueuedValues<Integer> queueStream =
        CreateStream.fromQueue(Collections.<Iterable<Integer>>emptyList());

    p.apply(queueStream).setCoder(VarIntCoder.of())
        .apply(ParDo.of(new PassthroughFn<>()));

    p.traverseTopologically(new StreamingSourceTracker(jssc, p, ParDo.Bound.class,  -1));
    assertThat(StreamingSourceTracker.numAssertions, equalTo(1));
  }

  @Test
  public void testTrackFlattened() {
    options.setRunner(SparkRunner.class);
    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc,
        new Duration(options.getBatchIntervalMillis()));

    Pipeline p = Pipeline.create(options);

    CreateStream.QueuedValues<Integer> queueStream1 =
        CreateStream.fromQueue(Collections.<Iterable<Integer>>emptyList());
    CreateStream.QueuedValues<Integer> queueStream2 =
        CreateStream.fromQueue(Collections.<Iterable<Integer>>emptyList());

    PCollection<Integer> pcol1 = p.apply(queueStream1).setCoder(VarIntCoder.of());
    PCollection<Integer> pcol2 = p.apply(queueStream2).setCoder(VarIntCoder.of());
    PCollection<Integer> flattened =
        PCollectionList.of(pcol1).and(pcol2).apply(Flatten.<Integer>pCollections());
    flattened.apply(ParDo.of(new PassthroughFn<>()));

    p.traverseTopologically(new StreamingSourceTracker(jssc, p, ParDo.Bound.class, -1, -2));
    assertThat(StreamingSourceTracker.numAssertions, equalTo(1));
  }

  private static class PassthroughFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  private static class StreamingSourceTracker extends Pipeline.PipelineVisitor.Defaults {
    private final EvaluationContext ctxt;
    private final SparkRunner.Evaluator evaluator;
    private final Class<? extends PTransform> transformClassToAssert;
    private final Integer[] expected;

    private static int numAssertions = 0;

    private StreamingSourceTracker(
        JavaStreamingContext jssc,
        Pipeline pipeline,
        Class<? extends PTransform> transformClassToAssert,
        Integer... expected) {
      this.ctxt = new EvaluationContext(jssc.sparkContext(), pipeline, jssc);
      this.evaluator = new SparkRunner.Evaluator(
          new StreamingTransformTranslator.Translator(new TransformTranslator.Translator()), ctxt);
      this.transformClassToAssert = transformClassToAssert;
      this.expected = expected;
    }

    private void assertSourceIds(List<Integer> streamingSources) {
      numAssertions++;
      assertThat(streamingSources, containsInAnyOrder(expected));
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      return evaluator.enterCompositeTransform(node);
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      PTransform transform = node.getTransform();
      if (transform.getClass() == transformClassToAssert) {
        AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform();
        ctxt.setCurrentTransform(appliedTransform);
        //noinspection unchecked
        Dataset dataset = ctxt.borrowDataset((PTransform<? extends PValue, ?>) transform);
        assertSourceIds(((UnboundedDataset<?>) dataset).getStreamingSources());
        ctxt.setCurrentTransform(null);
      } else {
        evaluator.visitPrimitiveTransform(node);
      }
    }
  }

}
