package org.apache.beam.runners.tez.translation;

import com.google.common.collect.Iterables;
import org.apache.beam.runners.tez.TezPipelineOptions;
import org.apache.beam.runners.tez.TezRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the TranslationContext class
 */
public class TranslationContextTest {

  private static final String TEST_SOURCE = "Test.txt";
  private static final String DAG_NAME = "TestDag";
  private static final String VERTEX1_NAME = "TestVertex1";
  private static final String VERTEX2_NAME = "TestVertex2";
  private static final String PVALUE_NAME = "TestPValue";

  private static PValue value1;
  private static PValue value2;
  private static PValue value3;
  private static TranslationContext context;
  private static DAG dag;

  @Before
  public void setUp() {
    dag = DAG.create(DAG_NAME);
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(TezRunner.class);
    TezPipelineOptions tezOptions = PipelineOptionsValidator.validate(TezPipelineOptions.class, options);
    context = new TranslationContext(tezOptions, new TezConfiguration());
    value1 = new PValueBase() {
      @Override
      public String getName() {
        return PVALUE_NAME;
      }
    };
    value2 = new PValueBase() {
      @Override
      public String getName() {
        return PVALUE_NAME;
      }
    };
    value3 = new PValueBase() {
      @Override
      public String getName() {
        return PVALUE_NAME;
      }
    };
  }

  @Test
  public void testVertexConnect() throws Exception {
    Vertex vertex1 = Vertex.create(VERTEX1_NAME, ProcessorDescriptor.create(TezDoFnProcessor.class.getName()));
    Vertex vertex2 = Vertex.create(VERTEX2_NAME, ProcessorDescriptor.create(TezDoFnProcessor.class.getName()));
    context.addVertex(VERTEX1_NAME, vertex1, value1, value2);
    context.addVertex(VERTEX2_NAME, vertex2, value2, value3);
    context.populateDAG(dag);
    Vertex vertex1Output = Iterables.getOnlyElement(dag.getVertex(VERTEX1_NAME).getOutputVertices());
    Vertex vertex2Input = Iterables.getOnlyElement(dag.getVertex(VERTEX2_NAME).getInputVertices());

    Assert.assertEquals(vertex2, vertex1Output);
    Assert.assertEquals(vertex1, vertex2Input);
  }

  @Test
  public void testDataSourceConnect() throws Exception {
    Vertex vertex1 = Vertex.create(VERTEX1_NAME, ProcessorDescriptor.create(TezDoFnProcessor.class.getName()));
    context.addVertex(VERTEX1_NAME, vertex1, value1, value2);
    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(context.getConfig()),
        TextInputFormat.class, TEST_SOURCE).groupSplits(true).generateSplitsInAM(true).build();
    context.addSource(value1, dataSource);
    context.populateDAG(dag);
    DataSourceDescriptor vertex1Source = Iterables.getOnlyElement(dag.getVertex(VERTEX1_NAME).getDataSources());

    Assert.assertEquals(dataSource, vertex1Source);
  }
}
