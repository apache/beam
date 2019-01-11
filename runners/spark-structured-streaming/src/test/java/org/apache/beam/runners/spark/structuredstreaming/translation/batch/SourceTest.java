package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PipelineOptionsSerializationUtils;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkRunner;
import org.apache.beam.runners.spark.structuredstreaming.utils.SerializationDebugger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for beam to spark source translation.
 */
@RunWith(JUnit4.class)
public class SourceTest implements Serializable {
  private static Pipeline pipeline;
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass(){
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testSerialization() throws IOException{
    BoundedSource<Integer> source = new BoundedSource<Integer>() {

      @Override public List<? extends BoundedSource<Integer>> split(long desiredBundleSizeBytes,
          PipelineOptions options) throws Exception {
        return new ArrayList<>();
      }

      @Override public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0;
      }

      @Override public BoundedReader<Integer> createReader(PipelineOptions options)
          throws IOException {
        return null;
      }
    };
    String serializedSource = Base64Serializer.serializeUnchecked(source);
    Map<String, String> dataSourceOptions = new HashMap<>();
    dataSourceOptions.put(DatasetSourceBatch.BEAM_SOURCE_OPTION, serializedSource);
    dataSourceOptions.put(DatasetSourceBatch.DEFAULT_PARALLELISM, "4");
    dataSourceOptions.put(DatasetSourceBatch.PIPELINE_OPTIONS,
        PipelineOptionsSerializationUtils.serializeToJson(pipeline.getOptions()));
    DataSourceReader objectToTest = new DatasetSourceBatch()
        .createReader(new DataSourceOptions(dataSourceOptions));
    SerializationDebugger.testSerialization(objectToTest, TEMPORARY_FOLDER.newFile());
  }

  @Test
  public void testBoundedSource(){
    pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    pipeline.run();
  }

}
