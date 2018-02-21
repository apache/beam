/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.avro;

import com.google.common.io.Files;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.testing.DatasetAssert;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.util.Arrays.asList;

/** Test Reading avro file with schema */
public class AvroSourceTest {

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Collects Avro record as JSON string
   *
   * @param outSink
   * @param inSource
   * @throws Exception
   */
  public static void runFlow(
      DataSink<String> outSink,
      DataSource<Pair<AvroKey<GenericData.Record>, NullWritable>> inSource)
      throws Exception {
    Flow flow = Flow.create("simple read avro");
    Dataset<Pair<AvroKey<GenericData.Record>, NullWritable>> input = flow.createInput(inSource);
    final Dataset<String> output =
        FlatMap.named("avro2csv").of(input).using(AvroSourceTest::apply).output();
    output.persist(outSink);
    Executor executor = new LocalExecutor();
    executor.submit(flow).get();
  }

  /**
   * write avro generic record as JSON
   *
   * @param pair data
   * @param c collector
   */
  private static void apply(
      Pair<AvroKey<GenericData.Record>, NullWritable> pair, Collector<String> c) {
    AvroKey<GenericData.Record> record = pair.getFirst();
    c.collect(record.toString());
  }

  /**
   * Copy resource data to temporary location and get absolute path to this file. Useful for unit
   * testing.
   *
   * @param tmpFolder junit temporary folder, will be delete after test ends
   * @param resourceRelativePath relative path to src/test/resources
   * @return absolute path to temporary file
   * @throws IOException
   * @throws NullPointerException usualy when the relative path is wrong
   */
  private static String createTmpFile(TemporaryFolder tmpFolder, String resourceRelativePath)
      throws IOException {
    InputStream file =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceRelativePath);
    File temporaryFile = tmpFolder.newFile("tmp-data.avro");
    Files.write(IOUtils.toByteArray(file), temporaryFile);
    return temporaryFile.getAbsolutePath();
  }

  /**
   * Returns resource file as string
   *
   * @param resourceRelativePath relative to src/test/resources
   * @return content of the file
   * @throws IOException
   */
  public static String loadFileToString(String resourceRelativePath) throws IOException {
    InputStream file =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceRelativePath);
    return IOUtils.toString(file);
  }

  /**
   * Test with schema in JSON or simple schema less <code>
   * AvroHadoopSource inSource = new AvroHadoopSource(new Path(rootFolder));</code>
   */
  @Test
  public void testAvroRead() throws Exception {
    String tmpFile = createTmpFile(tmpFolder, "avro/data.avro");
    String rootFolder = new File(tmpFile).getParent();
    ListDataSink<String> outSink = ListDataSink.get();
    String schema = loadFileToString("avro/example-schema.json");
    AvroHadoopSource inSource =
        AvroHadoopSource.of().path(new Path(rootFolder)).schema(schema).build();
    runFlow(outSink, inSource);
    DatasetAssert.unorderedEquals(expected(), outSink.getOutputs());
  }

  private List<String> expected() {
    return asList(
        "{\"field1\": 2, \"field2\": \"test data 2\", \"field3\": false}",
        "{\"field1\": 1, \"field2\": \"test data 1\", \"field3\": true}");
  }
}
