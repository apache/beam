/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cdap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.cdap.plugin.common.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.cdap.batch.EmployeeBatchSink;
import org.apache.beam.sdk.io.cdap.batch.EmployeeBatchSource;
import org.apache.beam.sdk.io.cdap.batch.EmployeeInputFormat;
import org.apache.beam.sdk.io.cdap.batch.EmployeeInputFormatProvider;
import org.apache.beam.sdk.io.cdap.batch.EmployeeOutputFormat;
import org.apache.beam.sdk.io.cdap.batch.EmployeeOutputFormatProvider;
import org.apache.beam.sdk.io.cdap.context.BatchSinkContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSourceContextImpl;
import org.apache.beam.sdk.io.cdap.streaming.EmployeeReceiver;
import org.apache.beam.sdk.io.cdap.streaming.EmployeeStreamingSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Test class for {@link CdapIO}. */
@RunWith(JUnit4.class)
public class CdapIOTest {

  private static final long PULL_FREQUENCY_SEC = 1L;
  private static final long START_OFFSET = 0L;
  private static final Duration WINDOW_DURATION = Duration.standardMinutes(1);

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Map<String, Object> TEST_EMPLOYEE_PARAMS_MAP =
      ImmutableMap.<String, Object>builder()
          .put(EmployeeConfig.OBJECT_TYPE, "employee")
          .put(Constants.Reference.REFERENCE_NAME, "referenceName")
          .build();

  @Before
  public void setUp() {
    OutputCommitter mockedOutputCommitter = Mockito.mock(OutputCommitter.class);
    EmployeeOutputFormat.initWrittenOutput(mockedOutputCommitter);
  }

  @Test
  public void testReadBuildsCorrectly() {
    EmployeeConfig pluginConfig =
        new ConfigWrapper<>(EmployeeConfig.class).withParams(TEST_EMPLOYEE_PARAMS_MAP).build();

    CdapIO.Read<String, String> read =
        CdapIO.<String, String>read()
            .withCdapPlugin(
                Plugin.createBatch(
                    EmployeeBatchSource.class,
                    EmployeeInputFormat.class,
                    EmployeeInputFormatProvider.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class);

    Plugin<String, String> cdapPlugin = read.getCdapPlugin();
    assertNotNull(cdapPlugin);
    assertEquals(EmployeeBatchSource.class, cdapPlugin.getPluginClass());
    assertEquals(EmployeeInputFormat.class, cdapPlugin.getFormatClass());
    assertEquals(EmployeeInputFormatProvider.class, cdapPlugin.getFormatProviderClass());
    assertNotNull(cdapPlugin.getContext());
    assertEquals(BatchSourceContextImpl.class, cdapPlugin.getContext().getClass());
    assertEquals(PluginConstants.PluginType.SOURCE, cdapPlugin.getPluginType());
    assertNotNull(cdapPlugin.getHadoopConfiguration());
    assertEquals(pluginConfig, read.getPluginConfig());
    assertEquals(String.class, read.getKeyClass());
    assertEquals(String.class, read.getValueClass());
  }

  @Test
  public void testReadObjectCreationFailsIfCdapPluginClassIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CdapIO.<String, String>read().withCdapPluginClass(null));
  }

  @Test
  public void testReadObjectCreationFailsIfPluginConfigIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> CdapIO.<String, String>read().withPluginConfig(null));
  }

  @Test
  public void testReadObjectCreationFailsIfKeyClassIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> CdapIO.<String, String>read().withKeyClass(null));
  }

  @Test
  public void testReadObjectCreationFailsIfValueClassIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> CdapIO.<String, String>read().withValueClass(null));
  }

  @Test
  public void testReadObjectCreationFailsIfPullFrequencySecIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CdapIO.<String, String>read().withPullFrequencySec(null));
  }

  @Test
  public void testReadObjectCreationFailsIfStartOffsetIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> CdapIO.<String, String>read().withStartOffset(null));
  }

  @Test
  public void testReadExpandingFailsMissingCdapPluginClass() {
    PBegin testPBegin = PBegin.in(TestPipeline.create());
    CdapIO.Read<String, String> read = CdapIO.read();
    assertThrows(IllegalStateException.class, () -> read.expand(testPBegin));
  }

  @Test
  public void testReadObjectCreationFailsIfCdapPluginClassIsNotSupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> CdapIO.<String, String>read().withCdapPluginClass(EmployeeBatchSource.class));
  }

  @Test
  public void testReadFromCdapBatchPlugin() {
    EmployeeConfig pluginConfig =
        new ConfigWrapper<>(EmployeeConfig.class).withParams(TEST_EMPLOYEE_PARAMS_MAP).build();
    CdapIO.Read<String, String> read =
        CdapIO.<String, String>read()
            .withCdapPlugin(
                Plugin.createBatch(
                    EmployeeBatchSource.class,
                    EmployeeInputFormat.class,
                    EmployeeInputFormatProvider.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class);

    List<KV<String, String>> expected = new ArrayList<>();
    for (int i = 1; i < EmployeeInputFormat.NUM_OF_TEST_EMPLOYEE_RECORDS; i++) {
      expected.add(KV.of(String.valueOf(i), EmployeeInputFormat.EMPLOYEE_NAME_PREFIX + i));
    }
    PCollection<KV<String, String>> actual = p.apply("ReadBatchTest", read);
    PAssert.that(actual).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testReadFromCdapStreamingPlugin() {
    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setBlockOnRun(false);
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    EmployeeConfig pluginConfig =
        new ConfigWrapper<>(EmployeeConfig.class).withParams(TEST_EMPLOYEE_PARAMS_MAP).build();

    CdapIO.Read<String, String> read =
        CdapIO.<String, String>read()
            .withCdapPlugin(
                Plugin.createStreaming(
                    EmployeeStreamingSource.class,
                    Long::valueOf,
                    EmployeeReceiver.class,
                    config -> new Object[] {config}))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartOffset(START_OFFSET);

    List<String> storedRecords = EmployeeReceiver.getStoredRecords();

    PCollection<String> actual =
        p.apply("ReadStreamingTest", read)
            .setCoder(KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), StringUtf8Coder.of()))
            .apply(Values.create());

    PAssert.that(actual).containsInAnyOrder(storedRecords);
    p.run().waitUntilFinish(Duration.standardSeconds(15));
  }

  @Test
  public void testWriteBuildsCorrectly() {
    EmployeeConfig pluginConfig =
        new ConfigWrapper<>(EmployeeConfig.class).withParams(TEST_EMPLOYEE_PARAMS_MAP).build();

    CdapIO.Write<String, String> write =
        CdapIO.<String, String>write()
            .withCdapPlugin(
                Plugin.createBatch(
                    EmployeeBatchSink.class,
                    EmployeeOutputFormat.class,
                    EmployeeOutputFormatProvider.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class)
            .withLocksDirPath(tmpFolder.getRoot().getAbsolutePath());

    Plugin<String, String> cdapPlugin = write.getCdapPlugin();
    assertNotNull(cdapPlugin);
    assertNotNull(write.getLocksDirPath());
    assertEquals(EmployeeBatchSink.class, cdapPlugin.getPluginClass());
    assertEquals(EmployeeOutputFormat.class, cdapPlugin.getFormatClass());
    assertEquals(EmployeeOutputFormatProvider.class, cdapPlugin.getFormatProviderClass());
    assertNotNull(cdapPlugin.getContext());
    assertEquals(BatchSinkContextImpl.class, cdapPlugin.getContext().getClass());
    assertEquals(PluginConstants.PluginType.SINK, cdapPlugin.getPluginType());
    assertNotNull(cdapPlugin.getHadoopConfiguration());
    assertEquals(pluginConfig, write.getPluginConfig());
    assertEquals(String.class, write.getKeyClass());
    assertEquals(String.class, write.getValueClass());
  }

  @Test
  public void testWriteObjectCreationFailsIfCdapPluginClassIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CdapIO.<String, String>write().withCdapPluginClass(null));
  }

  @Test
  public void testWriteObjectCreationFailsIfPluginConfigIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CdapIO.<String, String>write().withPluginConfig(null));
  }

  @Test
  public void testWriteObjectCreationFailsIfKeyClassIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> CdapIO.<String, String>write().withKeyClass(null));
  }

  @Test
  public void testWriteObjectCreationFailsIfValueClassIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> CdapIO.<String, String>write().withValueClass(null));
  }

  @Test
  public void testWriteObjectCreationFailsIfLockDirIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CdapIO.<String, String>write().withLocksDirPath(null));
  }

  @Test
  public void testWriteExpandingFailsMissingCdapPluginClass() {
    PBegin testPBegin = PBegin.in(TestPipeline.create());
    PCollection<KV<String, String>> testPCollection =
        Create.empty(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())).expand(testPBegin);
    CdapIO.Write<String, String> write = CdapIO.write();
    assertThrows(IllegalStateException.class, () -> write.expand(testPCollection));
  }

  @Test
  public void testWriteObjectCreationFailsIfCdapPluginClassIsNotSupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> CdapIO.<String, String>write().withCdapPluginClass(EmployeeBatchSink.class));
  }

  @Test
  public void testWriteWithCdapBatchSinkPlugin() throws IOException {
    List<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < EmployeeInputFormat.NUM_OF_TEST_EMPLOYEE_RECORDS; i++) {
      data.add(KV.of(String.valueOf(i), EmployeeInputFormat.EMPLOYEE_NAME_PREFIX + i));
    }
    PCollection<KV<String, String>> input = p.apply(Create.of(data));

    EmployeeConfig pluginConfig =
        new ConfigWrapper<>(EmployeeConfig.class).withParams(TEST_EMPLOYEE_PARAMS_MAP).build();
    input.apply(
        "Write",
        CdapIO.<String, String>write()
            .withCdapPlugin(
                Plugin.createBatch(
                    EmployeeBatchSink.class,
                    EmployeeOutputFormat.class,
                    EmployeeOutputFormatProvider.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class)
            .withLocksDirPath(tmpFolder.getRoot().getAbsolutePath()));
    p.run();

    List<KV<String, String>> writtenOutput = EmployeeOutputFormat.getWrittenOutput();
    assertEquals(data.size(), writtenOutput.size());
    assertTrue(data.containsAll(writtenOutput));
    assertTrue(writtenOutput.containsAll(data));

    Mockito.verify(EmployeeOutputFormat.getOutputCommitter()).commitJob(Mockito.any());
  }

  @Test
  public void testWindowedWriteCdapBatchSinkPlugin() throws IOException {
    List<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < EmployeeInputFormat.NUM_OF_TEST_EMPLOYEE_RECORDS; i++) {
      data.add(KV.of(String.valueOf(i), EmployeeInputFormat.EMPLOYEE_NAME_PREFIX + i));
    }
    PCollection<KV<String, String>> input =
        p.apply(Create.of(data)).apply(Window.into(FixedWindows.of(WINDOW_DURATION)));

    EmployeeConfig pluginConfig =
        new ConfigWrapper<>(EmployeeConfig.class).withParams(TEST_EMPLOYEE_PARAMS_MAP).build();
    input.apply(
        "Write",
        CdapIO.<String, String>write()
            .withCdapPlugin(
                Plugin.createBatch(
                    EmployeeBatchSink.class,
                    EmployeeOutputFormat.class,
                    EmployeeOutputFormatProvider.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class)
            .withLocksDirPath(tmpFolder.getRoot().getAbsolutePath()));
    p.run();

    List<KV<String, String>> writtenOutput = EmployeeOutputFormat.getWrittenOutput();
    assertEquals(data.size(), writtenOutput.size());
    assertTrue(data.containsAll(writtenOutput));
    assertTrue(writtenOutput.containsAll(data));

    Mockito.verify(EmployeeOutputFormat.getOutputCommitter()).commitJob(Mockito.any());
  }
}
