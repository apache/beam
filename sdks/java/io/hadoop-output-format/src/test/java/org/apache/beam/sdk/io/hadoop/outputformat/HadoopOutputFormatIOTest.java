/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.outputformat;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.inputformat.Employee;
import org.apache.beam.sdk.io.hadoop.inputformat.TestEmployeeDataSet;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link HadoopOutputFormatIO}. */
@RunWith(JUnit4.class)
public class HadoopOutputFormatIOTest {
  private static SerializableConfiguration serConf;

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUp() {
    EmployeeOutputFormat.initWrittenOutput();
    serConf = loadTestConfiguration(EmployeeOutputFormat.class, Text.class, Employee.class);
  }

  private static SerializableConfiguration loadTestConfiguration(
      Class<?> outputFormatClassName, Class<?> keyClass, Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass("mapreduce.job.outputformat.class", outputFormatClassName, OutputFormat.class);
    conf.setClass("mapreduce.job.outputformat.key.class", keyClass, Object.class);
    conf.setClass("mapreduce.job.outputformat.value.class", valueClass, Object.class);
    return new SerializableConfiguration(conf);
  }

  @Test
  public void testWriteBuildsCorrectly() {
    HadoopOutputFormatIO.Write<Text, Employee> write =
        HadoopOutputFormatIO.<Text, Employee>write().withConfiguration(serConf.get());

    assertEquals(serConf.get(), write.getConfiguration().get());
    assertEquals(EmployeeOutputFormat.class, write.getOutputFormatClass().getRawType());
    assertEquals(Text.class, write.getOutputFormatKeyClass().getRawType());
    assertEquals(Employee.class, write.getOutputFormatValueClass().getRawType());
  }

  /**
   * This test validates {@link HadoopOutputFormatIO.Write Write} transform object creation fails
   * with null configuration. {@link HadoopOutputFormatIO.Write#withConfiguration(Configuration)
   * withConfiguration(Configuration)} method checks configuration is null and throws exception if
   * it is null.
   */
  @Test
  public void testWriteObjectCreationFailsIfConfigurationIsNull() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Configuration can not be null");
    HadoopOutputFormatIO.<Text, Employee>write().withConfiguration(null);
  }

  /**
   * This test validates functionality of {@link
   * HadoopOutputFormatIO.Write#withConfiguration(Configuration) withConfiguration(Configuration)}
   * function when Hadoop OutputFormat class is not provided by the user in configuration.
   */
  @Test
  public void testWriteValidationFailsMissingInputFormatInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopOutputFormatIO.OUTPUTFORMAT_KEY_CLASS, Text.class, Object.class);
    configuration.setClass(
        HadoopOutputFormatIO.OUTPUTFORMAT_VALUE_CLASS, Employee.class, Object.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Configuration must contain \"mapreduce.job.outputformat.class\"");
    HadoopOutputFormatIO.<Text, Employee>write().withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link
   * HadoopOutputFormatIO.Write#withConfiguration(Configuration) withConfiguration(Configuration)}
   * function when key class is not provided by the user in configuration.
   */
  @Test
  public void testWriteValidationFailsMissingKeyClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(
        HadoopOutputFormatIO.OUTPUTFORMAT_CLASS, TextOutputFormat.class, OutputFormat.class);
    configuration.setClass(
        HadoopOutputFormatIO.OUTPUTFORMAT_VALUE_CLASS, Employee.class, Object.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Configuration must contain \"mapreduce.job.outputformat.key.class\"");
    HadoopOutputFormatIO.<Text, Employee>write().withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link
   * HadoopOutputFormatIO.Write#withConfiguration(Configuration) withConfiguration(Configuration)}
   * function when value class is not provided by the user in configuration.
   */
  @Test
  public void testWriteValidationFailsMissingValueClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(
        HadoopOutputFormatIO.OUTPUTFORMAT_CLASS, TextOutputFormat.class, OutputFormat.class);
    configuration.setClass(HadoopOutputFormatIO.OUTPUTFORMAT_KEY_CLASS, Text.class, Object.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Configuration must contain \"mapreduce.job.outputformat.value.class\"");
    HadoopOutputFormatIO.<Text, Employee>write().withConfiguration(configuration);
  }

  @Test
  public void testWritingData() {
    List<KV<Text, Employee>> data = TestEmployeeDataSet.getEmployeeData();
    PCollection<KV<Text, Employee>> input = p.apply(Create.of(data));
    input.apply(
        "Write", HadoopOutputFormatIO.<Text, Employee>write().withConfiguration(serConf.get()));
    p.run();

    List<KV<Text, Employee>> writtenOutput = EmployeeOutputFormat.getWrittenOutput();
    assertEquals(data.size(), writtenOutput.size());
    assertTrue(data.containsAll(writtenOutput));
    assertTrue(writtenOutput.containsAll(data));
  }

  @Test
  public void testWritingDataFailInvalidKeyType() {
    List<KV<String, Employee>> data = new ArrayList<>();
    data.add(KV.of("key", new Employee("name", "address")));
    PCollection<KV<String, Employee>> input = p.apply(Create.of(data));
    input.apply(
        "Write", HadoopOutputFormatIO.<String, Employee>write().withConfiguration(serConf.get()));
    thrown.expect(Pipeline.PipelineExecutionException.class);
    p.run();
  }

  @Test
  public void testWritingDataFailInvalidValueType() {
    List<KV<Text, Text>> data = new ArrayList<>();
    data.add(KV.of(new Text("key"), new Text("value")));
    PCollection<KV<Text, Text>> input = p.apply(Create.of(data));
    input.apply("Write", HadoopOutputFormatIO.<Text, Text>write().withConfiguration(serConf.get()));
    thrown.expect(Pipeline.PipelineExecutionException.class);
    p.run();
  }

  /**
   * This test validates functionality of {@link
   * HadoopOutputFormatIO.Write#populateDisplayData(DisplayData.Builder)
   * populateDisplayData(DisplayData.Builder)}.
   */
  @Test
  public void testWriteDisplayData() {
    HadoopOutputFormatIO.Write<String, String> write =
        HadoopOutputFormatIO.<String, String>write().withConfiguration(serConf.get());
    DisplayData displayData = DisplayData.from(write);

    assertThat(
        displayData,
        hasDisplayItem(
            HadoopOutputFormatIO.OUTPUTFORMAT_CLASS,
            serConf.get().get(HadoopOutputFormatIO.OUTPUTFORMAT_CLASS)));
    assertThat(
        displayData,
        hasDisplayItem(
            HadoopOutputFormatIO.OUTPUTFORMAT_KEY_CLASS,
            serConf.get().get(HadoopOutputFormatIO.OUTPUTFORMAT_KEY_CLASS)));
    assertThat(
        displayData,
        hasDisplayItem(
            HadoopOutputFormatIO.OUTPUTFORMAT_VALUE_CLASS,
            serConf.get().get(HadoopOutputFormatIO.OUTPUTFORMAT_VALUE_CLASS)));
  }
}
