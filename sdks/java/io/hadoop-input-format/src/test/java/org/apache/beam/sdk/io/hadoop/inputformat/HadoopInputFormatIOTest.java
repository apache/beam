/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http:www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource.SerializableSplit;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.Read;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.ConfigurableEmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.Employee;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.EmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.EmployeeInputFormat.EmployeeRecordReader;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.EmployeeInputFormat.NewObjectsEmployeeInputSplit;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.ReuseObjectsEmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.TestEmployeeDataSet;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import com.datastax.driver.core.Row;

/**
 * Unit tests for {@link HadoopInputFormatIO}.
 */
@RunWith(JUnit4.class)
public class HadoopInputFormatIOTest {
  static SerializableConfiguration serConf;
  static SimpleFunction<Text, String> myKeyTranslate;
  static SimpleFunction<Employee, String> myValueTranslate;

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private PBegin input = PBegin.in(p);

  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    serConf = loadTestConfiguration(
                  EmployeeInputFormat.class,
                  Text.class,
                  Employee.class);
    myKeyTranslate = new SimpleFunction<Text, String>() {
      @Override
      public String apply(Text input) {
        return input.toString();
      }
    };
    myValueTranslate = new SimpleFunction<Employee, String>() {
      @Override
      public String apply(Employee input) {
        return input.getEmpName() + "_" + input.getEmpAddress();
      }
    };
  }

  @Test
  public void testReadBuildsCorrectly() {
    HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(myKeyTranslate)
        .withValueTranslation(myValueTranslate);
    assertEquals(serConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(myValueTranslate.getOutputTypeDescriptor(), read.getValueClass());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor(), read.getKeyClass());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} builds correctly in different order
   * of with configuration/key translation/value translation. This test also validates output
   * PCollection key/value classes are set correctly even if Hadoop configuration is set after
   * setting key/value translation.
   */
  @Test
  public void testReadBuildsCorrectlyInDifferentOrder() {
    HadoopInputFormatIO.Read<String, String> read =
        HadoopInputFormatIO.<String, String>read()
            .withValueTranslation(myValueTranslate)
            .withConfiguration(serConf.getHadoopConfiguration())
            .withKeyTranslation(myKeyTranslate);
    assertEquals(serConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor(), read.getKeyClass());
    assertEquals(myValueTranslate.getOutputTypeDescriptor(), read.getValueClass());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} object creation if
   * {@link HadoopInputFormatIO.Read#withConfiguration() withConfiguration()} is called more than
   * once.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testReadBuildsCorrectlyIfWithConfigurationIsCalledMoreThanOneTime()
      throws IOException, InterruptedException {
    SerializableConfiguration diffConf =
        loadTestConfiguration(
            EmployeeInputFormat.class,
            Employee.class,
            Text.class);
    HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(myKeyTranslate)
        .withConfiguration(diffConf.getHadoopConfiguration());
    assertEquals(diffConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(null, read.getValueTranslationFunction());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor(), read.getKeyClass());
    assertEquals(diffConf.getHadoopConfiguration()
        .getClass(HadoopInputFormatIOConstants.VALUE_CLASS, Object.class), read.getValueClass().getRawType());
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#populateDisplayData()
   * populateDisplayData()}.
   */
  @Test
  public void testReadDisplayData() {
    HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(myKeyTranslate)
        .withValueTranslation(myValueTranslate);
    DisplayData displayData = DisplayData.from(read);
    Iterator<Entry<String, String>> propertyElement = serConf.getHadoopConfiguration().iterator();
    while (propertyElement.hasNext()) {
      Entry<String, String> element = propertyElement.next();
      assertThat(displayData, hasDisplayItem(element.getKey(), element.getValue()));
    }
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * null configuration. {@link HadoopInputFormatIO.Read#withConfiguration() withConfiguration()}
   * method checks configuration is null and throws exception if it is null.
   */
  @Test
  public void testReadObjectCreationFailsIfConfigurationIsNull() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.NULL_CONFIGURATION_ERROR_MSG);
    HadoopInputFormatIO.<Text, Employee>read()
          .withConfiguration(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with only
   * configuration.
   */
  @Test
  public void testReadObjectCreationWithConfiguration() {
    HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(serConf.getHadoopConfiguration());
    assertEquals(serConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(null, read.getKeyTranslationFunction());
    assertEquals(null, read.getValueTranslationFunction());
    assertEquals(serConf.getHadoopConfiguration().getClass(HadoopInputFormatIOConstants.KEY_CLASS,
        Object.class), read.getKeyClass().getRawType());
    assertEquals(serConf.getHadoopConfiguration().getClass(HadoopInputFormatIOConstants.VALUE_CLASS,
        Object.class), read.getValueClass().getRawType());

  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * configuration and null key translation. {@link HadoopInputFormatIO.Read#withKeyTranslation()
   * withKeyTranslation()} checks keyTranslation is null and throws exception if it null value is
   * passed.
   */
  @Test
  public void testReadObjectCreationFailsIfKeyTranslationFunctionIsNull() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.NULL_KEY_TRANSLATIONFUNC_ERROR_MSG);
    HadoopInputFormatIO.<String, Employee>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with
   * configuration and key translation.
   */
  @Test
  public void testReadObjectCreationWithConfigurationKeyTranslation() {
    HadoopInputFormatIO.Read<String, Employee> read = HadoopInputFormatIO.<String, Employee>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(myKeyTranslate);
    assertEquals(serConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(null, read.getValueTranslationFunction());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),
        read.getKeyClass().getRawType());
    assertEquals(serConf.getHadoopConfiguration().getClass("value.class", Object.class),
        read.getValueClass().getRawType());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * configuration and null value translation.
   * {@link HadoopInputFormatIO.Read#withValueTranslation() withValueTranslation()} checks
   * valueTranslation is null and throws exception if null value is passed.
   */
  @Test
  public void testReadObjectCreationFailsIfValueTranslationFunctionIsNull() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.NULL_VALUE_TRANSLATIONFUNC_ERROR_MSG);
    HadoopInputFormatIO.<Text, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withValueTranslation(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with
   * configuration and value translation.
   */
  @Test
  public void testReadObjectCreationWithConfigurationValueTranslation() {
    HadoopInputFormatIO.Read<Text, String> read = HadoopInputFormatIO.<Text, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withValueTranslation(myValueTranslate);
    assertEquals(serConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(null, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(serConf.getHadoopConfiguration().getClass("key.class", Object.class),
        read.getKeyClass().getRawType());
    assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),
        read.getValueClass().getRawType());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with
   * configuration, key translation and value translation.
   */
  @Test
  public void testReadObjectCreationWithConfigurationKeyTranslationValueTranslation() {
    HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(myKeyTranslate)
        .withValueTranslation(myValueTranslate);
    assertEquals(serConf.getHadoopConfiguration(),
        read.getConfiguration().getHadoopConfiguration());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),
        read.getKeyClass().getRawType());
    assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),
        read.getValueClass().getRawType());
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#validate()
   * Read.validate()} function when Read transform is created without calling
   * {@link HadoopInputFormatIO.Read#withConfiguration() withConfiguration()}.
   */
  @Test
  public void testReadValidationFailsMissingConfiguration() {
    HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.MISSING_CONFIGURATION_ERROR_MSG);
    read.validate(input);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when Hadoop InputFormat class is not provided by the user in
   * configuration.
   */
  @Test
  public void testReadValidationFailsMissingInputFormatInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopInputFormatIOConstants.KEY_CLASS, Text.class, Object.class);
    configuration.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, Employee.class, Object.class);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_ERROR_MSG);
    HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when key class is not provided by the user in configuration.
   */
  @Test
  public void testReadValidationFailsMissingKeyClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        EmployeeInputFormat.class, InputFormat.class);
    configuration.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, Employee.class, Object.class);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_KEY_CLASS_ERROR_MSG);
    HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when value class is not provided by the user in configuration.
   */
  @Test
  public void testReadValidationFailsMissingValueClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        EmployeeInputFormat.class, InputFormat.class);
    configuration.setClass(HadoopInputFormatIOConstants.KEY_CLASS, Text.class, Object.class);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_VALUE_CLASS_ERROR_MSG);
    HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#validate()
   * Read.validate()} function when myKeyTranslate's (simple function provided by user for key
   * translation) input type is not same as Hadoop InputFormat's keyClass(Which is property set in
   * configuration as "key.class").
   */
  @Test
  public void testReadValidationFailsWithWrongInputTypeKeyTranslationFunction() {
    SimpleFunction<LongWritable, String> myKeyTranslateWithWrongInputType =
        new SimpleFunction<LongWritable, String>() {
          @Override
          public String apply(LongWritable input) {
            return input.toString();
          }
        };
    HadoopInputFormatIO.Read<String, Employee> read = HadoopInputFormatIO.<String, Employee>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withKeyTranslation(myKeyTranslateWithWrongInputType);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        String.format(HadoopInputFormatIOConstants.WRONG_KEY_TRANSLATIONFUNC_ERROR_MSG,
            serConf.getHadoopConfiguration()
                .getClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME, InputFormat.class),
            serConf.getHadoopConfiguration().getClass(HadoopInputFormatIOConstants.KEY_CLASS,
                Object.class)));
    read.validate(input);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#validate() Read.validate()} function when myValueTranslate's (simple
   * function provided by user for value translation) input type is not same as Hadoop InputFormat's
   * valueClass(Which is property set in configuration as "value.class").
   */
  @Test
  public void testReadValidationFailsWithWrongInputTypeValueTranslationFunction() {
    SimpleFunction<LongWritable, String> myValueTranslateWithWrongInputType =
        new SimpleFunction<LongWritable, String>() {
          @Override
          public String apply(LongWritable input) {
            return input.toString();
          }
        };
    HadoopInputFormatIO.Read<Text, String> read = HadoopInputFormatIO.<Text, String>read()
            .withConfiguration(serConf.getHadoopConfiguration())
            .withValueTranslation(myValueTranslateWithWrongInputType);
    String expectedMessage =
        String.format(HadoopInputFormatIOConstants.WRONG_VALUE_TRANSLATIONFUNC_ERROR_MSG,
            serConf.getHadoopConfiguration()
                .getClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME, InputFormat.class),
            serConf.getHadoopConfiguration()
                .getClass(HadoopInputFormatIOConstants.VALUE_CLASS, Object.class));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(expectedMessage);
    read.validate(input);
  }

  /**
   * This test validates reading from Hadoop InputFormat if wrong key class is set in
   * configuration.
   */
  @Test
  public void testReadFailsWithWrongKeyClass() {
    SerializableConfiguration wrongConf = loadTestConfiguration(
       EmployeeInputFormat.class,
       MapWritable.class, // Actual key class is Text.class.
       Employee.class);
    HadoopInputFormatIO.Read<Text, String> read = HadoopInputFormatIO.<Text, String>read()
        .withConfiguration(wrongConf.getHadoopConfiguration());
    String expectedMessage = String.format(
        "java.lang.IllegalArgumentException: "
            + HadoopInputFormatIOConstants.WRONG_INPUTFORMAT_KEY_CLASS_ERROR_MSG,
        Text.class.getName(), MapWritable.class.getName());
    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage(expectedMessage);
    p.apply("ReadTest", read);
    p.run();
  }

  /**
   * This test validates reading from Hadoop InputFormat if wrong value class is set in
   * configuration.
   */
  @Test
  public void testReadFailsWithWrongValueClass() {
    SerializableConfiguration wrongConf = loadTestConfiguration(
       EmployeeInputFormat.class,
       Text.class,
       MapWritable.class); // Actual value class is Employee.class.
    HadoopInputFormatIO.Read<Text, MapWritable> read = HadoopInputFormatIO.<Text, MapWritable>read()
        .withConfiguration(wrongConf.getHadoopConfiguration());
    String expectedMessage = String.format(
        "java.lang.IllegalArgumentException: "
            + HadoopInputFormatIOConstants.WRONG_INPUTFORMAT_VALUE_CLASS_ERROR_MSG,
        Employee.class.getName(), MapWritable.class.getName());
    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage(expectedMessage);
    p.apply("ReadTest", read);
    p.run();
  }

  @Test
  public void testReadingData() throws Exception {
    HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(serConf.getHadoopConfiguration());
    List<KV<Text, Employee>> expected = TestEmployeeDataSet.getEmployeeData();
    PCollection<KV<Text, Employee>> actual = p.apply("ReadTest", read);
    PAssert.that(actual).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource} if RecordReader object
   * creation fails.
   */
  @Test
  public void testReadIfCreateRecordReaderFails() throws Exception {
    thrown.expect(Exception.class);
    thrown.expectMessage("Exception in creating RecordReader");
    InputFormat<Text, Employee> mockInputFormat = Mockito.mock(EmployeeInputFormat.class);
    Mockito.when(
        mockInputFormat.createRecordReader(Mockito.any(InputSplit.class),
            Mockito.any(TaskAttemptContext.class))).thenThrow(
        new IOException("Exception in creating RecordReader"));
    HadoopInputFormatBoundedSource<Text, Employee> boundedSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            new SerializableSplit());
    boundedSource.setInputFormatObj(mockInputFormat);
    SourceTestUtils.readFromSource(boundedSource, p.getOptions());
  }

  /**
   * This test validates behavior of HadoopInputFormatSource if
   * {@link InputFormat#createRecordReader() createRecordReader()} of InputFormat returns null.
   */
  @Test
  public void testReadWithNullCreateRecordReader() throws Exception {
    InputFormat<Text, Employee> mockInputFormat = Mockito.mock(EmployeeInputFormat.class);
    thrown.expect(IOException.class);
    thrown
        .expectMessage(String.format(
            HadoopInputFormatIOConstants.NULL_CREATE_RECORDREADER_ERROR_MSG,
            mockInputFormat.getClass()));
    Mockito.when(
        mockInputFormat.createRecordReader(Mockito.any(InputSplit.class),
            Mockito.any(TaskAttemptContext.class))).thenReturn(null);
    HadoopInputFormatBoundedSource<Text, Employee> boundedSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            new SerializableSplit());
    boundedSource.setInputFormatObj(mockInputFormat);
    SourceTestUtils.readFromSource(boundedSource, p.getOptions());
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource.HadoopInputFormatReader#start()
   * start()} method if InputFormat's {@link InputFormat#getSplits() getSplits()} returns
   * InputSplitList having zero records.
   */
  @Test
  public void testReadersStartWhenZeroRecords() throws Exception {
    InputFormat<Text, Employee> mockInputFormat = Mockito.mock(EmployeeInputFormat.class);
    EmployeeRecordReader mockReader = Mockito.mock(EmployeeRecordReader.class);
    Mockito.when(
        mockInputFormat.createRecordReader(Mockito.any(InputSplit.class),
            Mockito.any(TaskAttemptContext.class))).thenReturn(mockReader);
    Mockito.when(mockReader.nextKeyValue()).thenReturn(false);
    InputSplit mockInputSplit = Mockito.mock(NewObjectsEmployeeInputSplit.class);
    HadoopInputFormatBoundedSource<Text, Employee> boundedSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            new SerializableSplit(mockInputSplit));
    BoundedReader<KV<Text, Employee>> boundedReader = boundedSource.createReader(p.getOptions());
    assertEquals(false, boundedReader.start());
    assertEquals(Double.valueOf(1), boundedReader.getFractionConsumed());
  }

  /**
   * This test validates the method getFractionConsumed()- which indicates the progress of the read
   * in range of 0 to 1.
   */
  @Test
  public void testReadersGetFractionConsumed() throws Exception {
    List<KV<Text, Employee>> referenceRecords = TestEmployeeDataSet.getEmployeeData();
    HadoopInputFormatBoundedSource<Text, Employee> hifSource = getTestHIFSource(
        EmployeeInputFormat.class,
        Text.class,
        Employee.class,
        WritableCoder.of(Text.class),
        AvroCoder.of(Employee.class));
    long estimatedSize = hifSource.getEstimatedSizeBytes(p.getOptions());
    // Validate if estimated size is equal to the size of records.
    assertEquals(referenceRecords.size(), estimatedSize);
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList =
        hifSource.splitIntoBundles(0, p.getOptions());
    // Validate if splitIntoBundles() has split correctly.
    assertEquals(TestEmployeeDataSet.NUMBER_OF_SPLITS, boundedSourceList.size());
    List<KV<Text, Employee>> bundleRecords = new ArrayList<>();
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      List<KV<Text, Employee>> elements = new ArrayList<KV<Text, Employee>>();
      BoundedReader<KV<Text, Employee>> reader = source.createReader(p.getOptions());
      float recordsRead = 0;
      // When start is not called, getFractionConsumed() should return 0.
      assertEquals(Double.valueOf(0), reader.getFractionConsumed());
      boolean start = reader.start();
      assertEquals(true, start);
      if (start) {
        elements.add(reader.getCurrent());
        boolean advance = reader.advance();
        // Validate if getFractionConsumed() returns the correct fraction based on
        // the number of records read in the split.
        assertEquals(
            Double.valueOf(++recordsRead / TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT),
            reader.getFractionConsumed());
        assertEquals(true, advance);
        while (advance) {
          elements.add(reader.getCurrent());
          advance = reader.advance();
          assertEquals(
              Double.valueOf(++recordsRead / TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT),
              reader.getFractionConsumed());
        }
        bundleRecords.addAll(elements);
      }
      // Validate if getFractionConsumed() returns 1 after reading is complete.
      assertEquals(Double.valueOf(1), reader.getFractionConsumed());
      reader.close();
    }
    assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
  }

  /**
   * This test validates that reader and its parent source reads the same records.
   */
  @Test
  public void testReaderAndParentSourceReadsSameData() throws Exception {
    InputSplit mockInputSplit = Mockito.mock(NewObjectsEmployeeInputSplit.class);
    HadoopInputFormatBoundedSource<Text, Employee> boundedSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            new SerializableSplit(mockInputSplit));
    BoundedReader<KV<Text, Employee>> reader = boundedSource
        .createReader(p.getOptions());
    SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(reader, p.getOptions());
  }

  /**
   * This test verifies that the method
   * {@link HadoopInputFormatBoundedSource.HadoopInputFormatReader#getCurrentSource()
   * getCurrentSource()} returns correct source object.
   */
  @Test
  public void testGetCurrentSourceFunction() throws Exception {
    SerializableSplit split = new SerializableSplit();
    BoundedSource<KV<Text, Employee>> source =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            split);
    BoundedReader<KV<Text, Employee>> HIFReader = source.createReader(p.getOptions());
    BoundedSource<KV<Text, Employee>> HIFSource = HIFReader.getCurrentSource();
    assertEquals(HIFSource, source);
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#createReader()
   * createReader()} method when {@link HadoopInputFormatBoundedSource#splitIntoBundles()
   * splitIntoBundles()} is not called.
   */
  @Test
  public void testCreateReaderIfSplitIntoBundlesNotCalled() throws Exception {
    HadoopInputFormatBoundedSource<Text, Employee> hifSource = getTestHIFSource(
        EmployeeInputFormat.class,
        Text.class,
        Employee.class,
        WritableCoder.of(Text.class),
        AvroCoder.of(Employee.class));
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.CREATEREADER_UNSPLIT_SOURCE_ERROR_MSG);
    hifSource.createReader(p.getOptions());
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#computeSplitsIfNecessary()
   * computeSplits()} when Hadoop InputFormat's {@link InputFormat#getSplits() getSplits()}
   * returns empty list.
   */
  @Test
  public void testComputeSplitsIfGetSplitsReturnsEmptyList() throws Exception {
    InputFormat<?, ?> mockInputFormat = Mockito.mock(EmployeeInputFormat.class);
    SerializableSplit mockInputSplit = Mockito.mock(SerializableSplit.class);
    Mockito.when(mockInputFormat.getSplits(Mockito.any(JobContext.class))).thenReturn(
        new ArrayList<InputSplit>());
    HadoopInputFormatBoundedSource<Text, Employee> hifSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            mockInputSplit);
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.COMPUTESPLITS_EMPTY_SPLITS_ERROR_MSG);
    hifSource.setInputFormatObj(mockInputFormat);
    hifSource.computeSplitsIfNecessary();
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#computeSplitsIfNecessary()
   * computeSplits()} when Hadoop InputFormat's {@link InputFormat#getSplits() getSplits()}
   * returns NULL value.
   */
  @Test
  public void testComputeSplitsIfGetSplitsReturnsNullValue() throws Exception {
    InputFormat<Text, Employee> mockInputFormat = Mockito.mock(EmployeeInputFormat.class);
    SerializableSplit mockInputSplit = Mockito.mock(SerializableSplit.class);
    Mockito.when(mockInputFormat.getSplits(Mockito.any(JobContext.class))).thenReturn(null);
    HadoopInputFormatBoundedSource<Text, Employee> hifSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            mockInputSplit);
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.COMPUTESPLITS_NULL_GETSPLITS_ERROR_MSG);
    hifSource.setInputFormatObj(mockInputFormat);
    hifSource.computeSplitsIfNecessary();
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#computeSplitsIfNecessary()
   * computeSplits()} if Hadoop InputFormat's {@link InputFormat#getSplits() getSplits()} returns
   * InputSplit list having some null values.
   */
  @Test
  public void testComputeSplitsIfGetSplitsReturnsListHavingNullValues() throws Exception {
    // InputSplit list having null value.
    InputSplit mockInputSplit =
        Mockito.mock(InputSplit.class, Mockito.withSettings().extraInterfaces(Writable.class));
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    inputSplitList.add(mockInputSplit);
    inputSplitList.add(null);
    InputFormat<Text, Employee> mockInputFormat = Mockito.mock(EmployeeInputFormat.class);
    Mockito.when(mockInputFormat.getSplits(Mockito.any(JobContext.class))).thenReturn(
        inputSplitList);
    HadoopInputFormatBoundedSource<Text, Employee> hifSource =
        new HadoopInputFormatBoundedSource<Text, Employee>(
            serConf,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class),
            null, // No key translation required.
            null, // No value translation required.
            new SerializableSplit());
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOConstants.COMPUTESPLITS_NULL_SPLIT_ERROR_MSG);
    hifSource.setInputFormatObj(mockInputFormat);
    hifSource.computeSplitsIfNecessary();
  }

  /**
   * This test validates records emitted in PCollection are immutable if InputFormat's recordReader
   * returns same objects(i.e. same locations in memory) but with updated values for each record.
   */
  @Test
  public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreMutable() throws Exception {
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList = getBoundedSourceList(
       ReuseObjectsEmployeeInputFormat.class,
       Text.class,
       Employee.class,
       WritableCoder.of(Text.class),
       AvroCoder.of(Employee.class));
    List<KV<Text, Employee>> bundleRecords = new ArrayList<>();
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      List<KV<Text, Employee>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
      bundleRecords.addAll(elems);
    }
    List<KV<Text, Employee>> referenceRecords = TestEmployeeDataSet.getEmployeeData();
    assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
  }

  /**
   * Test reading if InputFormat implements {@link org.apache.hadoop.conf.Configurable
   * Configurable}.
   */
  @Test
  public void testReadingWithConfigurableInputFormat() throws Exception {
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList = getBoundedSourceList(
        ConfigurableEmployeeInputFormat.class,
        Text.class,
        Employee.class,
        WritableCoder.of(Text.class),
        AvroCoder.of(Employee.class));
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      // Cast to HadoopInputFormatBoundedSource to access getInputFormat().
      @SuppressWarnings("unchecked")
      HadoopInputFormatBoundedSource<Text, Employee> hifSource =
          (HadoopInputFormatBoundedSource<Text, Employee>) source;
      hifSource.createInputFormatInstance();
      ConfigurableEmployeeInputFormat inputFormatObj =
          (ConfigurableEmployeeInputFormat) hifSource.getInputFormat();
      assertEquals(true, inputFormatObj.isConfSet);
    }
  }

  /**
   * This test validates records emitted in PCollection are immutable if InputFormat's
   * {@link org.apache.hadoop.mapreduce.RecordReader RecordReader} returns different objects (i.e.
   * different locations in memory).
   */
  @Test
  public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreImmutable() throws Exception {
   List<BoundedSource<KV<Text, Employee>>> boundedSourceList = getBoundedSourceList(
       EmployeeInputFormat.class,
       Text.class,
       Employee.class,
       WritableCoder.of(Text.class),
       AvroCoder.of(Employee.class));
    List<KV<Text, Employee>> bundleRecords = new ArrayList<>();
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      List<KV<Text, Employee>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
      bundleRecords.addAll(elems);
    }
    List<KV<Text, Employee>> referenceRecords = TestEmployeeDataSet.getEmployeeData();
    assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
  }

  /**
   * This test validates {@link WritableCoder WritableCoder's} encoding and decoding process.
   */
  @Test
  public void testLinkedMapWritableEncoding() throws Exception {
    LinkedMapWritable map = new LinkedMapWritable();
    map.put(new Text("path"), new Text("/home/asharma/MOCK1.csv"));
    map.put(new Text("country"), new Text("Czech Republic"));
    map.put(new Text("@timestamp"), new Text("2016-11-11T08:06:42.260Z"));
    map.put(new Text("gender"), new Text("Male"));
    map.put(new Text("@version"), new Text("1"));
    map.put(new Text("Id"), new Text("131"));
    map.put(new Text("salary"), new Text("$8.65"));
    map.put(new Text("email"), new Text("alex@example.com"));
    map.put(new Text("desc"), new Text("Other contact with macaw, subsequent encounter"));
    WritableCoder<LinkedMapWritable> coder = WritableCoder.of(LinkedMapWritable.class);
    CoderUtils.clone(coder, map);
    CoderProperties.coderDecodeEncodeEqual(coder, map);
  }

  /**
   * This test validates if a method {@link Read#getDefaultCoder() getDefaultCoder()} gives
   * {@link VarLongCoder} if input class is Long.
   */
  @Test
  public void testDefaultCoderFromCodeRegistry() {
    Configuration conf =
        loadTestConfiguration(
            org.elasticsearch.hadoop.mr.EsInputFormat.class,
            Text.class,
            MapWritable.class)
        .getHadoopConfiguration();
    TypeDescriptor<Long> typedesc = new TypeDescriptor<Long>() {};
    Pipeline pipeline = TestPipeline.create();
    Read<Text, String> read = HadoopInputFormatIO.<Text, String>read().withConfiguration(conf);
    Coder<Long> coder = read.getDefaultCoder(typedesc, pipeline.getCoderRegistry());
    assertEquals(coder.getClass(), VarLongCoder.class);
  }

  /**
   * This test validates if a method {@link Read#getDefaultCoder() getDefaultCoder()} gives
   * WritableCoder if input class is instance of Writable class.
   */
  @Test
  public void testWritableCoder() {
    Configuration conf =
        loadTestConfiguration(
            org.elasticsearch.hadoop.mr.EsInputFormat.class,
            Text.class,
            MapWritable.class)
        .getHadoopConfiguration();
    TypeDescriptor<MapWritable> typedesc = new TypeDescriptor<MapWritable>() {};
    DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
    Pipeline pipeline = Pipeline.create(directRunnerOptions);
    Read<Text, String> read = HadoopInputFormatIO.<Text, String>read().withConfiguration(conf);
    Coder<MapWritable> coder = read.getDefaultCoder(typedesc, pipeline.getCoderRegistry());
    assertEquals(coder.getClass(), WritableCoder.class);
  }

  /**
   * This test validates if IllegalStateException with message CoderNotFoundException is thrown if
   * Beam Coder is not available for the given input class.
   */
  @Test(expected = IllegalStateException.class)
  public void testNonRegisteredCustomCoder() {
    Configuration conf =
        loadTestConfiguration(
            org.elasticsearch.hadoop.mr.EsInputFormat.class,
            Text.class,
            MapWritable.class)
        .getHadoopConfiguration();
    TypeDescriptor<Row> typedesc = new TypeDescriptor<Row>() {};
    DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
    Pipeline pipeline = Pipeline.create(directRunnerOptions);
    Read<Text, String> read = HadoopInputFormatIO.<Text, String>read().withConfiguration(conf);
    read.getDefaultCoder(typedesc, pipeline.getCoderRegistry());
  }

  private static SerializableConfiguration loadTestConfiguration(
      Class<?> inputFormatClassName,
      Class<?> keyClass,
      Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        inputFormatClassName,
        InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, keyClass, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, valueClass, Object.class);
    return new SerializableConfiguration(conf);
  }

  private <K, V> HadoopInputFormatBoundedSource<K, V> getTestHIFSource(
      Class<?> inputFormatClass,
      Class<K> inputFormatKeyClass,
      Class<V> inputFormatValueClass,
      Coder<K> keyCoder,
      Coder<V> valueCoder){
    SerializableConfiguration serConf =
        loadTestConfiguration(
            inputFormatClass,
            inputFormatKeyClass,
            inputFormatValueClass);
    return new HadoopInputFormatBoundedSource<K, V>(
            serConf,
            keyCoder,
            valueCoder,
            null, // No key translation required.
            null);// No value translation required.
  }

  private <K, V> List<BoundedSource<KV<K, V>>> getBoundedSourceList(
      Class<?> inputFormatClass,
      Class<K> inputFormatKeyClass,
      Class<V> inputFormatValueClass,
      Coder<K> keyCoder,
      Coder<V> valueCoder) throws Exception{
    HadoopInputFormatBoundedSource<K, V> boundedSource = getTestHIFSource(
        inputFormatClass,
        inputFormatKeyClass,
        inputFormatValueClass,
        keyCoder,
        valueCoder);
    return boundedSource.splitIntoBundles(0, p.getOptions());
  }
}
