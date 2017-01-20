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
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOContants;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.BadCreateReaderInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.BadEmptySplitsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.BadGetSplitsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.BadNoRecordsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.BadNullCreateReaderInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.BadNullSplitsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.ConfigurableEmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.Employee;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.NewObjectsEmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.ReuseObjectsEmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.TestEmployeeDataSet;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
                  NewObjectsEmployeeInputFormat.class, 
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
   * {@link HadoopInputFormatIO.Read#withConfiguration() withConfiguration()} is called more that
   * one time.
   */
  @Test
  public void testReadBuildsCorrectlyIfWithConfigurationIsCalledMoreThanOneTime() {
    SerializableConfiguration diffConf =
        loadTestConfiguration(BadNullCreateReaderInputFormat.class, Employee.class, Text.class);
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
        .getClass(HadoopInputFormatIOContants.VALUE_CLASS, Object.class), read.getValueClass().getRawType());
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
    assertThat(displayData,
        hasDisplayItem("KeyTranslation", myKeyTranslate.toString()));
    assertThat(displayData,
        hasDisplayItem("ValueTranslation", myValueTranslate.toString()));
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * null configuration. {@link HadoopInputFormatIO.Read#withConfiguration() withConfiguration()}
   * method checks configuration is null and throws exception if it is null.
   */
  @Test
  public void testReadObjectCreationFailsIfConfigurationIsNull() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.NULL_CONFIGURATION_ERROR_MSG);
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
    assertEquals(serConf.getHadoopConfiguration().getClass(HadoopInputFormatIOContants.KEY_CLASS,
        Object.class), read.getKeyClass().getRawType());
    assertEquals(serConf.getHadoopConfiguration().getClass(HadoopInputFormatIOContants.VALUE_CLASS,
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
    thrown.expectMessage(HadoopInputFormatIOContants.NULL_KEY_TRANSLATIONFUNC_ERROR_MSG);
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
    thrown.expectMessage(HadoopInputFormatIOContants.NULL_VALUE_TRANSLATIONFUNC_ERROR_MSG);
    HadoopInputFormatIO.<Text, String>read()
        .withConfiguration(serConf.getHadoopConfiguration())
        .withValueTranslation(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with configuration and value translation.
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
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with configuration, key translation and
   * value translation.
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
    thrown.expectMessage(HadoopInputFormatIOContants.MISSING_CONFIGURATION_ERROR_MSG);
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
    configuration.setClass(HadoopInputFormatIOContants.KEY_CLASS, Text.class, Object.class);
    configuration.setClass(HadoopInputFormatIOContants.VALUE_CLASS, Employee.class, Object.class);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.MISSING_INPUTFORMAT_ERROR_MSG);
    HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when key class is not provided by the user in configuration.
   */
  @Test
  public void testReadValidationFailsMissingKeyClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
        NewObjectsEmployeeInputFormat.class, InputFormat.class);
    configuration.setClass(HadoopInputFormatIOContants.VALUE_CLASS, Employee.class, Object.class);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.MISSING_INPUTFORMAT_KEY_CLASS_ERROR_MSG);
    HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
        .withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when value class is not provided by the user in configuration.
   */
  @Test
  public void testReadValidationFailsMissingValueClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
        NewObjectsEmployeeInputFormat.class, InputFormat.class);
    configuration.setClass(HadoopInputFormatIOContants.KEY_CLASS, Text.class, Object.class);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.MISSING_INPUTFORMAT_VALUE_CLASS_ERROR_MSG);
    HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
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
        String.format(HadoopInputFormatIOContants.WRONG_KEY_TRANSLATIONFUNC_ERROR_MSG,
            serConf.getHadoopConfiguration()
                .getClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME, InputFormat.class),
            serConf.getHadoopConfiguration().getClass(HadoopInputFormatIOContants.KEY_CLASS,
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
        String.format(HadoopInputFormatIOContants.WRONG_VALUE_TRANSLATIONFUNC_ERROR_MSG,
            serConf.getHadoopConfiguration()
                .getClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME, InputFormat.class),
            serConf.getHadoopConfiguration()
                .getClass(HadoopInputFormatIOContants.VALUE_CLASS, Object.class));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(expectedMessage);
    read.validate(input);
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
   * This test validates behavior of {@link HadoopInputFormatBoundedSource} if RecordReader object creation fails
   * in {@link HadoopInputFormatBoundedSource.HadoopInputFormatReader#start() start()} method.
   */
  @Test
  public void testReadersStartIfCreateRecordReaderFails() throws Exception {
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList =getBoundedSourceList(
        BadCreateReaderInputFormat.class,
        Text.class, 
        Employee.class,
        WritableCoder.of(Text.class),
        AvroCoder.of(Employee.class));
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      BoundedReader<KV<Text, Employee>> reader = source.createReader(p.getOptions());
      thrown.expect(Exception.class);
      thrown
          .expectMessage("Exception in creating RecordReader in BadCreateRecordReaderInputFormat");
      reader.start();
    }
  }

  /**
   * This test validates behavior of HadoopInputFormatSource if
   * {@link InputFormat#createRecordReader() createRecordReader()} of InputFormat returns null.
   */
  @Test
  public void testReadersStartWithNullCreateRecordReader() throws Exception {
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList = getBoundedSourceList(
        BadNullCreateReaderInputFormat.class,
        Text.class, 
        Employee.class,
        WritableCoder.of(Text.class),
        AvroCoder.of(Employee.class));
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      BoundedReader<KV<Text, Employee>> reader = source.createReader(p.getOptions());
      thrown.expect(IOException.class);
      thrown.expectMessage(
          String.format(HadoopInputFormatIOContants.NULL_CREATE_RECORDREADER_ERROR_MSG,
              new BadNullCreateReaderInputFormat().getClass()));
      reader.start();
    }
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#createReader()
   * createReader()} and {@link HadoopInputFormatBoundedSource.HadoopInputFormatReader#start()
   * start()} methods if InputFormat's {@link InputFormat#getSplits() getSplits()} returns
   * InputSplitList having having no records.
   */
  @Test
  public void testReadersCreateReaderAndStartWithZeroRecords() throws Exception {
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList = getBoundedSourceList(
        BadNoRecordsInputFormat.class,
        Text.class, 
        Employee.class,
        WritableCoder.of(Text.class),
        AvroCoder.of(Employee.class));
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      BoundedReader<KV<Text, Employee>> reader = source.createReader(p.getOptions());
      assertEquals(false, reader.start());
    }
  }

  /**
   * This test validates fractions consumed while reading data.
   */
  @Test
  public void testReadersGetFractionConsumed() throws Exception {
    List<KV<Text, Employee>> referenceRecords = TestEmployeeDataSet.getEmployeeData();
    HadoopInputFormatBoundedSource<Text, Employee> hifSource = getTestHIFSource(
            NewObjectsEmployeeInputFormat.class, 
            Text.class,
            Employee.class,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class));
    long estimatedSize = hifSource.getEstimatedSizeBytes(p.getOptions());
    assertEquals(referenceRecords.size(), estimatedSize);
    List<BoundedSource<KV<Text, Employee>>> boundedSourceList = hifSource
            .splitIntoBundles(0, p.getOptions());
    assertEquals(TestEmployeeDataSet.NUMBER_OF_SPLITS, boundedSourceList.size());
    List<KV<Text, Employee>> bundleRecords = new ArrayList<>();
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      List<KV<Text, Employee>> elements = new ArrayList<KV<Text, Employee>>();
      BoundedReader<KV<Text, Employee>> reader = source.createReader(p.getOptions());
      float recordsRead = 0;
      assertEquals(Double.valueOf(0), reader.getFractionConsumed());
      boolean start = reader.start();
      assertEquals(true, start);
      if (start) {
        elements.add(reader.getCurrent());
        assertEquals(Double.valueOf(++recordsRead / TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT),
            reader.getFractionConsumed());
        boolean advance = reader.advance();
        assertEquals(true, advance);
        while (advance) {
          elements.add(reader.getCurrent());
          assertEquals(
              Double.valueOf(++recordsRead / TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT),
              reader.getFractionConsumed());
          advance = reader.advance();
        }
        bundleRecords.addAll(elements);
      }
      reader.close();
    }
    assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
  }

  /**
   * This test validates that reader and its parent source reads the same records.
   */
  @Test
  public void testReaderAndParentSourceReadsSameData() throws Exception {
    HadoopInputFormatBoundedSource<String, String> parentHIFSource =
        new HadoopInputFormatBoundedSource<String, String>(
            serConf, 
            StringUtf8Coder.of(),
            StringUtf8Coder.of(), 
            myKeyTranslate, 
            myValueTranslate, 
            null); // Initially split is null as source is not split.
    List<BoundedSource<KV<String, String>>> boundedSourceList =
        (List<BoundedSource<KV<String, String>>>) parentHIFSource
        .splitIntoBundles(0, p.getOptions());
    for (BoundedSource<KV<String, String>> source : boundedSourceList) {
      BoundedReader<KV<String, String>> reader = source.createReader(p.getOptions());
      SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(reader, p.getOptions());
    }
  }
  
  /**
   * This test verifies that the method
   * {@link HadoopInputFormatBoundedSource.HadoopInputFormatReader#getCurrentSource()
   * getCurrentSource()} returns correct source object.
   */
  @Test
  public void testGetCurrentSourceFunction() throws Exception {
    HadoopInputFormatBoundedSource<String, String> parentHIFSource =
        new HadoopInputFormatBoundedSource<String, String>(
            serConf, 
            StringUtf8Coder.of(),
            StringUtf8Coder.of(), 
            myKeyTranslate, 
            myValueTranslate, 
            null); // Initially split is null as source is not split.
    List<BoundedSource<KV<String, String>>> boundedSourceList =
        (List<BoundedSource<KV<String, String>>>) parentHIFSource
        .splitIntoBundles(0, p.getOptions());
    for (BoundedSource<KV<String, String>> source : boundedSourceList) {
      BoundedReader<KV<String, String>> HIFReader = source.createReader(p.getOptions());
      BoundedSource<KV<String, String>> HIFSource = HIFReader.getCurrentSource();
      assertEquals(HIFSource, source);
    }
  }


  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#createReader()
   * createReader()} method when {@link HadoopInputFormatBoundedSource#splitIntoBundles()
   * splitIntoBundles()} is not called.
   */
  @Test
  public void testCreateReaderIfSplitIntoBundlesNotCalled() throws Exception {
    HadoopInputFormatBoundedSource<String, String> parentHIFSource =
        new HadoopInputFormatBoundedSource<String, String>(
            serConf, 
            StringUtf8Coder.of(),
            StringUtf8Coder.of(), 
            myKeyTranslate, 
            myValueTranslate, 
            null); // Initially split is null as source is not split.
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.CREATEREADER_UNSPLIT_SOURCE_ERROR_MSG);
    parentHIFSource.createReader(p.getOptions());
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#getEstimatedSizeBytes()
   * getEstimatedSizeBytes()} and {@link HadoopInputFormatBoundedSource#splitIntoBundles()
   * splitIntoBundles()} when Hadoop InputFormat's {@link InputFormat#getSplits() getSplits()}
   * returns empty list.
   */
  @Test
  public void testSplitIntoBundlesIfGetSplitsReturnsEmptyList() throws Exception {
    HadoopInputFormatBoundedSource<Text, Employee> parentHIFSource = getTestHIFSource(
            BadEmptySplitsInputFormat.class, 
            Text.class, 
            Employee.class, 
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class));
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.COMPUTESPLITS_EMPTY_SPLITS_ERROR_MSG);
    parentHIFSource.getEstimatedSizeBytes(p.getOptions());
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.COMPUTESPLITS_EMPTY_SPLITS_ERROR_MSG);
    parentHIFSource.splitIntoBundles(0, p.getOptions());
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#getEstimatedSizeBytes()
   * getEstimatedSizeBytes()} and {@link HadoopInputFormatBoundedSource#splitIntoBundles()
   * splitIntoBundles()} when Hadoop InputFormat's {@link InputFormat#getSplits() getSplits()}
   * returns NULL value.
   */
  @Test
  public void testSplitIntoBundlesIfGetSplitsReturnsNullValue() throws Exception {
    HadoopInputFormatBoundedSource<Text, Employee> parentHIFSource = getTestHIFSource(
            BadNullSplitsInputFormat.class,
            Text.class, 
            Employee.class,
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class));
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_GETSPLITS_ERROR_MSG);
    parentHIFSource.getEstimatedSizeBytes(p.getOptions());

    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_GETSPLITS_ERROR_MSG);
    parentHIFSource.splitIntoBundles(0, p.getOptions());
  }

  /**
   * This test validates behavior of {@link HadoopInputFormatBoundedSource#getEstimatedSizeBytes()
   * getEstimatedSizeBytes()} and {@link HadoopInputFormatBoundedSource#splitIntoBundles()
   * splitIntoBundles()} if Hadoop InputFormat's {@link InputFormat#getSplits() getSplits()} returns
   * InputSplit list having some null values.
   */
  @Test
  public void testSplitIntoBundlesIfGetSplitsReturnsListHavingNullValues() throws Exception {
    HadoopInputFormatBoundedSource<Text, Employee> hifSource = getTestHIFSource(
            BadGetSplitsInputFormat.class, 
            Text.class, 
            Employee.class, 
            WritableCoder.of(Text.class),
            AvroCoder.of(Employee.class));
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_SPLIT_ERROR_MSG);
    hifSource.getEstimatedSizeBytes(p.getOptions());
    thrown.expect(IOException.class);
    thrown.expectMessage(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_SPLIT_ERROR_MSG);
    hifSource.splitIntoBundles(0, p.getOptions());
  }
  
  /**
   * This test validates functionality of {@link HadoopInputFormatIO} if user sets wrong key class
   * and value class.
   */
  @Test
  public void testHIFSourceIfUserSetsWrongKeyOrValueClass() throws Exception {
    List<BoundedSource<KV<String, String>>> boundedSourceList = getBoundedSourceList(
            NewObjectsEmployeeInputFormat.class,
            String.class, 
            String.class,
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    List<KV<String, String>> bundleRecords = new ArrayList<>();
    for (BoundedSource<KV<String, String>> source : boundedSourceList) {
      thrown.expect(ClassCastException.class);
      List<KV<String, String>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
      bundleRecords.addAll(elems);
    }
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
    List<KV<Text, Employee>> bundleRecords = new ArrayList<>();
    for (BoundedSource<KV<Text, Employee>> source : boundedSourceList) {
      List<KV<Text, Employee>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
      bundleRecords.addAll(elems);
    }
    List<KV<Text, Employee>> referenceRecords = TestEmployeeDataSet.getEmployeeData();
    assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
  }

  /**
   * This test validates records emitted in PCollection are immutable if InputFormat's
   * {@link org.apache.hadoop.mapreduce.RecordReader RecordReader} returns different objects (i.e.
   * different locations in memory).
   */
  @Test
  public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreImmutable() throws Exception {
   List<BoundedSource<KV<Text, Employee>>> boundedSourceList = getBoundedSourceList(
       NewObjectsEmployeeInputFormat.class, 
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

  private static SerializableConfiguration loadTestConfiguration(
      Class<?> inputFormatClassName,
      Class<?> keyClass, 
      Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME, 
        inputFormatClassName,
        InputFormat.class);
    conf.setClass(HadoopInputFormatIOContants.KEY_CLASS, keyClass, Object.class);
    conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS, valueClass, Object.class);
    return new SerializableConfiguration(conf);
  }
  
  private <K,V> HadoopInputFormatBoundedSource<K,V> getTestHIFSource(
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
    return new HadoopInputFormatBoundedSource<K,V>(
            serConf, 
            keyCoder,
            valueCoder);
  }

  private <K,V> List<BoundedSource<KV<K, V>>> getBoundedSourceList(
      Class<?> inputFormatClass,
      Class<K> inputFormatKeyClass,
      Class<V> inputFormatValueClass,
      Coder<K> keyCoder,
      Coder<V> valueCoder) throws Exception{
    HadoopInputFormatBoundedSource<K,V> boundedSource = getTestHIFSource(
        inputFormatClass,
        inputFormatKeyClass,
        inputFormatValueClass,
        keyCoder,
        valueCoder);        
    return boundedSource.splitIntoBundles(0, p.getOptions());
  }
}
