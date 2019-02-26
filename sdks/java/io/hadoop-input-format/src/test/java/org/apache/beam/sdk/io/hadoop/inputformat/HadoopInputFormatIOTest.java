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
package org.apache.beam.sdk.io.hadoop.inputformat;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link HadoopInputFormatIO}. */
@RunWith(JUnit4.class)
public class HadoopInputFormatIOTest {
  private static SerializableConfiguration serConf;
  private static SimpleFunction<Text, String> myKeyTranslate;
  private static SimpleFunction<Employee, String> myValueTranslate;

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private PBegin input = PBegin.in(p);

  @BeforeClass
  public static void setUp() {
    serConf = loadTestConfiguration(EmployeeInputFormat.class, Text.class, Employee.class);
    myKeyTranslate =
        new SimpleFunction<Text, String>() {
          @Override
          public String apply(Text input) {
            return input.toString();
          }
        };
    myValueTranslate =
        new SimpleFunction<Employee, String>() {
          @Override
          public String apply(Employee input) {
            return input.getEmpName() + "_" + input.getEmpAddress();
          }
        };
  }

  @Test
  public void testReadBuildsCorrectly() {
    HadoopInputFormatIO.Read<String, String> read =
        HadoopInputFormatIO.<String, String>read()
            .withConfiguration(serConf.get())
            .withKeyTranslation(myKeyTranslate)
            .withValueTranslation(myValueTranslate);
    assertEquals(serConf.get(), read.getConfiguration().get());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(myValueTranslate.getOutputTypeDescriptor(), read.getValueTypeDescriptor());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor(), read.getKeyTypeDescriptor());
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
            .withConfiguration(serConf.get())
            .withKeyTranslation(myKeyTranslate);
    assertEquals(serConf.get(), read.getConfiguration().get());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor(), read.getKeyTypeDescriptor());
    assertEquals(myValueTranslate.getOutputTypeDescriptor(), read.getValueTypeDescriptor());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} object creation if {@link
   * HadoopInputFormatIO.Read#withConfiguration() withConfiguration()} is called more than once.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testReadBuildsCorrectlyIfWithConfigurationIsCalledMoreThanOneTime() {
    SerializableConfiguration diffConf =
        loadTestConfiguration(EmployeeInputFormat.class, Employee.class, Text.class);
    HadoopInputFormatIO.Read<String, String> read =
        HadoopInputFormatIO.<String, String>read()
            .withConfiguration(serConf.get())
            .withKeyTranslation(myKeyTranslate)
            .withConfiguration(diffConf.get());
    assertEquals(diffConf.get(), read.getConfiguration().get());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(null, read.getValueTranslationFunction());
    assertEquals(myKeyTranslate.getOutputTypeDescriptor(), read.getKeyTypeDescriptor());
    assertEquals(
        diffConf.get().getClass("value.class", Object.class),
        read.getValueTypeDescriptor().getRawType());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * null configuration. {@link HadoopInputFormatIO.Read#withConfiguration() withConfiguration()}
   * method checks configuration is null and throws exception if it is null.
   */
  @Test
  public void testReadObjectCreationFailsIfConfigurationIsNull() {
    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<Text, Employee>read().withConfiguration(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with only
   * configuration.
   */
  @Test
  public void testReadObjectCreationWithConfiguration() {
    HadoopInputFormatIO.Read<Text, Employee> read =
        HadoopInputFormatIO.<Text, Employee>read().withConfiguration(serConf.get());
    assertEquals(serConf.get(), read.getConfiguration().get());
    assertEquals(null, read.getKeyTranslationFunction());
    assertEquals(null, read.getValueTranslationFunction());
    assertEquals(
        serConf.get().getClass("key.class", Object.class),
        read.getKeyTypeDescriptor().getRawType());
    assertEquals(
        serConf.get().getClass("value.class", Object.class),
        read.getValueTypeDescriptor().getRawType());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * configuration and null key translation. {@link HadoopInputFormatIO.Read#withKeyTranslation()
   * withKeyTranslation()} checks keyTranslation is null and throws exception if it null value is
   * passed.
   */
  @Test
  public void testReadObjectCreationFailsIfKeyTranslationFunctionIsNull() {
    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<String, Employee>read()
        .withConfiguration(serConf.get())
        .withKeyTranslation(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with
   * configuration and key translation.
   */
  @Test
  public void testReadObjectCreationWithConfigurationKeyTranslation() {
    HadoopInputFormatIO.Read<String, Employee> read =
        HadoopInputFormatIO.<String, Employee>read()
            .withConfiguration(serConf.get())
            .withKeyTranslation(myKeyTranslate);
    assertEquals(serConf.get(), read.getConfiguration().get());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(null, read.getValueTranslationFunction());
    assertEquals(
        myKeyTranslate.getOutputTypeDescriptor().getRawType(),
        read.getKeyTypeDescriptor().getRawType());
    assertEquals(
        serConf.get().getClass("value.class", Object.class),
        read.getValueTypeDescriptor().getRawType());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation fails with
   * configuration and null value translation. {@link
   * HadoopInputFormatIO.Read#withValueTranslation() withValueTranslation()} checks valueTranslation
   * is null and throws exception if null value is passed.
   */
  @Test
  public void testReadObjectCreationFailsIfValueTranslationFunctionIsNull() {
    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<Text, String>read()
        .withConfiguration(serConf.get())
        .withValueTranslation(null);
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with
   * configuration and value translation.
   */
  @Test
  public void testReadObjectCreationWithConfigurationValueTranslation() {
    HadoopInputFormatIO.Read<Text, String> read =
        HadoopInputFormatIO.<Text, String>read()
            .withConfiguration(serConf.get())
            .withValueTranslation(myValueTranslate);
    assertEquals(serConf.get(), read.getConfiguration().get());
    assertEquals(null, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(
        serConf.get().getClass("key.class", Object.class),
        read.getKeyTypeDescriptor().getRawType());
    assertEquals(
        myValueTranslate.getOutputTypeDescriptor().getRawType(),
        read.getValueTypeDescriptor().getRawType());
  }

  /**
   * This test validates {@link HadoopInputFormatIO.Read Read} transform object creation with
   * configuration, key translation and value translation.
   */
  @Test
  public void testReadObjectCreationWithConfigurationKeyTranslationValueTranslation() {
    HadoopInputFormatIO.Read<String, String> read =
        HadoopInputFormatIO.<String, String>read()
            .withConfiguration(serConf.get())
            .withKeyTranslation(myKeyTranslate)
            .withValueTranslation(myValueTranslate);
    assertEquals(serConf.get(), read.getConfiguration().get());
    assertEquals(myKeyTranslate, read.getKeyTranslationFunction());
    assertEquals(myValueTranslate, read.getValueTranslationFunction());
    assertEquals(
        myKeyTranslate.getOutputTypeDescriptor().getRawType(),
        read.getKeyTypeDescriptor().getRawType());
    assertEquals(
        myValueTranslate.getOutputTypeDescriptor().getRawType(),
        read.getValueTypeDescriptor().getRawType());
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#validateTransform()
   * Read.validateTransform()} function when Read transform is created without calling {@link
   * HadoopInputFormatIO.Read#withConfiguration() withConfiguration()}.
   */
  @Test
  public void testReadValidationFailsMissingConfiguration() {
    HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.read();
    thrown.expect(IllegalArgumentException.class);
    read.validateTransform();
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when Hadoop InputFormat class is not provided by the user in
   * configuration.
   */
  @Test
  public void testReadValidationFailsMissingInputFormatInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass("key.class", Text.class, Object.class);
    configuration.setClass("value.class", Employee.class, Object.class);
    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<Text, Employee>read().withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when key class is not provided by the user in configuration.
   */
  @Test
  public void testReadValidationFailsMissingKeyClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(
        "mapreduce.job.inputformat.class", EmployeeInputFormat.class, InputFormat.class);
    configuration.setClass("value.class", Employee.class, Object.class);
    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<Text, Employee>read().withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#withConfiguration()
   * withConfiguration()} function when value class is not provided by the user in configuration.
   */
  @Test
  public void testReadValidationFailsMissingValueClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(
        "mapreduce.job.inputformat.class", EmployeeInputFormat.class, InputFormat.class);
    configuration.setClass("key.class", Text.class, Object.class);
    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<Text, Employee>read().withConfiguration(configuration);
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#validateTransform()
   * Read.validateTransform()} function when myKeyTranslate's (simple function provided by user for
   * key translation) input type is not same as Hadoop InputFormat's keyClass(Which is property set
   * in configuration as "key.class").
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
    HadoopInputFormatIO.Read<String, Employee> read =
        HadoopInputFormatIO.<String, Employee>read()
            .withConfiguration(serConf.get())
            .withKeyTranslation(myKeyTranslateWithWrongInputType);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        String.format(
            "Key translation's input type is not same as hadoop InputFormat : %s key "
                + "class : %s",
            serConf.get().getClass("mapreduce.job.inputformat.class", InputFormat.class),
            serConf.get().getClass("key.class", Object.class)));
    read.validateTransform();
  }

  /**
   * This test validates functionality of {@link HadoopInputFormatIO.Read#validateTransform()
   * Read.validateTransform()} function when myValueTranslate's (simple function provided by user
   * for value translation) input type is not same as Hadoop InputFormat's valueClass(Which is
   * property set in configuration as "value.class").
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
    HadoopInputFormatIO.Read<Text, String> read =
        HadoopInputFormatIO.<Text, String>read()
            .withConfiguration(serConf.get())
            .withValueTranslation(myValueTranslateWithWrongInputType);
    String expectedMessage =
        String.format(
            "Value translation's input type is not same as hadoop InputFormat :  "
                + "%s value class : %s",
            serConf.get().getClass("mapreduce.job.inputformat.class", InputFormat.class),
            serConf.get().getClass("value.class", Object.class));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(expectedMessage);
    read.validateTransform();
  }

  @Test
  public void testReadingData() {
    HadoopInputFormatIO.Read<Text, Employee> read =
        HadoopInputFormatIO.<Text, Employee>read().withConfiguration(serConf.get());
    List<KV<Text, Employee>> expected = TestEmployeeDataSet.getEmployeeData();
    PCollection<KV<Text, Employee>> actual = p.apply("ReadTest", read);
    PAssert.that(actual).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testValidateConfigurationWithDBInputFormat() {
    Configuration conf = new Configuration();
    conf.setClass("key.class", LongWritable.class, Object.class);
    conf.setClass("value.class", Text.class, Object.class);
    conf.setClass("mapreduce.job.inputformat.class", DBInputFormat.class, InputFormat.class);

    thrown.expect(IllegalArgumentException.class);
    HadoopInputFormatIO.<String, String>read()
        .withConfiguration(new SerializableConfiguration(conf).get())
        .withKeyTranslation(myKeyTranslate)
        .withValueTranslation(myValueTranslate);
  }

  private static SerializableConfiguration loadTestConfiguration(
      Class<?> inputFormatClassName, Class<?> keyClass, Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
    conf.setClass("key.class", keyClass, Object.class);
    conf.setClass("value.class", valueClass, Object.class);
    return new SerializableConfiguration(conf);
  }
}
