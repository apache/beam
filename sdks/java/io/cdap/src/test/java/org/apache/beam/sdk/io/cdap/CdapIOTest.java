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

import io.cdap.plugin.common.Constants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.cdap.context.BatchSourceContextImpl;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link CdapIO}. */
@RunWith(JUnit4.class)
public class CdapIOTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final Map<String, Object> TEST_EMPLOYEE_PARAMS_MAP =
      ImmutableMap.<String, Object>builder()
          .put(EmployeeBatchSourceConfig.OBJECT_TYPE, "employee")
          .put(Constants.Reference.REFERENCE_NAME, "referenceName")
          .build();

  @Test
  public void testReadBuildsCorrectly() {

    EmployeeBatchSourceConfig pluginConfig =
        new ConfigWrapper<>(EmployeeBatchSourceConfig.class)
            .withParams(TEST_EMPLOYEE_PARAMS_MAP)
            .build();

    CdapIO.Read<String, String> read =
        CdapIO.<String, String>read()
            .withCdapPlugin(
                Plugin.create(
                    EmployeeBatchSource.class,
                    EmployeeInputFormat.class,
                    EmployeeInputFormatProvider.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(String.class)
            .withValueClass(String.class);

    Plugin cdapPlugin = read.getCdapPlugin();
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
  public void testReadValidationFailsMissingCdapPluginClass() {
    CdapIO.Read<String, String> read = CdapIO.read();
    assertThrows(IllegalArgumentException.class, read::validateTransform);
  }

  @Test
  public void testReadObjectCreationFailsIfCdapPluginClassIsNotSupported() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CdapIO.<String, String>read().withCdapPluginClass(EmployeeBatchSource.class));
  }

  @Test
  public void testReadingData() {
    EmployeeBatchSourceConfig pluginConfig =
        new ConfigWrapper<>(EmployeeBatchSourceConfig.class)
            .withParams(TEST_EMPLOYEE_PARAMS_MAP)
            .build();
    CdapIO.Read<String, String> read =
        CdapIO.<String, String>read()
            .withCdapPlugin(
                Plugin.create(
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
    PCollection<KV<String, String>> actual = p.apply("ReadTest", read);
    PAssert.that(actual).containsInAnyOrder(expected);
    p.run();
  }
}
