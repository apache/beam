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

import io.cdap.plugin.common.Constants;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.cdap.context.BatchSourceContextImpl;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link CdapIO}. */
@SuppressWarnings({"ModifiedButNotUsed", "UnusedVariable"})
@RunWith(JUnit4.class)
public class CdapIOTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final Logger LOG = LoggerFactory.getLogger(CdapIOTest.class);

  private static final String TEST_LOCKS_DIR_PATH = "src/test/resources";

  @Test
  public void testReadBuildsCorrectly() {

    Map<String, Object> params = new HashMap<>();
    params.put(Constants.Reference.REFERENCE_NAME, "referenceName");
    params.put(EmployeeBatchSourceConfig.OBJECT_TYPE, "employee");
    EmployeeBatchSourceConfig pluginConfig =
        new ConfigWrapper<>(EmployeeBatchSourceConfig.class).withParams(params).build();

    CdapIO.Read<String, String> read =
        CdapIO.<String, String>read()
            .withCdapPluginClass(EmployeeBatchSource.class)
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
    assertEquals(pluginConfig, cdapPlugin.getPluginConfig());
    assertEquals(pluginConfig, read.getPluginConfig());
    assertEquals(String.class, read.getKeyClass());
    assertEquals(String.class, read.getValueClass());
  }
}
