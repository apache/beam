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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasLabel;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link BigtableConfig}. */
@RunWith(JUnit4.class)
public class BigtableConfigTest {

  static final ValueProvider<String> NOT_ACCESSIBLE_VALUE =
      new ValueProvider<String>() {
        @Override
        public String get() {
          throw new IllegalStateException("Value is not accessible");
        }

        @Override
        public boolean isAccessible() {
          return false;
        }
      };

  static final ValueProvider<String> PROJECT_ID =
      ValueProvider.StaticValueProvider.of("project_id");

  static final ValueProvider<String> INSTANCE_ID =
      ValueProvider.StaticValueProvider.of("instance_id");

  static final ValueProvider<String> TABLE_ID = ValueProvider.StaticValueProvider.of("table");

  static final SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> CONFIGURATOR =
      (SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>) input -> input;

  static final BigtableService SERVICE = Mockito.mock(BigtableService.class);

  @Rule public ExpectedException thrown = ExpectedException.none();

  private BigtableConfig config;

  @Before
  public void setup() throws Exception {
    config = BigtableConfig.builder().setValidate(false).build();
  }

  @Test
  public void testWithProjectId() {
    assertEquals(PROJECT_ID.get(), config.withProjectId(PROJECT_ID).getProjectId().get());

    thrown.expect(IllegalArgumentException.class);
    config.withProjectId(null);
  }

  @Test
  public void testWithInstanceId() {
    assertEquals(INSTANCE_ID.get(), config.withInstanceId(INSTANCE_ID).getInstanceId().get());

    thrown.expect(IllegalArgumentException.class);
    config.withInstanceId(null);
  }

  @Test
  public void testWithTableId() {
    assertEquals(TABLE_ID.get(), config.withTableId(TABLE_ID).getTableId().get());

    thrown.expect(IllegalArgumentException.class);
    config.withTableId(null);
  }

  @Test
  public void testWithBigtableOptionsConfigurator() {
    assertEquals(
        CONFIGURATOR,
        config.withBigtableOptionsConfigurator(CONFIGURATOR).getBigtableOptionsConfigurator());

    thrown.expect(IllegalArgumentException.class);
    config.withBigtableOptionsConfigurator(null);
  }

  @Test
  public void testWithValidate() {
    assertTrue(config.withValidate(true).getValidate());
  }

  @Test
  public void testWithBigtableService() {
    assertEquals(SERVICE, config.withBigtableService(SERVICE).getBigtableService());

    thrown.expect(IllegalArgumentException.class);
    config.withBigtableService(null);
  }

  @Test
  public void testValidate() {
    config.withProjectId(PROJECT_ID).withInstanceId(INSTANCE_ID).withTableId(TABLE_ID).validate();
  }

  @Test
  public void testValidateFailsWithoutProjectId() {
    config.withInstanceId(INSTANCE_ID).withTableId(TABLE_ID);

    thrown.expect(IllegalArgumentException.class);
    config.validate();
  }

  @Test
  public void testValidateFailsWithoutInstanceId() {
    config.withProjectId(PROJECT_ID).withTableId(TABLE_ID);

    thrown.expect(IllegalArgumentException.class);
    config.validate();
  }

  @Test
  public void testValidateFailsWithoutTableId() {
    config.withProjectId(PROJECT_ID).withInstanceId(INSTANCE_ID);

    thrown.expect(IllegalArgumentException.class);
    config.validate();
  }

  @Test
  public void testPopulateDisplayData() {
    DisplayData displayData =
        DisplayData.from(
            config.withProjectId(PROJECT_ID).withInstanceId(INSTANCE_ID).withTableId(TABLE_ID)
                ::populateDisplayData);

    assertThat(
        displayData,
        hasDisplayItem(
            allOf(
                hasKey("projectId"), hasLabel("Bigtable Project Id"), hasValue(PROJECT_ID.get()))));

    assertThat(
        displayData,
        hasDisplayItem(
            allOf(
                hasKey("instanceId"),
                hasLabel("Bigtable Instance Id"),
                hasValue(INSTANCE_ID.get()))));

    assertThat(
        displayData,
        hasDisplayItem(
            allOf(hasKey("tableId"), hasLabel("Bigtable Table Id"), hasValue(TABLE_ID.get()))));
  }

  @Test
  public void testGetBigtableServiceWithDefaultService() {
    assertEquals(SERVICE, config.withBigtableService(SERVICE).getBigtableService());
  }

  @Test
  public void testGetBigtableServiceWithConfigurator() {
    SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> configurator =
        (SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>)
            input ->
                input
                    .setInstanceId(INSTANCE_ID.get() + INSTANCE_ID.get())
                    .setProjectId(PROJECT_ID.get() + PROJECT_ID.get())
                    .setBulkOptions(new BulkOptions.Builder().setUseBulkApi(true).build());

    BigtableService service =
        config
            .withProjectId(PROJECT_ID)
            .withInstanceId(INSTANCE_ID)
            .withBigtableOptionsConfigurator(configurator)
            .getBigtableService(PipelineOptionsFactory.as(GcpOptions.class));

    assertEquals(PROJECT_ID.get(), service.getBigtableOptions().getProjectId());
    assertEquals(INSTANCE_ID.get(), service.getBigtableOptions().getInstanceId());
    assertTrue(service.getBigtableOptions().getBulkOptions().useBulkApi());
  }

  @Test
  public void testIsDataAccessible() {
    assertTrue(
        config
            .withTableId(TABLE_ID)
            .withProjectId(PROJECT_ID)
            .withInstanceId(INSTANCE_ID)
            .isDataAccessible());
    assertTrue(
        config
            .withTableId(TABLE_ID)
            .withProjectId(PROJECT_ID)
            .withBigtableOptions(new BigtableOptions.Builder().setInstanceId("instance_id").build())
            .isDataAccessible());
    assertTrue(
        config
            .withTableId(TABLE_ID)
            .withInstanceId(INSTANCE_ID)
            .withBigtableOptions(new BigtableOptions.Builder().setProjectId("project_id").build())
            .isDataAccessible());
    assertTrue(
        config
            .withTableId(TABLE_ID)
            .withBigtableOptions(
                new BigtableOptions.Builder()
                    .setProjectId("project_id")
                    .setInstanceId("instance_id")
                    .build())
            .isDataAccessible());

    assertFalse(
        config
            .withTableId(NOT_ACCESSIBLE_VALUE)
            .withProjectId(PROJECT_ID)
            .withInstanceId(INSTANCE_ID)
            .isDataAccessible());
    assertFalse(
        config
            .withTableId(TABLE_ID)
            .withProjectId(NOT_ACCESSIBLE_VALUE)
            .withInstanceId(INSTANCE_ID)
            .isDataAccessible());
    assertFalse(
        config
            .withTableId(TABLE_ID)
            .withProjectId(PROJECT_ID)
            .withInstanceId(NOT_ACCESSIBLE_VALUE)
            .isDataAccessible());
  }
}
