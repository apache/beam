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
package org.apache.beam.sdk.tpcds;

import org.apache.beam.sdk.schemas.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class TpcdsSchemasTest {
    private Map<String, Schema> schemaMap;
    private Map<String, Schema> immutableSchemaMap;

    @Before
    public void initializeMaps() throws Exception {
        schemaMap = TpcdsSchemas.getTpcdsSchemas();
        immutableSchemaMap = TpcdsSchemas.getTpcdsSchemasImmutableMap();
    }

    @Test
    public void testCallCenterSchema() throws Exception {
        Schema callCenterSchema =
                Schema.builder()
                        .addField("cc_call_center_sk", Schema.FieldType.INT64)
                        .addField("cc_call_center_id", Schema.FieldType.STRING)
                        .addNullableField("cc_rec_start_date", Schema.FieldType.STRING)
                        .addNullableField("cc_rec_end_date", Schema.FieldType.STRING)
                        .addNullableField("cc_closed_date_sk", Schema.FieldType.INT64)
                        .addNullableField("cc_open_date_sk", Schema.FieldType.INT64)
                        .addNullableField("cc_name", Schema.FieldType.STRING)
                        .addNullableField("cc_class", Schema.FieldType.STRING)
                        .addNullableField("cc_employees", Schema.FieldType.INT64)
                        .addNullableField("cc_sq_ft", Schema.FieldType.INT64)
                        .addNullableField("cc_hours", Schema.FieldType.STRING)
                        .addNullableField("cc_manager", Schema.FieldType.STRING)
                        .addNullableField("cc_mkt_id", Schema.FieldType.INT64)
                        .addNullableField("cc_mkt_class", Schema.FieldType.STRING)
                        .addNullableField("cc_mkt_desc", Schema.FieldType.STRING)
                        .addNullableField("cc_market_manager", Schema.FieldType.STRING)
                        .addNullableField("cc_division", Schema.FieldType.INT64)
                        .addNullableField("cc_division_name", Schema.FieldType.STRING)
                        .addNullableField("cc_company", Schema.FieldType.INT64)
                        .addNullableField("cc_company_name", Schema.FieldType.STRING)
                        .addNullableField("cc_street_number", Schema.FieldType.STRING)
                        .addNullableField("cc_street_name", Schema.FieldType.STRING)
                        .addNullableField("cc_street_type", Schema.FieldType.STRING)
                        .addNullableField("cc_suite_number", Schema.FieldType.STRING)
                        .addNullableField("cc_city", Schema.FieldType.STRING)
                        .addNullableField("cc_county", Schema.FieldType.STRING)
                        .addNullableField("cc_state", Schema.FieldType.STRING)
                        .addNullableField("cc_zip", Schema.FieldType.STRING)
                        .addNullableField("cc_country", Schema.FieldType.STRING)
                        .addNullableField("cc_gmt_offset", Schema.FieldType.DOUBLE)
                        .addNullableField("cc_tax_percentage", Schema.FieldType.DOUBLE)
                        .build();

        assertNotEquals(schemaMap.get("call_center"), callCenterSchema);
        assertEquals(immutableSchemaMap.get("call_center"), callCenterSchema);
    }

    @Test
    public void testCatalogPageSchemaNullable() throws Exception {
        Schema catalogPageSchemaNullable =
                Schema.builder()
                        .addNullableField("cp_catalog_page_sk", Schema.FieldType.INT64)
                        .addNullableField("cp_catalog_page_id", Schema.FieldType.STRING)
                        .addNullableField("cp_start_date_sk", Schema.FieldType.INT64)
                        .addNullableField("cp_end_date_sk", Schema.FieldType.INT64)
                        .addNullableField("cp_department", Schema.FieldType.STRING)
                        .addNullableField("cp_catalog_number", Schema.FieldType.INT64)
                        .addNullableField("cp_catalog_page_number", Schema.FieldType.INT64)
                        .addNullableField("cp_description", Schema.FieldType.STRING)
                        .addNullableField("cp_type", Schema.FieldType.STRING)
                        .build();

        assertEquals(schemaMap.get("catalog_page"), catalogPageSchemaNullable);
        assertNotEquals(schemaMap.get("catalog_page"), TpcdsSchemas.getCatalogPageSchema());
        assertEquals(immutableSchemaMap.get("catalog_page"), TpcdsSchemas.getCatalogPageSchema());
    }

    @Test
    public void testCustomerAddressSchemaNullable() throws Exception {
        Schema customerAddressSchemaNullable =
                Schema.builder()
                        .addNullableField("ca_address_sk", Schema.FieldType.INT64)
                        .addNullableField("ca_address_id", Schema.FieldType.STRING)
                        .addNullableField("ca_street_number", Schema.FieldType.STRING)
                        .addNullableField("ca_street_name", Schema.FieldType.STRING)
                        .addNullableField("ca_street_type", Schema.FieldType.STRING)
                        .addNullableField("ca_suite_number", Schema.FieldType.STRING)
                        .addNullableField("ca_city", Schema.FieldType.STRING)
                        .addNullableField("ca_county", Schema.FieldType.STRING)
                        .addNullableField("ca_state", Schema.FieldType.STRING)
                        .addNullableField("ca_zip", Schema.FieldType.STRING)
                        .addNullableField("ca_country", Schema.FieldType.STRING)
                        .addNullableField("ca_gmt_offset", Schema.FieldType.DOUBLE)
                        .addNullableField("ca_location_type", Schema.FieldType.STRING)
                        .build();

        assertEquals(schemaMap.get("customer_address"), customerAddressSchemaNullable);
        assertNotEquals(schemaMap.get("customer_address"), TpcdsSchemas.getCustomerAddressSchema());
        assertEquals(immutableSchemaMap.get("customer_address"), TpcdsSchemas.getCustomerAddressSchema());
    }
}
