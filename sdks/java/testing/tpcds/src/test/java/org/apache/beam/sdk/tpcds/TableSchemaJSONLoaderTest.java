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

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class TableSchemaJSONLoaderTest {
    @Test
    public void testStoreReturnsTable() throws Exception {
        String storeReturnsSchemaString = TableSchemaJSONLoader.parseTableSchema("store_returns");
        String expected = "sr_returned_date_sk bigint,"
                + "sr_return_time_sk bigint,"
                + "sr_item_sk bigint,"
                + "sr_customer_sk bigint,"
                + "sr_cdemo_sk bigint,"
                + "sr_hdemo_sk bigint,"
                + "sr_addr_sk bigint,"
                + "sr_store_sk bigint,"
                + "sr_reason_sk bigint,"
                + "sr_ticket_number bigint,"
                + "sr_return_quantity bigint,"
                + "sr_return_amt double,"
                + "sr_return_tax double,"
                + "sr_return_amt_inc_tax double,"
                + "sr_fee double,"
                + "sr_return_ship_cost double,"
                + "sr_refunded_cash double,"
                + "sr_reversed_charge double,"
                + "sr_store_credit double,"
                + "sr_net_loss double";
        assertEquals(expected, storeReturnsSchemaString);
    }

    @Test
    public void testItemTable() throws Exception {
        String itemSchemaString = TableSchemaJSONLoader.parseTableSchema("item");
        String expected = "i_item_sk bigint,"
                + "i_item_id varchar,"
                + "i_rec_start_date varchar,"
                + "i_rec_end_date varchar,"
                + "i_item_desc varchar,"
                + "i_current_price double,"
                + "i_wholesale_cost double,"
                + "i_brand_id bigint,"
                + "i_brand varchar,"
                + "i_class_id bigint,"
                + "i_class varchar,"
                + "i_category_id bigint,"
                + "i_category varchar,"
                + "i_manufact_id bigint,"
                + "i_manufact varchar,"
                + "i_size varchar,"
                + "i_formulation varchar,"
                + "i_color varchar,"
                + "i_units varchar,"
                + "i_container varchar,"
                + "i_manager_id bigint,"
                + "i_product_name varchar";
        assertEquals(expected, itemSchemaString);
    }

    @Test
    public void testDateDimTable() throws Exception {
        String dateDimSchemaString = TableSchemaJSONLoader.parseTableSchema("date_dim");
        String expected = "d_date_sk bigint,"
                + "d_date_id varchar,"
                + "d_date varchar,"
                + "d_month_seq bigint,"
                + "d_week_seq bigint,"
                + "d_quarter_seq bigint,"
                + "d_year bigint,"
                + "d_dow bigint,"
                + "d_moy bigint,"
                + "d_dom bigint,"
                + "d_qoy bigint,"
                + "d_fy_year bigint,"
                + "d_fy_quarter_seq bigint,"
                + "d_fy_week_seq bigint,"
                + "d_day_name varchar,"
                + "d_quarter_name varchar,"
                + "d_holiday varchar,"
                + "d_weekend varchar,"
                + "d_following_holiday varchar,"
                + "d_first_dom bigint,"
                + "d_last_dom bigint,"
                + "d_same_day_ly bigint,"
                + "d_same_day_lq bigint,"
                + "d_current_day varchar,"
                + "d_current_week varchar,"
                + "d_current_month varchar,"
                + "d_current_quarter varchar,"
                + "d_current_year varchar";
        assertEquals(expected, dateDimSchemaString);
    }

    @Test
    public void testWarehouseTable() throws Exception {
        String warehouseSchemaString = TableSchemaJSONLoader.parseTableSchema("warehouse");
        String expected = "w_warehouse_sk bigint,"
                + "w_warehouse_id varchar,"
                + "w_warehouse_name varchar,"
                + "w_warehouse_sq_ft bigint,"
                + "w_street_number varchar,"
                + "w_street_name varchar,"
                + "w_street_type varchar,"
                + "w_suite_number varchar,"
                + "w_city varchar,"
                + "w_county varchar,"
                + "w_state varchar,"
                + "w_zip varchar,"
                + "w_country varchar,"
                + "w_gmt_offset double";
        assertEquals(expected, warehouseSchemaString);
    }

    @Test
    public void testGetAllTableNames() {
        List<String> tableNames = TableSchemaJSONLoader.getAllTableNames();
        Collections.sort(tableNames);
        List<String> expectedTableNames = Arrays.asList("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics",
                "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
                "warehouse", "web_page", "web_returns", "web_sales", "web_site");

        assertEquals(expectedTableNames.size(), tableNames.size());

        for (int i = 0; i < tableNames.size(); i++) {
            assertEquals(expectedTableNames.get(i), tableNames.get(i));
        }
    }
}
