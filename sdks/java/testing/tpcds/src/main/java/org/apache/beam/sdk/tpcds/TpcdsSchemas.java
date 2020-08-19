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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TpcdsSchemas {
    /**
     * Get all tpcds table schemas automatically by reading json files.
     * In this case all field will be nullable, this is a bit different from the tpcds specification, but doesn't affect query execution.
     *
     * @return A map of all tpcds table schemas with their table names as keys.
     * @throws Exception
     */
    public static Map<String, Schema> getTpcdsSchemas() throws Exception {
        Map<String, Schema> schemaMap = new HashMap<>();

        List<String> tableNames = TableSchemaJSONLoader.getAllTableNames();
        for (String tableName : tableNames) {
            Schema.Builder schemaBuilder = Schema.builder();

            String tableSchemaString = TableSchemaJSONLoader.parseTableSchema(tableName);
            String[] nameTypePairs = tableSchemaString.split(",");

            for (String nameTypePairString : nameTypePairs) {
                String[] nameTypePair = nameTypePairString.split("\\s+");
                String name = nameTypePair[0];
                String type = nameTypePair[1];

                Schema.FieldType fieldType;
                if (type.equals("bigint")) {
                    fieldType = Schema.FieldType.INT64;
                } else if (type.equals("double")) {
                    fieldType = Schema.FieldType.DOUBLE;
                } else {
                    fieldType = Schema.FieldType.STRING;
                }

                schemaBuilder.addNullableField(name, fieldType);
            }

            Schema tableSchema = schemaBuilder.build();
            schemaMap.put(tableName,tableSchema);
        }
        return schemaMap;
    }

    /**
     * Get all tpcds table schemas according to tpcds official specification. Some fields are set to be not nullable.
     *
     * @return A map of all tpcds table schemas with their table names as keys.
     */
    public static Map<String, Schema> getTpcdsSchemasImmutableMap() {
        ImmutableMap<String, Schema> immutableSchemaMap =
                ImmutableMap.<String, Schema> builder()
                        .put("call_center", callCenterSchema)
                        .put("catalog_page", catalogPageSchema)
                        .put("catalog_returns", catalogReturnsSchema)
                        .put("catalog_sales", catalogSalesSchema)
                        .put("customer", customerSchema)
                        .put("customer_address", customerAddressSchema)
                        .put("customer_demographics", customerDemographicsSchema)
                        .put("date_dim", dateDimSchema)
                        .put("household_demographics", householdDemographicsSchema)
                        .put("income_band", incomeBandSchema)
                        .put("inventory", inventorySchema)
                        .put("item", itemSchema)
                        .put("promotion", promotionSchema)
                        .put("reason", reasonSchema)
                        .put("ship_mode", shipModeSchema)
                        .put("store", storeSchema)
                        .put("store_returns", storeReturnsSchema)
                        .put("store_sales", storeSalesSchema)
                        .put("time_dim", timeDimSchema)
                        .put("warehouse", warehouseSchema)
                        .put("web_page", webPageSchema)
                        .put("web_returns", webReturnsSchema)
                        .put("web_sales", webSalesSchema)
                        .put("web_site", webSiteSchema)
                        .build();
        return immutableSchemaMap;
    }

    public static Schema getCallCenterSchema() { return callCenterSchema; }

    public static Schema getCatalogPageSchema() { return catalogPageSchema; }

    public static Schema getCatalogReturnsSchema() { return catalogReturnsSchema; }

    public static Schema getCatalogSalesSchema() { return catalogSalesSchema; }

    public static Schema getCustomerSchema() { return customerSchema; }

    public static Schema getCustomerAddressSchema() { return customerAddressSchema; }

    public static Schema getCustomerDemographicsSchema() { return customerDemographicsSchema; }

    public static Schema getDateDimSchema() { return dateDimSchema; }

    public static Schema getHouseholdDemographicsSchema() { return householdDemographicsSchema; }

    public static Schema getIncomeBandSchema() { return incomeBandSchema; }

    public static Schema getInventorySchema() { return inventorySchema; }

    public static Schema getItemSchema() { return itemSchema; }

    public static Schema getPromotionSchema() { return promotionSchema; }

    public static Schema getReasonSchema() { return reasonSchema; }

    public static Schema getShipModeSchema() { return shipModeSchema; }

    public static Schema getStoreSchema() { return storeSchema; }

    public static Schema getStoreReturnsSchema() { return storeReturnsSchema; }

    public static Schema getStoreSalesSchema() { return storeSalesSchema; }

    public static Schema getTimeDimSchema() { return timeDimSchema; }

    public static Schema getWarehouseSchema() { return warehouseSchema; }

    public static Schema getWebpageSchema() { return webPageSchema; }

    public static Schema getWebReturnsSchema() { return webReturnsSchema; }

    public static Schema getWebSalesSchema() { return webSalesSchema; }

    public static Schema getWebSiteSchema() { return webSiteSchema; }

    private static Schema callCenterSchema =
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

    private static Schema catalogPageSchema =
            Schema.builder()
                    .addField("cp_catalog_page_sk", Schema.FieldType.INT64)
                    .addField("cp_catalog_page_id", Schema.FieldType.STRING)
                    .addNullableField("cp_start_date_sk", Schema.FieldType.INT64)
                    .addNullableField("cp_end_date_sk", Schema.FieldType.INT64)
                    .addNullableField("cp_department", Schema.FieldType.STRING)
                    .addNullableField("cp_catalog_number", Schema.FieldType.INT64)
                    .addNullableField("cp_catalog_page_number", Schema.FieldType.INT64)
                    .addNullableField("cp_description", Schema.FieldType.STRING)
                    .addNullableField("cp_type", Schema.FieldType.STRING)
                    .build();

    private static Schema catalogReturnsSchema =
            Schema.builder()
                    .addNullableField("cr_returned_date_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_returned_time_sk", Schema.FieldType.INT64)
                    .addField("cr_item_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_refunded_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_refunded_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_refunded_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_refunded_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_returning_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_returning_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_returning_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_returning_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_call_center_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_catalog_page_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_ship_mode_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_warehouse_sk", Schema.FieldType.INT64)
                    .addNullableField("cr_reason_sk", Schema.FieldType.INT64)
                    .addField("cr_order_number", Schema.FieldType.INT64)
                    .addNullableField("cr_return_quantity", Schema.FieldType.INT64)
                    .addNullableField("cr_return_amount", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_return_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_return_amt_inc_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_fee", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_return_ship_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_refunded_cash", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_reversed_charge", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_store_credit", Schema.FieldType.DOUBLE)
                    .addNullableField("cr_net_loss", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema catalogSalesSchema =
            Schema.builder()
                    .addNullableField("cs_sold_date_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_sold_time_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_ship_date_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_bill_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_bill_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_bill_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_bill_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_ship_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_ship_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_ship_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_ship_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_call_center_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_catalog_page_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_ship_mode_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_warehouse_sk", Schema.FieldType.INT64)
                    .addField("cs_item_sk", Schema.FieldType.INT64)
                    .addNullableField("cs_promo_sk", Schema.FieldType.INT64)
                    .addField("cs_order_number", Schema.FieldType.INT64)
                    .addNullableField("cs_quantity", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_list_price", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_sales_price", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_ext_discount_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_ext_sales_price", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_ext_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_ext_list_price", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_ext_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_coupon_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_ext_ship_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_net_paid", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_net_paid_inc_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_net_paid_inc_ship", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_net_paid_inc_ship_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("cs_net_profit", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema customerSchema =
            Schema.builder()
                    .addField("c_customer_sk", Schema.FieldType.INT64)
                    .addField("c_customer_id", Schema.FieldType.STRING)
                    .addNullableField("c_current_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("c_current_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("c_current_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("c_first_shipto_date_sk", Schema.FieldType.INT64)
                    .addNullableField("c_first_sales_date_sk", Schema.FieldType.INT64)
                    .addNullableField("c_salutation", Schema.FieldType.STRING)
                    .addNullableField("c_first_name", Schema.FieldType.STRING)
                    .addNullableField("c_last_name", Schema.FieldType.STRING)
                    .addNullableField("c_preferred_cust_flag", Schema.FieldType.STRING)
                    .addNullableField("c_birth_day", Schema.FieldType.INT64)
                    .addNullableField("c_birth_month", Schema.FieldType.INT64)
                    .addNullableField("c_birth_year", Schema.FieldType.INT64)
                    .addNullableField("c_birth_country", Schema.FieldType.STRING)
                    .addNullableField("c_login", Schema.FieldType.STRING)
                    .addNullableField("c_email_address", Schema.FieldType.STRING)
                    .addNullableField("c_last_review_date_sk", Schema.FieldType.INT64)
                    .build();

    private static Schema customerAddressSchema =
            Schema.builder()
                    .addField("ca_address_sk", Schema.FieldType.INT64)
                    .addField("ca_address_id", Schema.FieldType.STRING)
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

    private static Schema customerDemographicsSchema =
            Schema.builder()
                    .addField("cd_demo_sk", Schema.FieldType.INT64)
                    .addNullableField("cd_gender", Schema.FieldType.STRING)
                    .addNullableField("cd_marital_status", Schema.FieldType.STRING)
                    .addNullableField("cd_education_status", Schema.FieldType.STRING)
                    .addNullableField("cd_purchase_estimate", Schema.FieldType.INT64)
                    .addNullableField("cd_credit_rating", Schema.FieldType.STRING)
                    .addNullableField("cd_dep_count", Schema.FieldType.INT64)
                    .addNullableField("cd_dep_employed_count", Schema.FieldType.INT64)
                    .addNullableField("cd_dep_college_count", Schema.FieldType.INT64)
                    .build();

    private static Schema dateDimSchema =
            Schema.builder()
                    .addField("d_date_sk", Schema.FieldType.INT64)
                    .addField("d_date_id", Schema.FieldType.STRING)
                    .addNullableField("d_date", Schema.FieldType.STRING)
                    .addNullableField("d_month_seq", Schema.FieldType.INT64)
                    .addNullableField("d_week_seq", Schema.FieldType.INT64)
                    .addNullableField("d_quarter_seq", Schema.FieldType.INT64)
                    .addNullableField("d_year", Schema.FieldType.INT64)
                    .addNullableField("d_dow", Schema.FieldType.INT64)
                    .addNullableField("d_moy", Schema.FieldType.INT64)
                    .addNullableField("d_dom", Schema.FieldType.INT64)
                    .addNullableField("d_qoy", Schema.FieldType.INT64)
                    .addNullableField("d_fy_year", Schema.FieldType.INT64)
                    .addNullableField("d_fy_quarter_seq", Schema.FieldType.INT64)
                    .addNullableField("d_fy_week_seq", Schema.FieldType.INT64)
                    .addNullableField("d_day_name", Schema.FieldType.STRING)
                    .addNullableField("d_quarter_name", Schema.FieldType.STRING)
                    .addNullableField("d_holiday", Schema.FieldType.STRING)
                    .addNullableField("d_weekend", Schema.FieldType.STRING)
                    .addNullableField("d_following_holiday", Schema.FieldType.STRING)
                    .addNullableField("d_first_dom", Schema.FieldType.INT64)
                    .addNullableField("d_last_dom", Schema.FieldType.INT64)
                    .addNullableField("d_same_day_ly", Schema.FieldType.INT64)
                    .addNullableField("d_same_day_lq", Schema.FieldType.INT64)
                    .addNullableField("d_current_day", Schema.FieldType.STRING)
                    .addNullableField("d_current_week", Schema.FieldType.STRING)
                    .addNullableField("d_current_month", Schema.FieldType.STRING)
                    .addNullableField("d_current_quarter", Schema.FieldType.STRING)
                    .addNullableField("d_current_year", Schema.FieldType.STRING)
                    .build();

    private static Schema householdDemographicsSchema =
            Schema.builder()
                    .addField("hd_demo_sk", Schema.FieldType.INT64)
                    .addNullableField("hd_income_band_sk", Schema.FieldType.INT64)
                    .addNullableField("hd_buy_potential", Schema.FieldType.STRING)
                    .addNullableField("hd_dep_count", Schema.FieldType.INT64)
                    .addNullableField("hd_vehicle_count", Schema.FieldType.INT64)
                    .build();

    private static Schema incomeBandSchema =
            Schema.builder()
                    .addField("ib_income_band_sk", Schema.FieldType.INT64)
                    .addNullableField("ib_lower_bound", Schema.FieldType.INT64)
                    .addNullableField("ib_upper_bound", Schema.FieldType.INT64)
                    .build();

    private static Schema inventorySchema =
            Schema.builder()
                    .addField("inv_date_sk", Schema.FieldType.INT32)
                    .addField("inv_item_sk", Schema.FieldType.INT32)
                    .addField("inv_warehouse_sk", Schema.FieldType.INT32)
                    .addNullableField("inv_quantity_on_hand", Schema.FieldType.INT32)
                    .build();

    private static Schema itemSchema =
            Schema.builder()
                    .addField("i_item_sk", Schema.FieldType.INT64)
                    .addField("i_item_id", Schema.FieldType.STRING)
                    .addNullableField("i_rec_start_date", Schema.FieldType.STRING)
                    .addNullableField("i_rec_end_date", Schema.FieldType.STRING)
                    .addNullableField("i_item_desc", Schema.FieldType.STRING)
                    .addNullableField("i_current_price", Schema.FieldType.DOUBLE)
                    .addNullableField("i_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("i_brand_id", Schema.FieldType.INT64)
                    .addNullableField("i_brand", Schema.FieldType.STRING)
                    .addNullableField("i_class_id", Schema.FieldType.INT64)
                    .addNullableField("i_class", Schema.FieldType.STRING)
                    .addNullableField("i_category_id", Schema.FieldType.INT64)
                    .addNullableField("i_category", Schema.FieldType.STRING)
                    .addNullableField("i_manufact_id", Schema.FieldType.INT64)
                    .addNullableField("i_manufact", Schema.FieldType.STRING)
                    .addNullableField("i_size", Schema.FieldType.STRING)
                    .addNullableField("i_formulation", Schema.FieldType.STRING)
                    .addNullableField("i_color", Schema.FieldType.STRING)
                    .addNullableField("i_units", Schema.FieldType.STRING)
                    .addNullableField("i_container", Schema.FieldType.STRING)
                    .addNullableField("i_manager_id", Schema.FieldType.INT64)
                    .addNullableField("i_product_name", Schema.FieldType.STRING)
                    .build();

    private static Schema promotionSchema =
            Schema.builder()
                    .addField("p_promo_sk", Schema.FieldType.INT64)
                    .addField("p_promo_id", Schema.FieldType.STRING)
                    .addNullableField("p_start_date_sk", Schema.FieldType.INT64)
                    .addNullableField("p_end_date_sk", Schema.FieldType.INT64)
                    .addNullableField("p_item_sk", Schema.FieldType.INT64)
                    .addNullableField("p_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("p_response_target", Schema.FieldType.INT64)
                    .addNullableField("p_promo_name", Schema.FieldType.STRING)
                    .addNullableField("p_channel_dmail", Schema.FieldType.STRING)
                    .addNullableField("p_channel_email", Schema.FieldType.STRING)
                    .addNullableField("p_channel_catalog", Schema.FieldType.STRING)
                    .addNullableField("p_channel_tv", Schema.FieldType.STRING)
                    .addNullableField("p_channel_radio", Schema.FieldType.STRING)
                    .addNullableField("p_channel_press", Schema.FieldType.STRING)
                    .addNullableField("p_channel_event", Schema.FieldType.STRING)
                    .addNullableField("p_channel_demo", Schema.FieldType.STRING)
                    .addNullableField("p_channel_details", Schema.FieldType.STRING)
                    .addNullableField("p_purpose", Schema.FieldType.STRING)
                    .addNullableField("p_discount_active", Schema.FieldType.STRING)
                    .build();

    private static Schema reasonSchema =
            Schema.builder()
                    .addField("r_reason_sk", Schema.FieldType.INT64)
                    .addField("r_reason_id", Schema.FieldType.STRING)
                    .addNullableField("r_reason_desc", Schema.FieldType.STRING)
                    .build();

    private static Schema shipModeSchema =
            Schema.builder()
                    .addField("sm_ship_mode_sk", Schema.FieldType.INT64)
                    .addField("sm_ship_mode_id", Schema.FieldType.STRING)
                    .addNullableField("sm_type", Schema.FieldType.STRING)
                    .addNullableField("sm_code", Schema.FieldType.STRING)
                    .addNullableField("sm_carrier", Schema.FieldType.STRING)
                    .addNullableField("sm_contract", Schema.FieldType.STRING)
                    .build();

    private static Schema storeSchema =
            Schema.builder()
                    .addField("s_store_sk", Schema.FieldType.INT64)
                    .addField("s_store_id", Schema.FieldType.STRING)
                    .addNullableField("s_rec_start_date", Schema.FieldType.STRING)
                    .addNullableField("s_rec_end_date", Schema.FieldType.STRING)
                    .addNullableField("s_closed_date_sk", Schema.FieldType.INT64)
                    .addNullableField("s_store_name", Schema.FieldType.STRING)
                    .addNullableField("s_number_employees", Schema.FieldType.INT64)
                    .addNullableField("s_floor_space", Schema.FieldType.INT64)
                    .addNullableField("s_hours", Schema.FieldType.STRING)
                    .addNullableField("S_manager", Schema.FieldType.STRING)
                    .addNullableField("S_market_id", Schema.FieldType.INT64)
                    .addNullableField("S_geography_class", Schema.FieldType.STRING)
                    .addNullableField("S_market_desc", Schema.FieldType.STRING)
                    .addNullableField("s_market_manager", Schema.FieldType.STRING)
                    .addNullableField("s_division_id", Schema.FieldType.INT64)
                    .addNullableField("s_division_name", Schema.FieldType.STRING)
                    .addNullableField("s_company_id", Schema.FieldType.INT64)
                    .addNullableField("s_company_name", Schema.FieldType.STRING)
                    .addNullableField("s_street_number", Schema.FieldType.STRING)
                    .addNullableField("s_street_name", Schema.FieldType.STRING)
                    .addNullableField("s_street_type", Schema.FieldType.STRING)
                    .addNullableField("s_suite_number", Schema.FieldType.STRING)
                    .addNullableField("s_city", Schema.FieldType.STRING)
                    .addNullableField("s_county", Schema.FieldType.STRING)
                    .addNullableField("s_state", Schema.FieldType.STRING)
                    .addNullableField("s_zip", Schema.FieldType.STRING)
                    .addNullableField("s_country", Schema.FieldType.STRING)
                    .addNullableField("s_gmt_offset", Schema.FieldType.DOUBLE)
                    .addNullableField("s_tax_percentage", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema storeReturnsSchema =
            Schema.builder()
                    .addNullableField("sr_returned_date_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_return_time_sk", Schema.FieldType.INT64)
                    .addField("sr_item_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_store_sk", Schema.FieldType.INT64)
                    .addNullableField("sr_reason_sk", Schema.FieldType.INT64)
                    .addField("sr_ticket_number", Schema.FieldType.INT64)
                    .addNullableField("sr_return_quantity", Schema.FieldType.INT64)
                    .addNullableField("sr_return_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_return_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_return_amt_inc_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_fee", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_return_ship_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_refunded_cash", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_reversed_charge", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_store_credit", Schema.FieldType.DOUBLE)
                    .addNullableField("sr_net_loss", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema storeSalesSchema =
            Schema.builder()
                    .addNullableField("ss_sold_date_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_sold_time_sk", Schema.FieldType.INT64)
                    .addField("ss_item_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_store_sk", Schema.FieldType.INT64)
                    .addNullableField("ss_promo_sk", Schema.FieldType.INT64)
                    .addField("ss_ticket_number", Schema.FieldType.INT64)
                    .addNullableField("ss_quantity", Schema.FieldType.INT64)
                    .addNullableField("ss_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_list_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_sales_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_ext_discount_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_ext_sales_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_ext_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_ext_list_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_ext_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_coupon_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_net_paid", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_net_paid_inc_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("ss_net_profit", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema timeDimSchema =
            Schema.builder()
                    .addField("t_time_sk", Schema.FieldType.INT64)
                    .addField("t_time_id", Schema.FieldType.STRING)
                    .addNullableField("t_time", Schema.FieldType.INT64)
                    .addNullableField("t_hour", Schema.FieldType.INT64)
                    .addNullableField("t_minute", Schema.FieldType.INT64)
                    .addNullableField("t_second", Schema.FieldType.INT64)
                    .addNullableField("t_am_pm", Schema.FieldType.STRING)
                    .addNullableField("t_shift", Schema.FieldType.STRING)
                    .addNullableField("t_sub_shift", Schema.FieldType.STRING)
                    .addNullableField("t_meal_time", Schema.FieldType.STRING)
                    .build();

    private static Schema warehouseSchema =
            Schema.builder()
                    .addField("w_warehouse_sk", Schema.FieldType.INT64)
                    .addField("w_warehouse_id", Schema.FieldType.STRING)
                    .addNullableField("w_warehouse_name", Schema.FieldType.STRING)
                    .addNullableField("w_warehouse_sq_ft", Schema.FieldType.INT64)
                    .addNullableField("w_street_number", Schema.FieldType.STRING)
                    .addNullableField("w_street_name", Schema.FieldType.STRING)
                    .addNullableField("w_street_type", Schema.FieldType.STRING)
                    .addNullableField("w_suite_number", Schema.FieldType.STRING)
                    .addNullableField("w_city", Schema.FieldType.STRING)
                    .addNullableField("w_county", Schema.FieldType.STRING)
                    .addNullableField("w_state", Schema.FieldType.STRING)
                    .addNullableField("w_zip", Schema.FieldType.STRING)
                    .addNullableField("w_country", Schema.FieldType.STRING)
                    .addNullableField("w_gmt_offset", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema webPageSchema =
            Schema.builder()
                    .addField("wp_web_page_sk", Schema.FieldType.INT64)
                    .addField("wp_web_page_id", Schema.FieldType.STRING)
                    .addNullableField("wp_rec_start_date", Schema.FieldType.STRING)
                    .addNullableField("wp_rec_end_date", Schema.FieldType.STRING)
                    .addNullableField("wp_creation_date_sk", Schema.FieldType.INT64)
                    .addNullableField("wp_access_date_sk", Schema.FieldType.INT64)
                    .addNullableField("wp_autogen_flag", Schema.FieldType.STRING)
                    .addNullableField("wp_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("wp_url", Schema.FieldType.STRING)
                    .addNullableField("wp_type", Schema.FieldType.STRING)
                    .addNullableField("wp_char_count", Schema.FieldType.INT64)
                    .addNullableField("wp_link_count", Schema.FieldType.INT64)
                    .addNullableField("wp_image_count", Schema.FieldType.INT64)
                    .addNullableField("wp_max_ad_count", Schema.FieldType.INT64)
                    .build();

    private static Schema webReturnsSchema =
            Schema.builder()
                    .addNullableField("wr_returned_date_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_returned_time_sk", Schema.FieldType.INT64)
                    .addField("wr_item_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_refunded_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_refunded_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_refunded_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_refunded_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_returning_customer_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_returning_cdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_returning_hdemo_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_returning_addr_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_web_page_sk", Schema.FieldType.INT64)
                    .addNullableField("wr_reason_sk", Schema.FieldType.INT64)
                    .addField("wr_order_number", Schema.FieldType.INT64)
                    .addNullableField("wr_return_quantity", Schema.FieldType.INT64)
                    .addNullableField("wr_return_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_return_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_return_amt_inc_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_fee", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_return_ship_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_refunded_cash", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_reversed_charge", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_account_credit", Schema.FieldType.DOUBLE)
                    .addNullableField("wr_net_loss", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema webSalesSchema =
            Schema.builder()
                    .addNullableField("ws_sold_date_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_sold_time_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_ship_date_sk", Schema.FieldType.INT32)
                    .addField("ws_item_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_bill_customer_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_bill_cdemo_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_bill_hdemo_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_bill_addr_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_ship_customer_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_ship_cdemo_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_ship_hdemo_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_ship_addr_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_web_page_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_web_site_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_ship_mode_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_warehouse_sk", Schema.FieldType.INT32)
                    .addNullableField("ws_promo_sk", Schema.FieldType.INT32)
                    .addField("ws_order_number", Schema.FieldType.INT64)
                    .addNullableField("ws_quantity", Schema.FieldType.INT32)
                    .addNullableField("ws_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_list_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_sales_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_ext_discount_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_ext_sales_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_ext_wholesale_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_ext_list_price", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_ext_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_coupon_amt", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_ext_ship_cost", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_net_paid", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_net_paid_inc_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_net_paid_inc_ship", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_net_paid_inc_ship_tax", Schema.FieldType.DOUBLE)
                    .addNullableField("ws_net_profit", Schema.FieldType.DOUBLE)
                    .build();

    private static Schema webSiteSchema =
            Schema.builder()
                    .addField("web_site_sk", Schema.FieldType.STRING)
                    .addField("web_site_id", Schema.FieldType.STRING)
                    .addNullableField("web_rec_start_date", Schema.FieldType.STRING)
                    .addNullableField("web_rec_end_date", Schema.FieldType.STRING)
                    .addNullableField("web_name", Schema.FieldType.STRING)
                    .addNullableField("web_open_date_sk", Schema.FieldType.INT32)
                    .addNullableField("web_close_date_sk", Schema.FieldType.INT32)
                    .addNullableField("web_class", Schema.FieldType.STRING)
                    .addNullableField("web_manager", Schema.FieldType.STRING)
                    .addNullableField("web_mkt_id", Schema.FieldType.INT32)
                    .addNullableField("web_mkt_class", Schema.FieldType.STRING)
                    .addNullableField("web_mkt_desc", Schema.FieldType.STRING)
                    .addNullableField("web_market_manager", Schema.FieldType.STRING)
                    .addNullableField("web_company_id", Schema.FieldType.INT32)
                    .addNullableField("web_company_name", Schema.FieldType.STRING)
                    .addNullableField("web_street_number", Schema.FieldType.STRING)
                    .addNullableField("web_street_name", Schema.FieldType.STRING)
                    .addNullableField("web_street_type", Schema.FieldType.STRING)
                    .addNullableField("web_suite_number", Schema.FieldType.STRING)
                    .addNullableField("web_city", Schema.FieldType.STRING)
                    .addNullableField("web_county", Schema.FieldType.STRING)
                    .addNullableField("web_state", Schema.FieldType.STRING)
                    .addNullableField("web_zip", Schema.FieldType.STRING)
                    .addNullableField("web_country", Schema.FieldType.STRING)
                    .addNullableField("web_gmt_offset", Schema.FieldType.DOUBLE)
                    .addNullableField("web_tax_percentage", Schema.FieldType.DOUBLE)
                    .build();
}
