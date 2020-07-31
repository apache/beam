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

public class QueryReaderTest {
    @Test
    public void testQuery3String() throws Exception {
        String query3String = QueryReader.readQuery("query3");
        String expected = "select  dt.d_year \n" +
                "       ,item.i_brand_id brand_id \n" +
                "       ,item.i_brand brand\n" +
                "       ,sum(ss_ext_sales_price) sum_agg\n" +
                " from  date_dim dt \n" +
                "      ,store_sales\n" +
                "      ,item\n" +
                " where dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
                "   and store_sales.ss_item_sk = item.i_item_sk\n" +
                "   and item.i_manufact_id = 436\n" +
                "   and dt.d_moy=12\n" +
                " group by dt.d_year\n" +
                "      ,item.i_brand\n" +
                "      ,item.i_brand_id\n" +
                " order by dt.d_year\n" +
                "         ,sum_agg desc\n" +
                "         ,brand_id\n" +
                " limit 100";
        assertEquals(expected, query3String);
    }

    @Test
    public void testQuery4String() throws Exception {
        String query4String = QueryReader.readQuery("query4");
        String expected = "with year_total as (\n" +
                " select c_customer_id customer_id\n" +
                "       ,c_first_name customer_first_name\n" +
                "       ,c_last_name customer_last_name\n" +
                "       ,c_preferred_cust_flag customer_preferred_cust_flag\n" +
                "       ,c_birth_country customer_birth_country\n" +
                "       ,c_login customer_login\n" +
                "       ,c_email_address customer_email_address\n" +
                "       ,d_year dyear\n" +
                "       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total\n" +
                "       ,'s' sale_type\n" +
                " from customer\n" +
                "     ,store_sales\n" +
                "     ,date_dim\n" +
                " where c_customer_sk = ss_customer_sk\n" +
                "   and ss_sold_date_sk = d_date_sk\n" +
                " group by c_customer_id\n" +
                "         ,c_first_name\n" +
                "         ,c_last_name\n" +
                "         ,c_preferred_cust_flag\n" +
                "         ,c_birth_country\n" +
                "         ,c_login\n" +
                "         ,c_email_address\n" +
                "         ,d_year\n" +
                " union all\n" +
                " select c_customer_id customer_id\n" +
                "       ,c_first_name customer_first_name\n" +
                "       ,c_last_name customer_last_name\n" +
                "       ,c_preferred_cust_flag customer_preferred_cust_flag\n" +
                "       ,c_birth_country customer_birth_country\n" +
                "       ,c_login customer_login\n" +
                "       ,c_email_address customer_email_address\n" +
                "       ,d_year dyear\n" +
                "       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total\n" +
                "       ,'c' sale_type\n" +
                " from customer\n" +
                "     ,catalog_sales\n" +
                "     ,date_dim\n" +
                " where c_customer_sk = cs_bill_customer_sk\n" +
                "   and cs_sold_date_sk = d_date_sk\n" +
                " group by c_customer_id\n" +
                "         ,c_first_name\n" +
                "         ,c_last_name\n" +
                "         ,c_preferred_cust_flag\n" +
                "         ,c_birth_country\n" +
                "         ,c_login\n" +
                "         ,c_email_address\n" +
                "         ,d_year\n" +
                "union all\n" +
                " select c_customer_id customer_id\n" +
                "       ,c_first_name customer_first_name\n" +
                "       ,c_last_name customer_last_name\n" +
                "       ,c_preferred_cust_flag customer_preferred_cust_flag\n" +
                "       ,c_birth_country customer_birth_country\n" +
                "       ,c_login customer_login\n" +
                "       ,c_email_address customer_email_address\n" +
                "       ,d_year dyear\n" +
                "       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total\n" +
                "       ,'w' sale_type\n" +
                " from customer\n" +
                "     ,web_sales\n" +
                "     ,date_dim\n" +
                " where c_customer_sk = ws_bill_customer_sk\n" +
                "   and ws_sold_date_sk = d_date_sk\n" +
                " group by c_customer_id\n" +
                "         ,c_first_name\n" +
                "         ,c_last_name\n" +
                "         ,c_preferred_cust_flag\n" +
                "         ,c_birth_country\n" +
                "         ,c_login\n" +
                "         ,c_email_address\n" +
                "         ,d_year\n" +
                "         )\n" +
                "  select  \n" +
                "                  t_s_secyear.customer_id\n" +
                "                 ,t_s_secyear.customer_first_name\n" +
                "                 ,t_s_secyear.customer_last_name\n" +
                "                 ,t_s_secyear.customer_email_address\n" +
                " from year_total t_s_firstyear\n" +
                "     ,year_total t_s_secyear\n" +
                "     ,year_total t_c_firstyear\n" +
                "     ,year_total t_c_secyear\n" +
                "     ,year_total t_w_firstyear\n" +
                "     ,year_total t_w_secyear\n" +
                " where t_s_secyear.customer_id = t_s_firstyear.customer_id\n" +
                "   and t_s_firstyear.customer_id = t_c_secyear.customer_id\n" +
                "   and t_s_firstyear.customer_id = t_c_firstyear.customer_id\n" +
                "   and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n" +
                "   and t_s_firstyear.customer_id = t_w_secyear.customer_id\n" +
                "   and t_s_firstyear.sale_type = 's'\n" +
                "   and t_c_firstyear.sale_type = 'c'\n" +
                "   and t_w_firstyear.sale_type = 'w'\n" +
                "   and t_s_secyear.sale_type = 's'\n" +
                "   and t_c_secyear.sale_type = 'c'\n" +
                "   and t_w_secyear.sale_type = 'w'\n" +
                "   and t_s_firstyear.dyear =  2001\n" +
                "   and t_s_secyear.dyear = 2001+1\n" +
                "   and t_c_firstyear.dyear =  2001\n" +
                "   and t_c_secyear.dyear =  2001+1\n" +
                "   and t_w_firstyear.dyear = 2001\n" +
                "   and t_w_secyear.dyear = 2001+1\n" +
                "   and t_s_firstyear.year_total > 0\n" +
                "   and t_c_firstyear.year_total > 0\n" +
                "   and t_w_firstyear.year_total > 0\n" +
                "   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end\n" +
                "           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end\n" +
                "   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end\n" +
                "           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end\n" +
                " order by t_s_secyear.customer_id\n" +
                "         ,t_s_secyear.customer_first_name\n" +
                "         ,t_s_secyear.customer_last_name\n" +
                "         ,t_s_secyear.customer_email_address\n" +
                "limit 100";
        assertEquals(expected, query4String);
    }

    @Test
    public void testQuery55String() throws Exception {
        String query55String = QueryReader.readQuery("query55");
        String expected = "select  i_brand_id brand_id, i_brand brand,\n" +
                " \tsum(ss_ext_sales_price) ext_price\n" +
                " from date_dim, store_sales, item\n" +
                " where d_date_sk = ss_sold_date_sk\n" +
                " \tand ss_item_sk = i_item_sk\n" +
                " \tand i_manager_id=36\n" +
                " \tand d_moy=12\n" +
                " \tand d_year=2001\n" +
                " group by i_brand, i_brand_id\n" +
                " order by ext_price desc, i_brand_id\n" +
                "limit 100";
        assertEquals(expected, query55String);
    }
}
