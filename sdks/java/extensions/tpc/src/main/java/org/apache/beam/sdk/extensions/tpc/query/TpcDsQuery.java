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
package org.apache.beam.sdk.extensions.tpc.query;

/** Ds.*/
public class TpcDsQuery {
    public static final String QUERY3 =
            "select  dt.d_year \n" +
            "       ,item.i_brand_id brand_id \n" +
            "       ,item.i_brand brand\n" +
            "       ,sum(ss_sales_price) sum_agg\n" +
            " from  date_dim dt \n" +
            "      ,store_sales\n" +
            "      ,item\n" +
            " where dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
            "   and store_sales.ss_item_sk = item.i_item_sk\n" +
            "   and item.i_manufact_id = 816\n" +
            "   and dt.d_moy=11\n" +
            " group by dt.d_year\n" +
            "      ,item.i_brand\n" +
            "      ,item.i_brand_id\n" +
            " order by dt.d_year\n" +
            "         ,sum_agg desc\n" +
            "         ,brand_id\n" +
            " limit 100";

    public static final String QUERY7 = "select  i_item_id, \n" +
            "        avg(ss_quantity) agg1,\n" +
            "        avg(ss_list_price) agg2,\n" +
            "        avg(ss_coupon_amt) agg3,\n" +
            "        avg(ss_sales_price) agg4 \n" +
            " from store_sales, customer_demographics, date_dim, item, promotion\n" +
            " where ss_sold_date_sk = d_date_sk and\n" +
            "       ss_item_sk = i_item_sk and\n" +
            "       ss_cdemo_sk = cd_demo_sk and\n" +
            "       ss_promo_sk = p_promo_sk and\n" +
            "       cd_gender = 'F' and \n" +
            "       cd_marital_status = 'W' and\n" +
            "       cd_education_status = 'College' and\n" +
            "       (p_channel_email = 'N' or p_channel_event = 'N') and\n" +
            "       d_year = 2001 \n" +
            " group by i_item_id\n" +
            " order by i_item_id\n" +
            " limit 100";

  public static final String QUERY11 =
      "with year_total as (\n"
          + " select c_customer_id customer_id\n"
          + "       ,c_first_name customer_first_name\n"
          + "       ,c_last_name customer_last_name\n"
          + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
          + "       ,c_birth_country customer_birth_country\n"
          + "       ,c_login customer_login\n"
          + "       ,c_email_address customer_email_address\n"
          + "       ,d_year dyear\n"
          + "       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total\n"
          + "       ,'s' sale_type\n"
          + " from customer\n"
          + "     ,store_sales\n"
          + "     ,date_dim\n"
          + " where c_customer_sk = ss_customer_sk\n"
          + "   and ss_sold_date_sk = d_date_sk\n"
          + " group by c_customer_id\n"
          + "         ,c_first_name\n"
          + "         ,c_last_name\n"
          + "         ,c_preferred_cust_flag \n"
          + "         ,c_birth_country\n"
          + "         ,c_login\n"
          + "         ,c_email_address\n"
          + "         ,d_year \n"
          + " union all\n"
          + " select c_customer_id customer_id\n"
          + "       ,c_first_name customer_first_name\n"
          + "       ,c_last_name customer_last_name\n"
          + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
          + "       ,c_birth_country customer_birth_country\n"
          + "       ,c_login customer_login\n"
          + "       ,c_email_address customer_email_address\n"
          + "       ,d_year dyear\n"
          + "       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total\n"
          + "       ,'w' sale_type\n"
          + " from customer\n"
          + "     ,web_sales\n"
          + "     ,date_dim\n"
          + " where c_customer_sk = ws_bill_customer_sk\n"
          + "   and ws_sold_date_sk = d_date_sk\n"
          + " group by c_customer_id\n"
          + "         ,c_first_name\n"
          + "         ,c_last_name\n"
          + "         ,c_preferred_cust_flag \n"
          + "         ,c_birth_country\n"
          + "         ,c_login\n"
          + "         ,c_email_address\n"
          + "         ,d_year\n"
          + "         )\n"
          + "  select  \n"
          + "                  t_s_secyear.customer_id\n"
          + "                 ,t_s_secyear.customer_first_name\n"
          + "                 ,t_s_secyear.customer_last_name\n"
          + "                 ,t_s_secyear.customer_email_address\n"
          + " from year_total t_s_firstyear\n"
          + "     ,year_total t_s_secyear\n"
          + "     ,year_total t_w_firstyear\n"
          + "     ,year_total t_w_secyear\n"
          + " where t_s_secyear.customer_id = t_s_firstyear.customer_id\n"
          + "         and t_s_firstyear.customer_id = t_w_secyear.customer_id\n"
          + "         and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n"
          + "         and t_s_firstyear.sale_type = 's'\n"
          + "         and t_w_firstyear.sale_type = 'w'\n"
          + "         and t_s_secyear.sale_type = 's'\n"
          + "         and t_w_secyear.sale_type = 'w'\n"
          + "         and t_s_firstyear.dyear = 1998\n"
          + "         and t_s_secyear.dyear = 1998+1\n"
          + "         and t_w_firstyear.dyear = 1998\n"
          + "         and t_w_secyear.dyear = 1998+1\n"
          + "         and t_s_firstyear.year_total > 0\n"
          + "         and t_w_firstyear.year_total > 0\n"
          + "         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end\n"
          + "             > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end\n"
          + " order by t_s_secyear.customer_id\n"
          + "         ,t_s_secyear.customer_first_name\n"
          + "         ,t_s_secyear.customer_last_name\n"
          + "         ,t_s_secyear.customer_email_address\n"
          + "limit 100";

  public static final String QUERY22 = "select  i_product_name\n" +
          "             ,i_brand\n" +
          "             ,i_class\n" +
          "             ,i_category\n" +
          "             ,avg(inv_quantity_on_hand) qoh\n" +
          "       from inventory\n" +
          "           ,date_dim\n" +
          "           ,item\n" +
          "       where inv_date_sk=d_date_sk\n" +
          "              and inv_item_sk=i_item_sk\n" +
          "              and d_month_seq between 1200 and 1200 + 11\n" +
          "       group by rollup(i_product_name\n" +
          "                       ,i_brand\n" +
          "                       ,i_class\n" +
          "                       ,i_category)\n" +
          "order by qoh, i_product_name, i_brand, i_class, i_category\n" +
          "limit 100";

  public static final String QUERY38 = "select  count(*) from (\n" +
          "    select distinct c_last_name, c_first_name, d_date\n" +
          "    from store_sales, date_dim, customer\n" +
          "          where store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
          "      and store_sales.ss_customer_sk = customer.c_customer_sk\n" +
          "      and d_month_seq between 1189 and 1189 + 11\n" +
          "  intersect\n" +
          "    select distinct c_last_name, c_first_name, d_date\n" +
          "    from catalog_sales, date_dim, customer\n" +
          "          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n" +
          "      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
          "      and d_month_seq between 1189 and 1189 + 11\n" +
          "  intersect\n" +
          "    select distinct c_last_name, c_first_name, d_date\n" +
          "    from web_sales, date_dim, customer\n" +
          "          where web_sales.ws_sold_date_sk = date_dim.d_date_sk\n" +
          "      and web_sales.ws_bill_customer_sk = customer.c_customer_sk\n" +
          "      and d_month_seq between 1189 and 1189 + 11\n" +
          ") hot_cust\n" +
          "limit 100";

  public static final String QUERY42 = "select  dt.d_year\n" +
          "    ,item.i_category_id\n" +
          "    ,item.i_category\n" +
          "    ,sum(ss_ext_sales_price)\n" +
          " from   date_dim dt\n" +
          "    ,store_sales\n" +
          "    ,item\n" +
          " where dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
          "    and store_sales.ss_item_sk = item.i_item_sk\n" +
          "    and item.i_manager_id = 1   \n" +
          "    and dt.d_moy=11\n" +
          "    and dt.d_year=1998\n" +
          " group by   dt.d_year\n" +
          "        ,item.i_category_id\n" +
          "        ,item.i_category\n" +
          " order by       sum(ss_ext_sales_price) desc,dt.d_year\n" +
          "        ,item.i_category_id\n" +
          "        ,item.i_category\n" +
          "limit 100";
}
