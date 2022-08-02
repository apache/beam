-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

with all_sales as (
 select d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,sum(sales_cnt) as sales_cnt
       ,sum(sales_amt) as sales_amt
 from (select d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - coalesce(cr_return_quantity,0) as sales_cnt
             ,cs_ext_sales_price - coalesce(cr_return_amount,0.0) as sales_amt
       from catalog_sales join item on i_item_sk=cs_item_sk
                          join date_dim on d_date_sk=cs_sold_date_sk
                          left join catalog_returns on (cs_order_number=cr_order_number
                                                    and cs_item_sk=cr_item_sk)
       where i_category='Sports'
       union
       select d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ss_quantity - coalesce(sr_return_quantity,0) as sales_cnt
             ,ss_ext_sales_price - coalesce(sr_return_amt,0.0) as sales_amt
       from store_sales join item on i_item_sk=ss_item_sk
                        join date_dim on d_date_sk=ss_sold_date_sk
                        left join store_returns on (ss_ticket_number=sr_ticket_number
                                                and ss_item_sk=sr_item_sk)
       where i_category='Sports'
       union
       select d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ws_quantity - coalesce(wr_return_quantity,0) as sales_cnt
             ,ws_ext_sales_price - coalesce(wr_return_amt,0.0) as sales_amt
       from web_sales join item on i_item_sk=ws_item_sk
                      join date_dim on d_date_sk=ws_sold_date_sk
                      left join web_returns on (ws_order_number=wr_order_number
                                            and ws_item_sk=wr_item_sk)
       where i_category='Sports') sales_detail
 group by d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)
 select  prev_yr.d_year as prev_year
                          ,curr_yr.d_year as`year`
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
 FROM all_sales curr_yr, all_sales prev_yr
 where curr_yr.i_brand_id=prev_yr.i_brand_id
   and curr_yr.i_class_id=prev_yr.i_class_id
   and curr_yr.i_category_id=prev_yr.i_category_id
   and curr_yr.i_manufact_id=prev_yr.i_manufact_id
   and curr_yr.d_year=2002
   and prev_yr.d_year=2002-1
   and cast(curr_yr.sales_cnt as decimal(17,2))/cast(prev_yr.sales_cnt as decimal(17,2))<0.9
 order by sales_cnt_diff
 limit 100