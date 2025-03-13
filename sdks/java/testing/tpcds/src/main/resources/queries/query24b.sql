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

 with ssales as
 (select c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color,
         i_current_price, i_manager_id, i_units, i_size, sum(ss_net_paid) netpaid
 from store_sales, store_returns, store, item, customer, customer_address
 where ss_ticket_number = sr_ticket_number
   and ss_item_sk = sr_item_sk
   and ss_customer_sk = c_customer_sk
   and ss_item_sk = i_item_sk
   and ss_store_sk = s_store_sk
   and c_birth_country = upper(ca_country)
   and s_zip = ca_zip
   and s_market_id = 8
 group by c_last_name, c_first_name, s_store_name, ca_state, s_state,
          i_color, i_current_price, i_manager_id, i_units, i_size)
 select c_last_name, c_first_name, s_store_name, sum(netpaid) paid
 from ssales
 where i_color = 'chiffon'
 group by c_last_name, c_first_name, s_store_name
 having sum(netpaid) > (select 0.05*avg(netpaid) from ssales)