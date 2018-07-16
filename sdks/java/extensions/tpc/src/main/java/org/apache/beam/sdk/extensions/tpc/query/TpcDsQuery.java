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
}
