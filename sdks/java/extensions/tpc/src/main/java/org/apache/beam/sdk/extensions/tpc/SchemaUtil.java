package org.apache.beam.sdk.extensions.tpc;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.Schema;

/** Tpc Schema. */
public class SchemaUtil {
  //  private static final JavaTypeFactory TYPE_FACTORY =
  //      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  //  private final ImmutableMap<String, TpcdsTable> tableHMap;
  private final ImmutableMap<String, String> columnPrefixes;

  public static Schema storeSalesSchema =
      Schema.builder()
          .addNullableField("ss_sold_date_sk", Schema.FieldType.INT32)
          .addNullableField("ss_sold_time_sk", Schema.FieldType.INT32)
          .addNullableField("ss_item_sk", Schema.FieldType.INT32)
          .addNullableField("ss_customer_sk", Schema.FieldType.STRING)
          .addNullableField("ss_cdemo_sk", Schema.FieldType.INT32)
          .addNullableField("ss_hdemo_sk", Schema.FieldType.INT32)
          .addNullableField("ss_addr_sk", Schema.FieldType.INT32)
          .addNullableField("ss_store_sk", Schema.FieldType.INT32)
          .addNullableField("ss_promo_sk", Schema.FieldType.INT32)
          .addNullableField("ss_ticket_number", Schema.FieldType.INT64)
          .addNullableField("ss_quantity", Schema.FieldType.INT32)
          .addNullableField("ss_wholesale_cost", Schema.FieldType.FLOAT)
          .addNullableField("ss_list_price", Schema.FieldType.FLOAT)
          .addNullableField("ss_sales_price", Schema.FieldType.FLOAT)
          .addNullableField("ss_ext_discount_amt", Schema.FieldType.FLOAT)
          .addNullableField("ss_ext_sales_price", Schema.FieldType.FLOAT)
          .addNullableField("ss_ext_wholesale_cost", Schema.FieldType.FLOAT)
          .addNullableField("ss_ext_list_price", Schema.FieldType.FLOAT)
          .addNullableField("ss_ext_tax", Schema.FieldType.FLOAT)
          .addNullableField("ss_coupon_amt", Schema.FieldType.FLOAT)
          .addNullableField("ss_net_paid", Schema.FieldType.FLOAT)
          .addNullableField("ss_net_paid_inc_tax", Schema.FieldType.FLOAT)
          .addNullableField("ss_net_profit", Schema.FieldType.FLOAT)
          .build();

  public static Schema dateDimSchema =
      Schema.builder()
          .addNullableField("d_date_sk", Schema.FieldType.INT32)
          .addNullableField("d_date_id", Schema.FieldType.STRING)
          .addNullableField("d_date", Schema.FieldType.STRING)
          .addNullableField("d_month_seq", Schema.FieldType.INT32)
          .addNullableField("d_week_seq", Schema.FieldType.INT32)
          .addNullableField("d_quarter_seq", Schema.FieldType.INT32)
          .addNullableField("d_year", Schema.FieldType.INT32)
          .addNullableField("d_dow", Schema.FieldType.INT32)
          .addNullableField("d_moy", Schema.FieldType.INT32)
          .addNullableField("d_dom", Schema.FieldType.INT32)
          .addNullableField("d_qoy", Schema.FieldType.INT32)
          .addNullableField("d_fy_year", Schema.FieldType.INT32)
          .addNullableField("d_fy_quarter_seq", Schema.FieldType.INT32)
          .addNullableField("d_fy_week_seq", Schema.FieldType.INT32)
          .addNullableField("d_day_name", Schema.FieldType.STRING)
          .addNullableField("d_quarter_name", Schema.FieldType.STRING)
          .addNullableField("d_holiday", Schema.FieldType.STRING)
          .addNullableField("d_weekend", Schema.FieldType.STRING)
          .addNullableField("d_following_holiday", Schema.FieldType.STRING)
          .addNullableField("d_first_dom", Schema.FieldType.INT32)
          .addNullableField("d_last_dom", Schema.FieldType.INT32)
          .addNullableField("d_same_day_ly", Schema.FieldType.INT32)
          .addNullableField("d_same_day_lq", Schema.FieldType.INT32)
          .addNullableField("d_current_day", Schema.FieldType.STRING)
          .addNullableField("d_current_week", Schema.FieldType.STRING)
          .addNullableField("d_current_month", Schema.FieldType.STRING)
          .addNullableField("d_current_quarter", Schema.FieldType.STRING)
          .addNullableField("d_current_year", Schema.FieldType.STRING)
          .build();

  public static Schema itemSchema =
      Schema.builder()
          .addNullableField("i_item_sk", Schema.FieldType.INT32)
          .addNullableField("i_item_id", Schema.FieldType.STRING) //                 .string,
          .addNullableField("i_rec_start_date", Schema.FieldType.DATETIME) //          .date,
          .addNullableField("i_rec_end_date", Schema.FieldType.DATETIME) //            .date,
          .addNullableField("i_item_desc", Schema.FieldType.STRING) //             .string,
          .addNullableField("i_current_price", Schema.FieldType.FLOAT) //           .decimal(7,2),
          .addNullableField(
              "i_wholesale_cost", Schema.FieldType.FLOAT) //               .decimal(7,2),
          .addNullableField("i_brand_id", Schema.FieldType.INT32) //                .int,
          .addNullableField("i_brand", Schema.FieldType.STRING) //                   .string,
          .addNullableField("i_class_id", Schema.FieldType.INT32) //                .int,
          .addNullableField("i_class", Schema.FieldType.STRING) //                   .string,
          .addNullableField("i_category_id", Schema.FieldType.INT32) //             .int,
          .addNullableField("i_category", Schema.FieldType.STRING) //       .string,
          .addNullableField("i_manufact_id", Schema.FieldType.INT32) //             .int,
          .addNullableField("i_manufact", Schema.FieldType.STRING) //                .string,
          .addNullableField("i_size", Schema.FieldType.STRING) //                    .string,
          .addNullableField("i_formulation", Schema.FieldType.STRING) //             .string,
          .addNullableField("i_color", Schema.FieldType.STRING) //                   .string,
          .addNullableField("i_units", Schema.FieldType.STRING) //                   .string,
          .addNullableField("i_container", Schema.FieldType.STRING) //               .string,
          .addNullableField("i_manager_id", Schema.FieldType.INT32) // .int,
          .addNullableField("i_product_name", Schema.FieldType.STRING) //            .string),
          .build();

  public static Schema orderSchema =
      Schema.builder()
          .addInt32Field("o_orderkey")
          .addInt32Field("o_custkey")
          .addStringField("o_orderstatus")
          .addFloatField("o_totalprice")
          .addStringField("o_orderdate")
          .addStringField("o_orderpriority")
          .addStringField("o_clerk")
          .addInt32Field("o_shippriority")
          .addStringField("o_comment")
          .build();

  public static Schema customerSchema =
      Schema.builder()
          .addInt32Field("c_custkey")
          .addStringField("c_name")
          .addStringField("c_address")
          .addInt32Field("c_nationkey")
          .addStringField("c_phone")
          .addFloatField("c_acctbal")
          .addStringField("c_mktsegment")
          .addStringField("c_comment")
          .build();

  public static Schema lineitemSchema =
      Schema.builder()
          .addInt32Field("l_orderkey")
          .addInt32Field("l_partkey")
          .addInt32Field("l_suppkey")
          .addInt32Field("l_linenumber")
          .addFloatField("l_quantity")
          .addFloatField("l_extendedprice")
          .addFloatField("l_discount")
          .addFloatField("l_tax")
          .addStringField("l_returnflag")
          .addStringField("l_linestatus")
          .addStringField("l_shipdate")
          .addStringField("l_commitdate")
          .addStringField("l_receiptdate")
          .addStringField("l_shipinstruct")
          .addStringField("l_shipmode")
          .addStringField("l_comment")
          .build();

  public static Schema partsuppSchema =
      Schema.builder()
          .addInt32Field("ps_partkey") //identifier
          .addInt32Field("ps_suppkey") //identifier
          .addInt32Field("ps_availqty") //integer
          .addFloatField("ps_supplycost") //decimal
          .addStringField("ps_comment") //variable text, size 199
          .build();

  public static Schema regionSchema =
      Schema.builder()
          .addInt32Field("r_regionkey") //identifier
          .addStringField("r_name") //fixed text, size 25
          .addStringField("r_comment") //variable text, size 152
          .build();

  public static Schema supplierSchema =
      Schema.builder()
          .addInt32Field("s_suppkey") // identifier
          .addStringField("s_name") // fixed text, size 25
          .addStringField("s_address") //variable text, size 40
          .addInt32Field("s_nationkey") //identifier
          .addStringField("s_phone") //fixed text, size 15
          .addFloatField("s_acctbal") //decimal
          .addStringField("s_comment") //variable text, size 101
          .build();

  public static Schema partSchema =
      Schema.builder()
          .addInt32Field("p_partkey")
          .addStringField("p_name")
          .addStringField("p_mfgr")
          .addStringField("p_brand")
          .addStringField("p_type")
          .addInt32Field("p_size")
          .addStringField("p_container")
          .addFloatField("p_retailprice")
          .addStringField("p_comment")
          .build();

  public static Schema nationSchema =
      Schema.builder()
          .addInt32Field("n_nationkey")
          .addStringField("n_name")
          .addInt32Field("n_regionkey")
          .addStringField("n_comment")
          .build();

  public SchemaUtil() {
    //    final ImmutableMap.Builder<String, TpcdsTable> builder = ImmutableMap.builder();
    //    for (TpcdsTable<?> tpcdsTable : TpcdsTable.getTables()) {
    //      builder.put(tpcdsTable.getTableName(), tpcdsTable);
    //    }

    //    this.tableHMap = builder.build();

    this.columnPrefixes =
        ImmutableMap.<String, String>builder()
            .put("lineitem", "l_")
            .put("customer", "c_")
            .put("supplier", "s_")
            .put("partsupp", "ps_")
            .put("part", "p_")
            .put("orders", "o_")
            .put("nation", "n_")
            .put("region", "r_")
            .build();
  }
}
