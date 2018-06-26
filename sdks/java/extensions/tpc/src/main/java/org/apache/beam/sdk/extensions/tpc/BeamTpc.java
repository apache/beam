package org.apache.beam.sdk.extensions.tpc;

import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
//import org.apache.beam.sdk.extensions.tpc.CsvToRow;
//import org.apache.beam.sdk.extensions.tpc.RowToCsv;
//import org.apache.beam.sdk.schemas.Schema;
//import org.apache.beam.sdk.schemas.Schema;

/** Test now. */
public class BeamTpc {

  public static void main(String[] args) {
//    Schema reasonSchema =
//        Schema.builder()
//            .addInt32Field("r_reason_sk")
//            .addStringField("r_reason_id")
//            .addStringField("r_reason_desc")
//            .build();

    // Option for lanunch Tpc benchmark.
    TpcOptions tpcOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcOptions.class);

    //        PipelineOptions options =
    // PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(tpcOptions);

    String rootPath = tpcOptions.getInputFile();

    String storeSalesFilePath = rootPath + "store_sales_c.dat";
    String dateDimFilePath = rootPath + "date_dim.dat";
    String itemFilePath = rootPath + "item.dat";
    String nationFilePath = rootPath + "nation.tbl";
    //      String dateDimFilePath = rootPath + "date_dim.dat";
    //      String itemFilePath = rootPath + "item.dat";
    //      String storeFilePath = rootPath + "store.dat";
    //      String storeReturnFilePath = rootPath + "store_returns.dat";
    String customerFilePath = rootPath + "customer_c.tbl";
    String lineitemFilePath = rootPath + "lineitem_c.tbl";
    String orderFilePath = rootPath + "orders_c.tbl";
    //      String catalogSalesFilePath = rootPath + "catalog_sales.dat";
    //      String catalogReturnsFilePath = rootPath + "catalog_returns.dat";
    //      String inventoryFilePath = rootPath + "inventory.dat";
    //      String webSalesFilePath = rootPath + "web_sales.dat";
    //      String webReturnsFilePath = rootPath + "web_returns.dat";
    //      String callCenterFilePath = rootPath + "call_center.dat";
    //      String catalogPageFilePath = rootPath + "catalog_page.dat";
    //      String customerAddressFilePath = rootPath + "customer_address.dat";
    //      String customerDemographicsFilePath = rootPath + "customer_demographics.dat";
    //      String householdDemographicsFilePath = rootPath + "household_demographics.dat";
    //      String incomeBandFilePath = rootPath + "income_band.dat";
    //      String promotionFilePath = rootPath + "promotion.dat";
    //      String shipModeFilePath = rootPath + "ship_mode.dat";
    //      String timeDimFilePath = rootPath + "time_dim.dat";
    //      String warehouseFilePath = rootPath + "warehouse.dat";
    //      String webPageFilePath = rootPath + "web_page.dat";
    //      String webSiteFilePath = rootPath + "web_site.dat";

    CSVFormat csvFormat = CSVFormat.MYSQL.withDelimiter('|').withNullString("");

    PCollection<Row> storeSalesTable =
        new TextTable(
                SchemaUtil.storeSalesSchema,
                storeSalesFilePath,
                new CsvToRow(SchemaUtil.storeSalesSchema, csvFormat),
                new RowToCsv(csvFormat))
            .buildIOReader(pipeline.begin())
            .setCoder(SchemaUtil.storeSalesSchema.getRowCoder());

    PCollection<Row> dateDimTable =
        new TextTable(
                SchemaUtil.dateDimSchema,
                dateDimFilePath,
                new CsvToRow(SchemaUtil.dateDimSchema, csvFormat),
                new RowToCsv(csvFormat))
            .buildIOReader(pipeline.begin())
            .setCoder(SchemaUtil.dateDimSchema.getRowCoder());

    PCollection<Row> itemTable =
        new TextTable(
                SchemaUtil.itemSchema,
                itemFilePath,
                new CsvToRow(SchemaUtil.itemSchema, csvFormat),
                new RowToCsv(csvFormat))
            .buildIOReader(pipeline.begin())
            .setCoder(SchemaUtil.itemSchema.getRowCoder());

    //      PCollection<Row> reasonTable =
    //              new BeamTextCSVTable(reasonSchema, reasonFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(reasonSchema.getRowCoder());

    //      Schema nationSchema = SchemaUtil.storeSalesSchema;

    //      PCollection<Row> nationTable =
    //              new BeamTextCSVTable(nationSchema, nationFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(nationSchema.getRowCoder());

    //      PCollection<Row> dateDimTable =
    //              new BeamTextCSVTable(dateDimSchema, dateDimFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(dateDimSchema.getRowCoder());
    //
    //      PCollection<Row> itemTable =
    //              new BeamTextCSVTable(itemSchema, itemFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(itemSchema.getRowCoder());
    ////
    //      PCollection<Row> storeTable =
    //              new BeamTextCSVTable(storeSchema, storeFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(storeSchema.getRowCoder());
    ////
    //      PCollection<Row> storeReturnTable =
    //              new BeamTextCSVTable(storeReturnSchema, storeReturnFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(storeReturnSchema.getRowCoder());
    ////
    PCollection<Row> customerTable =
        new TextTable(
                SchemaUtil.customerSchema,
                customerFilePath,
                new CsvToRow(SchemaUtil.customerSchema, csvFormat),
                new RowToCsv(csvFormat))
            .buildIOReader(pipeline.begin())
            .setCoder(SchemaUtil.customerSchema.getRowCoder());

    PCollection<Row> orderTable =
        new TextTable(
                SchemaUtil.orderSchema,
                orderFilePath,
                new CsvToRow(SchemaUtil.orderSchema, csvFormat),
                new RowToCsv(csvFormat))
            .buildIOReader(pipeline.begin())
            .setCoder(SchemaUtil.orderSchema.getRowCoder());

    PCollection<Row> lineitemTable =
        new TextTable(
                SchemaUtil.lineitemSchema,
                lineitemFilePath,
                new CsvToRow(SchemaUtil.lineitemSchema, csvFormat),
                new RowToCsv(csvFormat))
            .buildIOReader(pipeline.begin())
            .setCoder(SchemaUtil.lineitemSchema.getRowCoder());
    //
    //      PCollection<Row> catalogSalesTable =
    //              new BeamTextCSVTable(catalogSalesSchema, catalogSalesFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(catalogSalesSchema.getRowCoder());
    ////
    //      PCollection<Row> catalogReturnsTable =
    //              new BeamTextCSVTable(catalogReturnsSchema, catalogReturnsFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(catalogReturnsSchema.getRowCoder());
    ////
    //      PCollection<Row> inventoryTable =
    //              new BeamTextCSVTable(inventorySchema, inventoryFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(inventorySchema.getRowCoder());
    //
    //      PCollection<Row> webSalesTable =
    //              new BeamTextCSVTable(webSalesSchema, webSalesFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(webSalesSchema.getRowCoder());
    //      //
    //      PCollection<Row> webReturnsTable =
    //              new BeamTextCSVTable(webReturnsSchema, webReturnsFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(webReturnsSchema.getRowCoder());

    //        PCollection<Row> callCenterTable =
    //            new BeamTextCSVTable(callCenterSchema, callCenterFilePath, format)
    //                .buildIOReader(pipeline)
    //                .setCoder(callCenterSchema.getRowCoder());

    //    PCollection<Row> catalogPageTable =
    //        new BeamTextCSVTable(catalogPageSchema, catalogPageFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(catalogPageSchema.getRowCoder());

    //      PCollection<Row> customerAddressTable =
    //              new BeamTextCSVTable(customerAddressSchema, customerAddressFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(customerAddressSchema.getRowCoder());
    //
    //      PCollection<Row> customerDemographicsTable =
    //              new BeamTextCSVTable(customerDemographicsSchema, customerDemographicsFilePath,
    // format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(customerDemographicsSchema.getRowCoder());
    //
    //      PCollection<Row> householdDemographicsTable =
    //              new BeamTextCSVTable(householdDemographicsSchema, householdDemographicsFilePath,
    //                      format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(householdDemographicsSchema.getRowCoder());

    //    PCollection<Row> incomeBandTable =
    //        new BeamTextCSVTable(incomeBandSchema, incomeBandFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(incomeBandSchema.getRowCoder());

    //      PCollection<Row> promotionTable =
    //              new BeamTextCSVTable(promotionSchema, promotionFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(promotionSchema.getRowCoder());
    //
    //      PCollection<Row> shipModeTable =
    //              new BeamTextCSVTable(shipModeSchema, shipModeFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(shipModeSchema.getRowCoder());
    //
    //      PCollection<Row> timeDimTable =
    //              new BeamTextCSVTable(timeDimSchema, timeDimFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(timeDimSchema.getRowCoder());
    //
    //      PCollection<Row> warehouseTable =
    //              new BeamTextCSVTable(warehouseSchema, warehouseFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(warehouseSchema.getRowCoder());

    //    PCollection<Row> webPageTable =
    //        new BeamTextCSVTable(webPageSchema, webPageFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(webPageSchema.getRowCoder());

    //    PCollection<Row> webSiteTable =
    //        new BeamTextCSVTable(webSiteSchema, webSiteFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(webSiteSchema.getRowCoder());

    PCollectionTuple tables =
        PCollectionTuple.of(new TupleTag<>("store_sales"), storeSalesTable)
            //                      .of(new TupleTag<>("nation"), nationTable)
            .and(new TupleTag<>("date_dim"), dateDimTable)
            //                      .and(new TupleTag<>("store_sales"), storeSalesTable)
            //                      .and(new TupleTag<>("store"), storeTable)
            .and(new TupleTag<>("item"), itemTable)
            //                        .and(new TupleTag<>("store_returns"), storeReturnTable)
            .and(new TupleTag<>("customer"), customerTable)
            .and(new TupleTag<>("orders"), orderTable)
            .and(new TupleTag<>("lineitem"), lineitemTable)
        //                      .and(new TupleTag<>("catalog_sales"), catalogSalesTable)
        //                            .and(new TupleTag<>("catalog_returns"), catalogReturnsTable)
        //                      .and(new TupleTag<>("inventory"), inventoryTable)
        //                      .and(new TupleTag<>("web_sales"), webSalesTable)
        //                      .and(new TupleTag<>("web_returns"), webReturnsTable)
        //                            .and(new TupleTag<>("call_center"), callCenterTable)
        //                .and(new TupleTag<>("catalog_page"), catalogPageTable)
        //                      .and(new TupleTag<>("customer_address"), customerAddressTable)
        //                      .and(new TupleTag<>("customer_demographics"),
        // customerDemographicsTable)
        //                      .and(new TupleTag<>("household_demographics"),
        // householdDemographicsTable)
        //                .and(new TupleTag<>("income_band"), incomeBandTable)
        //                      .and(new TupleTag<>("promotion"), promotionTable)
        //                      .and(new TupleTag<>("ship_mode"), shipModeTable)
        //                      .and(new TupleTag<>("time_dim"), timeDimTable)
        //                      .and(new TupleTag<>("warehouse"), warehouseTable)
        //                .and(new TupleTag<>("web_page"), webPageTable)
        //                .and(new TupleTag<>("web_site"), webSiteTable)
        ;

    String query =
        "select  dt.d_year \n"
            + "       ,item.i_brand_id brand_id \n"
            + "       ,item.i_brand brand\n"
            + "       ,sum(ss_sales_price) sum_agg\n"
            + " from  date_dim dt \n"
            + "      ,store_sales\n"
            + "      ,item\n"
            + " where dt.d_date_sk = store_sales.ss_sold_date_sk\n"
            + "   and store_sales.ss_item_sk = item.i_item_sk\n"
            + "   and item.i_manufact_id = 816\n"
            + "   and dt.d_moy=11\n"
            + " group by dt.d_year\n"
            + "      ,item.i_brand\n"
            + "      ,item.i_brand_id\n"
            + " order by dt.d_year\n"
            + "         ,sum_agg desc\n"
            + "         ,brand_id\n"
            + " limit 100";

    String queryh =
        "select\n"
            + "\tl_orderkey,\n"
            + "\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n"
            + "\to_orderdate,\n"
            + "\to_shippriority\n"
            + "from\n"
            + "\tcustomer,\n"
            + "\torders,\n"
            + "\tlineitem\n"
            + "where\n"
            + "\tc_mktsegment = 'BUILDING'\n"
            + "\tand c_custkey = o_custkey\n"
            + "\tand l_orderkey = o_orderkey\n"
            + "\tand o_orderdate < date '1995-03-15'\n"
            + "\tand l_shipdate > date '1995-03-15'\n"
            + "group by\n"
            + "\tl_orderkey,\n"
            + "\to_orderdate,\n"
            + "\to_shippriority\n"
            + "order by\n"
            + "\trevenue desc,\n"
            + "\to_orderdate\n"
            + "limit 10";

    tables
        .apply(SqlTransform.query(queryh))
        .apply(
            "exp_table",
            MapElements.via(
                new SimpleFunction<Row, Void>() {
                  @Override
                  public @Nullable Void apply(Row input) {
                    // expect output:
                    //  PCOLLECTION: [3, row, 3.0]
                    System.out.println("row: " + input.getValues());
                    return null;
                  }
                }));

    pipeline.run().waitUntilFinish();

    System.out.println("hi");
  }
}
