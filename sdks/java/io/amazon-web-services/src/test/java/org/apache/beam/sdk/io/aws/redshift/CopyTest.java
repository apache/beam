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
package org.apache.beam.sdk.io.aws.redshift;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.aws.redshift.Redshift.DataSourceConfiguration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link Copy}. */
@RunWith(JUnit4.class)
public class CopyTest {

  private String dbName = "test-db1";
  private String userName = "xxx";
  private String password = "xxx";
  private String endpoint = "test-cluster1.xxx.us-west-2.redshift.amazonaws.com";
  private int port = 5439;

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testCopy() {
    DataSourceConfiguration dataSourceConfiguration =
        Redshift.DataSourceConfiguration.create(endpoint, port, dbName, userName, password);

    Copy copyFn =
        Copy.builder()
            .setDataSourceConfiguration(dataSourceConfiguration)
            .setDestinationTableSpec("public.part")
            .setIAMRole("arn:aws:iam::xxx:role/xxx-custom-redshift-role")
            .setOptions("csv null as '\\000'")
            .build();

    testPipeline.getOptions().as(AwsOptions.class).setAwsRegion("us-west-2");
    testPipeline.apply(Create.of("s3://xxx/sample/part-csv.tbl")).apply(ParDo.of(copyFn));
    testPipeline.run();
  }

  private void createTable() {
    /* Sample data:
      15,dark sky,MFGR#3,MFGR#47,MFGR#3438,indigo,"LARGE ANODIZED BRASS",45,LG CASE
      22,floral beige,MFGR#4,MFGR#44,MFGR#4421,medium,"PROMO, POLISHED BRASS",19,LG DRUM
      23,bisque slate,MFGR#4,MFGR#41,MFGR#4137,firebrick,"MEDIUM ""BURNISHED"" TIN",42,JUMBO JAR
    */

    Connection conn = null;
    Statement stmt = null;
    try {

      System.out.println("Connecting to database.....");

      DataSource dataSource = Redshift.DataSourceConfiguration.create(endpoint, port, dbName, userName, password).buildDataSource();
      conn = dataSource.getConnection();

      stmt = conn.createStatement();
      String sql;
      sql =
          "CREATE TABLE public.part \n" +
          "(\n" +
          "  p_partkey     INTEGER NOT NULL,\n" +
          "  p_name        VARCHAR(22) NOT NULL,\n" +
          "  p_mfgr        VARCHAR(6),\n" +
          "  p_category    VARCHAR(7) NOT NULL,\n" +
          "  p_brand1      VARCHAR(9) NOT NULL,\n" +
          "  p_color       VARCHAR(11) NOT NULL,\n" +
          "  p_type        VARCHAR(25) NOT NULL,\n" +
          "  p_size        INTEGER NOT NULL,\n" +
          "  p_container   VARCHAR(10) NOT NULL\n" +
          ");";

      String sql5 = "Select * from public.part limit 100";

      ResultSet rs = stmt.executeQuery(sql5);
      //Get the data from the result set.
      while (rs.next()) {
        //Retrieve two columns.
        String catalog = rs.getString("p_type");
        String name = rs.getString("p_name");

        //Display values.
        System.out.print("Catalog: " + catalog);
        System.out.println(", Name: " + name);
      }
      rs.close();
      stmt.close();
      conn.close();
    } catch (Exception ex) {
      //For convenience, handle all errors here.
      System.out.println("Error: " + ex.getMessage());
    } finally {
      //Finally block to close resources.
      try {
        if (stmt != null)
          stmt.close();
      } catch (Exception ex) {
      }// nothing we can do
      try {
        if (conn != null)
          conn.close();
      } catch (Exception ex) {
        System.out.println(ex.getMessage());
      }
    }
    System.out.println("Finished connectivity test.");
  }
}
