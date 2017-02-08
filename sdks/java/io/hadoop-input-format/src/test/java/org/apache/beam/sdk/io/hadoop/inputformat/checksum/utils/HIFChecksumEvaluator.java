package org.apache.beam.sdk.io.hadoop.inputformat.checksum.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.annotation.Nonnull;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * Util class to evaluate checksums.
 */
public class HIFChecksumEvaluator {
  static Logger logger = Logger.getLogger("HIFChecksumEvaluator");

  
  public void evaluateChecksumForCassandra() throws SecurityException, IOException {
    List<String> data = new ArrayList<String>();
    FileHandler fh = new FileHandler("Cassandra.log");
    logger.addHandler(fh);
    Cluster cluster = Cluster.builder().addContactPoint("104.154.207.171").build();
    Session session = cluster.connect("ycsb");
    ResultSet resultSet = session.execute("select * from usertable");
    Row row = null;
    while ((row = resultSet.one()) != null) {
      data.add(row.getString("y_id") + "|" + row.getString("field0") + "|" + row
          .getString("field1") +"|" + row.getString("field2") +"|"+ row.getString("field3") + "|"
          + row.getString("field4") + "|"+ row.getString("field5") + "|" + row.getString("field6") + "|"
          + row.getString("field7") + "|" + row.getString("field8") + "|"+ row.getString("field9") + "|"
          + row.getString("field10") + "|" + row.getString("field11") + "|"+ row.getString("field12") + "|"
          + row.getString("field13") + "|" + row.getString("field14") + "|" + row.getString("field15") + "|"
          + row.getString("field16"));
    }
    cluster.close();
    String generatedHash = generateHash(data);
    System.out.println(generatedHash);
    logger.info(generatedHash);
  }

  @Test
  public void evaluateChecksumForElasticsearch() throws SecurityException, IOException {
    List<String> data = new ArrayList<String>();
    FileHandler fh = new FileHandler("ESRestClient.log");
    logger.addHandler(fh);
    fh.setFormatter(new SimpleFormatter());
    int count = 0;
    Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();

    TransportClient client = new PreBuiltTransportClient(settings);
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress
        .getByName("104.154.49.102"), 9300));

    List<Map<String, Object>> esData = new ArrayList<Map<String, Object>>();
    SearchResponse response = null;
    response =
        client.prepareSearch("test_data").setTypes("test_type")
            .setQuery(QueryBuilders.matchAllQuery()).setSize(1000).execute().actionGet();
    for (SearchHit hit : response.getHits()) {
      count++;
      // User_Name, Item_Code ani Txn_ID
      data.add(hit.getSource().get("User_Name").toString() + "|"
          + hit.getSource().get("Item_Code").toString() + "|"
          + hit.getSource().get("Txn_ID").toString() + "|"
          + hit.getSource().get("Item_ID").toString() + "|"
          + hit.getSource().get("last_updated").toString() + "|"
          + hit.getSource().get("Price").toString() + "|"
          + hit.getSource().get("Title").toString() + "|"
          + hit.getSource().get("Description").toString() + "|"
          + hit.getSource().get("Age").toString() + "|"
          + hit.getSource().get("Item_Name").toString() + "|"
          + hit.getSource().get("Item_Price").toString() + "|"
          + hit.getSource().get("Availability").toString() + "|"
          + hit.getSource().get("Batch_Num").toString() + "|"
          + hit.getSource().get("Last_Ordered").toString() + "|"
          + hit.getSource().get("City").toString() + "|")
          ;
      //+ hit.getSource().get("Country").toString()+ "|")
    }
    logger.info("Count = " + count + "|" + generateHash(data));
   

  }

  public static void main(String[] args) {
    // evaluateChecksumForCassandra();
  }

  private static String generateHash(@Nonnull List<String> elements) {
    List<HashCode> rowHashes = Lists.newArrayList();
    for (String element : elements) {
      rowHashes.add(Hashing.sha1().hashString(element, StandardCharsets.UTF_8));
    }
    return Hashing.combineUnordered(rowHashes).toString();
  }
}
