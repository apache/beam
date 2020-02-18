package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import java.io.FileReader;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class SchemaJSONLoader {
  public static String parseTableSchema(String tableName) throws Exception {
    JSONObject jsonObject = (JSONObject) new JSONParser().parse(new FileReader(tableName + ".json"));
    JSONArray jsonArray = (JSONArray) jsonObject;
    Iterator itr2 = jsonArray.iterator();
    Iterator<Map.Entry> itr1;
    while (itr2.hasNext())
    {
      itr1 = ((Map) itr2.next()).entrySet().iterator();
      while (itr1.hasNext()) {
        Map.Entry pair = itr1.next();
        System.out.println(pair.getKey() + " : " + pair.getValue());
      }
    }
    return "";
  }
}
