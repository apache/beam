package org.apache.beam.examples.spanner.csvloader;

import java.lang.Iterable;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ValueBinder;
import com.google.cloud.Timestamp;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.disney.dtss.desa.tools.SpannerCSVLoader.TableInfo;
import com.disney.dtss.desa.tools.SpannerCSVLoader.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerMutationBuilder implements Serializable {

    private static final SimpleDateFormat mRFC3339 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private static final Logger mLog = LoggerFactory.getLogger(SpannerMutationBuilder.class);
 
    private final TableInfo tableInfo;

    public SpannerMutationBuilder(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public Mutation build(String csvData) throws Exception {

        CSVParser parser = CSVParser.parse(csvData, CSVFormat.EXCEL);
        List<CSVRecord> list = parser.getRecords();
        if (list.size() != 1)
            throw new IllegalStateException(String.format("CSV Record count = %d, should only be 1", list.size()));

        String[] fields = new String[list.get(0).size()];
        for (int i = 0; i < fields.length; i++)
            fields[i] = list.get(0).get(i);

        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableInfo.getTableName());

        int i = -1;
        for (Schema column  : tableInfo.getSchema()) {
            i++;
            String name = column.getName();
            String type = column.getType();
            String mode = "N/A";
            if (column.isExclude())
                continue;
            convert(builder.set(name), mode, type, fields[i]);
        }

        return builder.build();
    }


    private Date makeDate(Object value) throws Exception {
        Date d = null;
        String s = (String) value;
        if (s != null && ! "".equals(s.trim())) {
            if (s.indexOf('.') != -1)
                try {
                    d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z").parse(s);
                }
                catch (Exception e) {
                    d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(s);
                }
            else
                d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse(s);
            //d = new Date(Long.parseLong((String) value) / 1000);
        }
        return d;
    }


    private Timestamp toTimestamp(Date d) {
        return Timestamp.parseTimestamp(mRFC3339.format(d));
    }


    private void convert(ValueBinder<Mutation.WriteBuilder> vb, String mode, String type, Object value) throws Exception {
        if("REPEATED".equals(mode)) {
            if ("STRING".equals(type)) {
                vb.toStringArray((Iterable<String>) value);
                return;
            }
            if ("INTEGER".equals(type)) {
                List<Long> vl = new ArrayList();
                for (String s : (List<String>) value)
                    if (s == null || "".equals(s.trim()))
                        vl.add(0L);
                    else
                        vl.add(Long.parseLong(s));
                vb.toInt64Array(vl);
                return;
            }
            if ("DOUBLE".equals(type)) {
                List<Double> vl = new ArrayList();
                for (Double d : (List<Double>) value)
                    vl.add(d);
                vb.toFloat64Array(vl);
                return;
            }
            if ("BOOLEAN".equals(type)) {
                List<Boolean> vl = new ArrayList();
                for (Boolean b : (List<Boolean>) value)
                    vl.add(b);
                vb.toBoolArray(vl);
                return;
            }
            if ("TIMESTAMP".equals(type)) {
                List<Timestamp> vl = new ArrayList();
                for (String s : (List<String>) value)
                    vl.add(toTimestamp(makeDate(s)));
                vb.toTimestampArray(vl);
                return;
            }
        }
        else {
            String s = (String) value;
            if (s != null)
                s = s.trim();
            if ("".equals(s) || "NULL".equals(s))
                s = null;
            
            if ("STRING".equals(type)) {
                vb.to(s);
                return;
            }
            if ("INTEGER".equals(type)) {
                if (s == null)
                    vb.to((Long) null);
                else 
                    vb.to(Long.parseLong(s));
                return;
            }
            if ("DOUBLE".equals(type)) {
                if (s == null)
                    vb.to((Double) null);
                else
                    vb.to(Double.parseDouble(s));
                return;
            }
            if ("BOOLEAN".equals(type)) {
                if ( s != null && "1".equals(s))
                    s = "true";
                if ( s != null && "0".equals(s))
                    s = "false";
                if (s == null)
                    vb.to((Boolean) null);
                else
                    vb.to(Boolean.parseBoolean(s));
                return;
            }
            if ("TIMESTAMP".equals(type)) {
                if (s == null)
                    vb.to((Timestamp) null);
                else
                    vb.to(toTimestamp(makeDate(s)));
                return;
            }
        }
        mLog.error("The BigQuery type " + type + " is not currently supported.");
        System.exit(0);
    }
}
