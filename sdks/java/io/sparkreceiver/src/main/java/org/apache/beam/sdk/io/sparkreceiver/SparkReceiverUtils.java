package org.apache.beam.sdk.io.sparkreceiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class SparkReceiverUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverUtils.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static Integer getOffsetByRecord(String record) {
        try {
            HashMap<String, Object> json =
                    objectMapper.readValue(record, HashMap.class);
            return (Integer) json.get("vid");
        } catch (Exception e) {
            LOG.error("Can not parse json", e);
            return 0;
        }
    }
}
