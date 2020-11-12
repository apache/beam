package org.apache.beam.validate.runner.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.beam.validate.runner.model.Configuration;
import org.apache.beam.validate.runner.model.TestResult;
import org.apache.beam.validate.runner.util.FileReaderUtil;

import java.net.URISyntaxException;
import java.util.Map;

public class StreamTestService implements TestService {

    public JSONObject getStreamTests() throws URISyntaxException {
        JSONArray streamObject = new JSONArray();
        try {
            Configuration configuration = FileReaderUtil.readConfiguration();
            for (Map<String, String> job : configuration.getStream()) {
                try {
                    TestResult result = new ObjectMapper().readValue(getUrl(job, configuration), TestResult.class);
                    streamObject.add(getBatchObject(job, result));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        JSONObject outputDetails = new JSONObject();
        outputDetails.put("stream", streamObject);
        return outputDetails;
    }
}
