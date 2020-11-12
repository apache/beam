package org.apache.beam.validate.runner.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.beam.validate.runner.model.Configuration;
import org.apache.beam.validate.runner.model.TestResult;
import org.apache.beam.validate.runner.util.FileReaderUtil;

import java.util.Map;

public class BatchTestService implements TestService {

    public JSONObject getBatchTests() {
        JSONArray batchObject = new JSONArray();
        try {
            Configuration configuration = FileReaderUtil.readConfiguration();
            for(Map<String, String> job : configuration.getBatch()) {
                try {
                    TestResult testResult = new ObjectMapper().readValue(getUrl(job,configuration), TestResult.class);
                    batchObject.add(getBatchObject(job,testResult));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        JSONObject outputDetails = new JSONObject();
        outputDetails.put("batch", batchObject);
        return outputDetails;
    }
}
