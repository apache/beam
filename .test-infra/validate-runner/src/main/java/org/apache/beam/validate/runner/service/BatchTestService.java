package org.apache.beam.validate.runner.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.json.JSONObject;
import org.apache.beam.validate.runner.model.CaseResult;
import org.apache.beam.validate.runner.model.Configuration;
import org.apache.beam.validate.runner.model.TestResult;
import org.apache.beam.validate.runner.util.FileReaderUtil;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BatchTestService implements TestService {
    private static Set<Pair<String, String>> batchTests = new HashSet<>();
    private HashMap<String, Set<CaseResult>> map = new HashMap<>();
    public JSONObject getBatchTests() {
        try {
            Configuration configuration = FileReaderUtil.readConfiguration();
            for(Map<String, String> job : configuration.getBatch()) {
                try {
                    TestResult testResult = new ObjectMapper().readValue(getUrl(job,configuration), TestResult.class);
                    batchTests.addAll(getTestNames(testResult));
                    map.put((String) job.keySet().toArray()[0], getAllTests(testResult));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        JSONObject outputDetails = new JSONObject();
        outputDetails.put("batch", process(batchTests, map));
        return outputDetails;
    }
}
