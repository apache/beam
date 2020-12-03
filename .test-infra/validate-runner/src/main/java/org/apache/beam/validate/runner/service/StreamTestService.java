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


public class StreamTestService implements TestService {
    private static Set<Pair<String, String>> streamTests = new HashSet<>();
    private HashMap<String, Set<CaseResult>> map = new HashMap<>();
    public JSONObject getStreamTests() {
        try {
            Configuration configuration = FileReaderUtil.readConfiguration();
            for (Map<String, String> job : configuration.getStream()) {
                try {
                    TestResult result = new ObjectMapper().readValue(getUrl(job, configuration), TestResult.class);
                    streamTests.addAll(getTestNames(result));
                    map.put((String) job.keySet().toArray()[0], getAllTests(result));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        JSONObject outputDetails = new JSONObject();
        outputDetails.put("stream", process(streamTests, map));
        return outputDetails;
    }
}
