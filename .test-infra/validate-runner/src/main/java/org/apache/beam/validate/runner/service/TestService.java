package org.apache.beam.validate.runner.service;

import com.offbytwo.jenkins.JenkinsServer;
import com.offbytwo.jenkins.model.Job;
import com.offbytwo.jenkins.model.JobWithDetails;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.beam.validate.runner.model.CaseResult;
import org.apache.beam.validate.runner.model.Configuration;
import org.apache.beam.validate.runner.model.TestResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

public interface TestService {

    default Set<CaseResult> getAllTests(TestResult testResult) {
        Set<CaseResult> caseResults = new HashSet<>();
        Optional.ofNullable(testResult.getSuites()).ifPresent(suites -> suites.forEach(item -> caseResults.addAll(item.getCases())));
//        List<String> tests = new ArrayList<>();
//        Optional.ofNullable(caseResults).ifPresent(cases -> cases.forEach(item -> tests.add(item.getClassName() + "." + item.getName() + " : " + item.getStatus())));
        return caseResults;
    }

    default URL getUrl(Map<String, String> job, Configuration configuration) throws URISyntaxException, IOException {
        Map<String, Job> jobs = new JenkinsServer(new URI(configuration.getServer())).getJobs();
        JobWithDetails jobWithDetails = jobs.get(job.get(job.keySet().toArray()[0])).details();
        return new URL(jobWithDetails.getLastSuccessfulBuild().getUrl() + configuration.getJsonapi());
    }

    default Set<Pair<String, String>> getTestNames(TestResult testResult) {
        Set<Pair<String, String>> caseResults = new HashSet<>();
        Optional.ofNullable(testResult.getSuites()).ifPresent(suites -> suites.forEach(item -> item.getCases().forEach(caseResult -> caseResults.add(new ImmutablePair<>(caseResult.getName(), caseResult.getClassName())))));
//        List<String> tests = new ArrayList<>();
//        Optional.ofNullable(caseResults).ifPresent(cases -> cases.forEach(item -> tests.add(item.getClassName() + "." + item.getName() + " : " + item.getStatus())));
        return caseResults;
    }

    default JSONObject process(Set<Pair<String, String>> testnames, HashMap<String, Set<CaseResult>> map) {
        map.forEach((k, v) -> {
            Set<CaseResult> caseResults = v;
            for(Pair<String, String> pair : testnames) {
                boolean found = false;
                for(CaseResult caseResult : caseResults) {
                    if (caseResult.getName().equals(pair.getKey())) {
                        found = true;
                        break;
                    }
                }
                if(found) {
                    found = false;
                } else {
                    caseResults.add(new CaseResult(pair.getValue(), "NOT RUN", pair.getKey()));
                }
            }
        });

        JSONObject jsonMain = new JSONObject();
        map.forEach((k, v) -> {
            JSONArray tests = new JSONArray();
            tests.addAll(v);
            JSONObject jsonOut = new JSONObject();
            jsonOut.put("testCases", tests);
            jsonMain.put(k, jsonOut);
        });
        return jsonMain;
    }
}
