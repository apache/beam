package org.apache.beam.validate.runner.service;

import com.offbytwo.jenkins.JenkinsServer;
import com.offbytwo.jenkins.model.Job;
import com.offbytwo.jenkins.model.JobWithDetails;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.beam.validate.runner.model.CaseResult;
import org.apache.beam.validate.runner.model.Configuration;
import org.apache.beam.validate.runner.model.TestResult;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TestService {

    default List<String> getAllTests(TestResult testResult) {
        List<CaseResult> caseResults = new ArrayList<>();
        Optional.ofNullable(testResult.getSuites()).ifPresent(suites -> suites.forEach(item -> caseResults.addAll(item.getCases())));
        List<String> tests = new ArrayList<>();
        Optional.ofNullable(caseResults).ifPresent(cases -> cases.forEach(item -> tests.add(item.getClassName() + "." + item.getName() + " : " + item.getStatus())));
        return tests;
    }

    default URL getUrl(Map<String, String> job, Configuration configuration) throws URISyntaxException, IOException {
        Map<String, Job> jobs = new JenkinsServer(new URI(configuration.getServer())).getJobs();
        JobWithDetails jobWithDetails = jobs.get(job.get(job.keySet().toArray()[0])).details();
        return new URL(jobWithDetails.getLastSuccessfulBuild().getUrl() + configuration.getJsonapi());
    }

    default JSONObject getBatchObject(Map<String, String> job, TestResult result) {
        JSONArray tests = new JSONArray();
        tests.addAll(getAllTests(result));

        JSONObject jsonOut = new JSONObject();
        jsonOut.put("testCases", tests);
        JSONObject jsonMain = new JSONObject();
        jsonMain.put(job.keySet().toArray()[0], jsonOut);

        return jsonMain;
    }
}
