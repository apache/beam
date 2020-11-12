package org.apache.beam.validate.runner.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TestResult {

    public List<SuiteResult> getSuites() {
        return suites;
    }

    public void setSuites(List<SuiteResult> suites) {
        this.suites = suites;
    }

    public TestResult(List<SuiteResult> suites) {
        this.suites = suites;
    }

    private List<SuiteResult> suites;
    public TestResult() {

    }
}
