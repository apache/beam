package org.apache.beam.validate.runner.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SuiteResult {

    public SuiteResult(List<CaseResult> cases) {
        this.cases = cases;
    }

    public SuiteResult() {

    }

    public List<CaseResult> getCases() {
        return cases;
    }

    public void setCases(List<CaseResult> cases) {
        this.cases = cases;
    }

    private List<CaseResult> cases;
}
