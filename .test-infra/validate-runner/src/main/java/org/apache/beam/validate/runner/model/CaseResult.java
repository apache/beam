package org.apache.beam.validate.runner.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CaseResult {
    private String className;
    private  String status;

    public CaseResult(String className, String status) {
        this.className = className;
        this.status = status;
    }

    public CaseResult() {

    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
