package org.apache.beam.validate.runner.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CaseResult {
    private String className;
    private String name;
    private  String status;

    public CaseResult(String className, String name, String status) {
        this.className = className;
        this.name = name;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CaseResult that = (CaseResult) o;
        return className.equals(that.className) &&
                status.equals(that.status) &&
                name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, status, name);
    }
}
