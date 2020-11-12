package org.apache.beam.validate.runner.model;

public class Configuration {
    private String[] jobs;
    private String server;
    private String jsonapi;

    public String[] getJobs() {
        return jobs;
    }

    public void setJobs(String[] jobs) {
        this.jobs = jobs;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getJsonapi() {
        return jsonapi;
    }

    public void setJsonapi(String jsonapi) {
        this.jsonapi = jsonapi;
    }
}
