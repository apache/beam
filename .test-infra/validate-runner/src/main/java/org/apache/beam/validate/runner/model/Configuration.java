package org.apache.beam.validate.runner.model;

import java.util.List;
import java.util.Map;

public class Configuration {
    private List<Map<String, String>> batch;
    private List<Map<String, String>> stream;
    private String server;
    private String jsonapi;

    public List<Map<String, String>> getBatch() {
        return batch;
    }

    public void setBatch(List<Map<String, String>> batch) {
        this.batch = batch;
    }

    public List<Map<String, String>> getStream() {
        return stream;
    }

    public void setStream(List<Map<String, String>> stream) {
        this.stream = stream;
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
