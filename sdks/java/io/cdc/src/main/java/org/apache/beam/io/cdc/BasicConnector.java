package org.apache.beam.io.cdc;

import org.apache.kafka.connect.source.SourceConnector;

import java.util.HashMap;
import java.util.Map;

public class BasicConnector {
    private Class<?> connectorClass;
    private String username;
    private String password;
    private String host;
    private String port;
    private Map<String,String> connectionProperties;
    private SourceConnector connector;
    private Map<String, String> configuration;

    public Class<?> getConnectorClass() {
        return connectorClass;
    }

    public void setConnectorClass(Class<?> connectorClass) {
        this.connectorClass = connectorClass;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setConnector(SourceConnector connector) {
        this.connector = connector;
    }

    public Map<String,String> getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Map<String,String> connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public Map<String, String> getConfiguration() {
        if(this.configuration != null) {
            return this.configuration;
        }
        HashMap<String,String> configuration = new HashMap<>();

        configuration.computeIfAbsent("connector.class", k -> getConnectorClass().getCanonicalName());
        configuration.computeIfAbsent("database.hostname", k -> getHost());
        configuration.computeIfAbsent("database.port", k -> getPort());
        configuration.computeIfAbsent("database.user", k -> getUsername());
        configuration.computeIfAbsent("database.password", k -> getPassword());

        for (Map.Entry<String, String> entry: getConnectionProperties().entrySet()) {
            configuration.computeIfAbsent(entry.getKey(), k -> entry.getValue());
        }

        this.configuration = configuration;
        return configuration;
    }

    public void initConnector() throws Exception {
        // TODO: create source connectors from source connector name
        if(this.connector == null) {
            this.connector = (SourceConnector) getConnectorClass().getDeclaredConstructor().newInstance();
        }

        this.connector.start(this.getConfiguration());
    }

    public SourceConnector getConnector() {
        return connector;
    }
}
