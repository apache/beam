package com.google.cloud.cassandra.dataflow.io;

import java.io.Serializable;

/*
 * Configuration class that holds the details required for cassandra connection
 */
public class CassandraReadConfiguration implements Serializable {
	private static final long serialVersionUID = 1L;
	private String[] host;
	private String keypace;
	private int port;
	private String table;
	private String query;
	private String rowKey;
	private Class entityName;
	private int jmxPort;

	CassandraReadConfiguration(String[] hosts, String keyspace, int port,
			String table,String query,String rowKey,Class entityName,int jmxport) {
		this.host = hosts;
		this.keypace = keyspace;
		this.port = port;
		this.table = table;
		this.query = query;
		this.rowKey = rowKey;
		this.entityName = entityName;
		this.jmxPort = jmxPort;
	}

	public String[] getHost() {
		return host;
	}

	public void setHost(String[] host) {
		this.host = host;
	}

	public String getKeypace() {
		return keypace;
	}

	public void setKeypace(String keypace) {
		this.keypace = keypace;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
	
	public String getQuery() {
		return query;
    }

	public void setQuery(String query) {
		this.query = query;
    }

	public String getRowKey() {
		if(!rowKey.isEmpty()){
		return rowKey;
		}
		else{
			return null;	
		}
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public Class get_entityName() {
		return entityName;
	}

	public void set_entityName(Class _entityName) {
		this.entityName = _entityName;
	}

	public int getJmxPort() {
		return jmxPort;
	}

	public void setJmxPort(int jmxHost) {
		this.jmxPort = jmxHost;
	}
}
