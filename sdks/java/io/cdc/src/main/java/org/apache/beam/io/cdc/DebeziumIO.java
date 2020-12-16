/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.io.cdc;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.kafka.connect.source.SourceConnector;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class DebeziumIO {
	private static final Logger LOG = LoggerFactory.getLogger(DebeziumIO.class);
	
	/**
	 * Read data from a Debezium source.
	 *
	 * @param <T> Type of the data to be read.
	*/
	public static <T> Read<T> read(){
		return new AutoValue_DebeziumIO_Read.Builder<T>().build();
	}
	
	/** Disallow construction of utility class. */
	private DebeziumIO() {}
	
	/** Implementation of {@link #read}. */
	@AutoValue
	public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

		private static final long serialVersionUID = 1L;

		abstract @Nullable ConnectorConfiguration getConnectorConfiguration();
		abstract @Nullable SourceRecordMapper<T> getFormatFunction();
		abstract @Nullable Coder<T> getCoder();
		abstract Builder<T> toBuilder();
		  
		@AutoValue.Builder
		abstract static class Builder<T> {
			abstract Builder<T> setConnectorConfiguration(ConnectorConfiguration config);
			abstract Builder<T> setCoder(Coder<T> coder);
			abstract Builder<T> setFormatFunction(SourceRecordMapper<T> mapperFn);
			abstract Read<T> build();
			  
		}

		public Read<T> withConnectorConfiguration(final ConnectorConfiguration config) {
			checkArgument(config != null, "config can not be null");
			return toBuilder().setConnectorConfiguration(config).build();
		}
		
		public Read<T> withFormatFunction(SourceRecordMapper<T> mapperFn) {
			checkArgument(mapperFn != null, "mapperFn can not be null");
			return toBuilder().setFormatFunction(mapperFn).build();
		}
		  
		public Read<T> withCoder(Coder<T> coder) {
			checkArgument(coder != null, "coder can not be null");
		    return toBuilder().setCoder(coder).build();
		}
		
		@Override
		public PCollection<T> expand(PBegin input) {
			return input
					.apply(Create.of(Lists.newArrayList(getConnectorConfiguration().getConfigurationMap()))
							.withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
					.apply(ParDo.of(new KafkaSourceConsumerFn<>(
									getConnectorConfiguration().getConnectorClass().get(),
									getFormatFunction())));
		}
		  
	}

	/**
	 * A POJO describing a Debezium configuration.
	*/
	@AutoValue
	public abstract static class ConnectorConfiguration implements Serializable {
		private static final long serialVersionUID = 1L;
		  
		abstract @Nullable ValueProvider<Class<?>> getConnectorClass();
		abstract @Nullable ValueProvider<String> getHostName();
		abstract @Nullable ValueProvider<String> getPort();
		abstract @Nullable ValueProvider<String> getUsername();
		abstract @Nullable ValueProvider<String> getPassword();
		abstract @Nullable ValueProvider<SourceConnector> getSourceConnector();
		abstract @Nullable ValueProvider<Map<String,String>> getConnectionProperties();
		abstract Builder builder();
		
		@AutoValue.Builder
		abstract static class Builder {
			abstract Builder setConnectorClass(ValueProvider<Class<?>> connectorClass);
			abstract Builder setHostName(ValueProvider<String> hostname);
			abstract Builder setPort(ValueProvider<String> port);
			abstract Builder setUsername(ValueProvider<String> username);
			abstract Builder setPassword(ValueProvider<String> password);
			abstract Builder setConnectionProperties(ValueProvider<Map<String,String>> connectionProperties);
			abstract Builder setSourceConnector(ValueProvider<SourceConnector> sourceConnector);
			abstract ConnectorConfiguration build();
		      
		}
		
		public static ConnectorConfiguration create() {
		    return new AutoValue_DebeziumIO_ConnectorConfiguration.Builder()
		    		.setConnectionProperties(ValueProvider.StaticValueProvider.of(new HashMap<>()))
		    		.build();
		}

		public ConnectorConfiguration withConnectorClass(Class<?> connectorClass) {
			checkArgument(connectorClass != null, "connectorClass can not be null");
			return withConnectorClass(ValueProvider.StaticValueProvider.of(connectorClass));
		}
		
		public ConnectorConfiguration withConnectorClass(ValueProvider<Class<?>> connectorClass) {
			checkArgument(connectorClass != null, "connectorClass can not be null");
			return builder().setConnectorClass(connectorClass).build();
		}

		public ConnectorConfiguration withHostName(String hostName) {
			checkArgument(hostName != null, "hostName can not be null");
			return withHostName(ValueProvider.StaticValueProvider.of(hostName));
		}
		
		public ConnectorConfiguration withHostName(ValueProvider<String> hostName) {
			checkArgument(hostName != null, "hostName can not be null");
			return builder().setHostName(hostName).build();
		}

		public ConnectorConfiguration withPort(String port) {
			checkArgument(port != null, "port can not be null");
			return withPort(ValueProvider.StaticValueProvider.of(port));
		}
		
		public ConnectorConfiguration withPort(ValueProvider<String> port) {
			checkArgument(port != null, "port can not be null");
			return builder().setPort(port).build();
		}

		public ConnectorConfiguration withUsername(String username) {
			checkArgument(username != null, "username can not be null");
			return withUsername(ValueProvider.StaticValueProvider.of(username));
	    }

	    public ConnectorConfiguration withUsername(ValueProvider<String> username) {
	    	checkArgument(username != null, "username can not be null");
	    	return builder().setUsername(username).build();
	    }

	    public ConnectorConfiguration withPassword(String password) {
	    	checkArgument(password != null, "password can not be null");
	    	return withPassword(ValueProvider.StaticValueProvider.of(password));
	    }

	    public ConnectorConfiguration withPassword(ValueProvider<String> password) {
	    	checkArgument(password != null, "password can not be null");
	    	return builder().setPassword(password).build();
	    }

	    public ConnectorConfiguration withConnectionProperties(Map<String,String> connectionProperties) {
	    	checkArgument(connectionProperties != null, "connectionProperties can not be null");
	    	return withConnectionProperties(ValueProvider.StaticValueProvider.of(connectionProperties));
	    }

	    public ConnectorConfiguration withConnectionProperties(ValueProvider<Map<String,String>> connectionProperties) {
	    	checkArgument(connectionProperties != null, "connectionProperties can not be null");
	    	return builder().setConnectionProperties(connectionProperties).build();
	    }

	    public ConnectorConfiguration withConnectionProperty(String key, String value) {
	    	checkArgument(key != null, "key can not be null");
	    	checkArgument(value != null, "value can not be null");
			checkArgument(getConnectionProperties().get() != null, "connectionProperties can not be null");

	    	ConnectorConfiguration config = builder().build();
	    	config.getConnectionProperties().get().putIfAbsent(key, value);
	    	return config;
	    }

	    public ConnectorConfiguration withSourceConnector(SourceConnector sourceConnector) {
	    	checkArgument(sourceConnector != null, "sourceConnector can not be null");
	    	return withSourceConnector(ValueProvider.StaticValueProvider.of(sourceConnector));
	    }

	    public ConnectorConfiguration withSourceConnector(ValueProvider<SourceConnector> sourceConnector) {
	    	checkArgument(sourceConnector != null, "sourceConnector can not be null");
	    	return builder().setSourceConnector(sourceConnector).build();
	    }
		
	    /**
	     * 
	     * @return Configuration Map.
	     */
		public Map<String, String> getConfigurationMap() {
	        HashMap<String,String> configuration = new HashMap<>();
	        
	        configuration.computeIfAbsent("connector.class", k -> getConnectorClass().get().getCanonicalName());
	        configuration.computeIfAbsent("database.hostname", k -> getHostName().get());
	        configuration.computeIfAbsent("database.port", k -> getPort().get());
	        configuration.computeIfAbsent("database.user", k -> getUsername().get());
	        configuration.computeIfAbsent("database.password", k -> getPassword().get());

	        for (Map.Entry<String, String> entry: getConnectionProperties().get().entrySet()) {
	            configuration.computeIfAbsent(entry.getKey(), k -> entry.getValue());
	        }

	        // Set default Database History impl. if not provided
			configuration.computeIfAbsent("database.history", k -> DebeziumSDFDatabaseHistory.class.getName());
	        
	        String stringProperties = Joiner.on('\n').withKeyValueSeparator(" -> ").join(configuration);
	        LOG.info("---------------- Connector configuration: {}", stringProperties);
	        
	        return configuration;
	    }
	}
}
