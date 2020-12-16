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

import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.GsonBuilder;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SourceRecordJson {
    private final SourceRecord sourceRecord;
    private final Struct value;
    private final Event event;

    public SourceRecordJson(SourceRecord sourceRecord) {
        if (sourceRecord == null) {
            throw new IllegalArgumentException();
        }

        this.sourceRecord = sourceRecord;
        this.value = (Struct) sourceRecord.value();

        Metadata metadata = this.loadMetadata();
        Before before = this.loadBefore();
        After after = this.loadAfter();

        this.event = new Event(metadata, before, after);
    }

    private Metadata loadMetadata() {
        Struct source;
        try {
            source = (Struct) this.value.get("source");
        } catch (RuntimeException e) {
            throw new IllegalArgumentException();
        }
        String schema;

        if(source == null) {
            return null;
        }

        try {
            // PostgreSQL and SQL server use Schema
            schema = source.getString("schema");
        } catch (DataException e) {
            // MySQL uses file instead
            schema = source.getString("file");
        }

        return new Metadata(source.getString("connector"),
                source.getString("version"),
                source.getString("name"),
                source.getString("db"),
                schema,
                source.getString("table"));
    }

    private Before loadBefore() {
        Struct before;
        try {
            before = (Struct) this.value.get("before");
        } catch (DataException e) {
            return null;
        }
        if(before == null) {
            return null;
        }

        Map<String, Object> fields = new HashMap<>();
        for(Field field: before.schema().fields()) {
            fields.put(field.name(), before.get(field));
        }

        return new Before(fields);
    }

    private After loadAfter() {
        Struct after;
        try {
            after = (Struct) this.value.get("after");
        } catch (DataException e) {
            return null;
        }
        if(after == null) {
            return null;
        }

        Map<String, Object> fields = new HashMap<>();
        for(Field field: after.schema().fields()) {
            fields.put(field.name(), after.get(field));
        }

        return new After(fields);
    }

    public String toJson() {
        return this.event.toJson();
    }

    public static class SourceRecordJsonMapper implements SourceRecordMapper<String> {

        @Override
        public String mapSourceRecord(SourceRecord sourceRecord) throws Exception {
            return new SourceRecordJson(sourceRecord).toJson();
        }
    }
}

class Event implements Serializable {
    private final Metadata metadata;
    private final Before before;
    private final After after;

    public Event(Metadata metadata, Before before, After after) {
        this.metadata = metadata;
        this.before = before;
        this.after = after;
    }

    public String toJson() {
        Gson gson = new GsonBuilder().serializeNulls().create();
        return gson.toJson(this);
    }
}

class Metadata implements Serializable {
    private final String connector;
    private final String version;
    private final String name;
    private final String database;
    private final String schema;
    private final String table;

    public Metadata(String connector, String version, String name, String database, String schema, String table) {
        this.connector = connector;
        this.version = version;
        this.name = name;
        this.database = database;
        this.schema = schema;
        this.table = table;
    }
}

class Before implements Serializable {
    private final Map<String, Object> fields;

    public Before(Map<String, Object> fields) {
        this.fields = fields;
    }
}

class After implements Serializable {
    private final Map<String, Object> fields;

    public After(Map<String, Object> fields) {
        this.fields = fields;
    }
}
