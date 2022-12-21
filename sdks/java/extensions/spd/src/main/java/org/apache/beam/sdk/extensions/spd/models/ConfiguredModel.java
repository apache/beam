package org.apache.beam.sdk.extensions.spd.models;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ConfiguredModel {
    private static final Logger LOG = LoggerFactory.getLogger(ConfiguredModel.class);

    public static String MATERIALIZED_CONFIG = "materialized";
    public static String SQL_HEADER_CONFIG = "sql_header";
    public static String ENABLED_CONFIG = "enabled";
    public static String TAGS_CONFIG = "tags";
    public static String PRE_HOOK_CONFIG = "pre-hook";
    public static String POST_HOOK_CONFIG = "post-hook";
    public static String DATABASE_CONFIG = "database";
    public static String SCHEMA_CONFIG = "schema";
    public static String ALIAS_CONFIG = "alias";
    public static String PERSIST_DOCS_CONFIG = "persis_docs";
    public static String FULL_REFRESH_CONFIG = "full_refresh";
    public static String META_CONFIG = "meta";
    public static String GRANTS_CONFIG = "grants";

    Map<String,Object> config;

    public ConfiguredModel() {
        config = new HashMap<>();
    }

    public void mergeConfiguration(ObjectNode newConfig) {
        Iterator<String> fields = newConfig.fieldNames();
        while(fields.hasNext()) {
            String field = fields.next();
            // Configuration entries always start with a "+" sign
            if(field == null || !field.startsWith("+")) {
                continue;
            }
            // Chop off the first character
            String name = field.substring(1);
            LOG.info("Found config field "+name);
            if(MATERIALIZED_CONFIG.equals(name)) {
                config.put(MATERIALIZED_CONFIG,newConfig.get(field).asText());
            } else if(SQL_HEADER_CONFIG.equals(name)) {
                config.put(SQL_HEADER_CONFIG,newConfig.get(field).asText());
            } else if(ENABLED_CONFIG.equals(name)) {
                config.put(ENABLED_CONFIG,newConfig.get(field).asBoolean());
            } else if(TAGS_CONFIG.equals(name)) {
                LOG.warn("Setting "+TAGS_CONFIG+" not supported.");
            } else if(PRE_HOOK_CONFIG.equals(name)) {
                LOG.warn("Setting "+PRE_HOOK_CONFIG+" not supported.");
            } else if(POST_HOOK_CONFIG.equals(name)) {
                LOG.warn("Setting "+POST_HOOK_CONFIG+" not supported.");
            } else if(DATABASE_CONFIG.equals(name)) {
                config.put(DATABASE_CONFIG,newConfig.get(field).asText());
            } else if(SCHEMA_CONFIG.equals(name)) {
                config.put(SCHEMA_CONFIG,newConfig.get(field).asText());
            } else if(ALIAS_CONFIG.equals(name)) {
                config.put(ALIAS_CONFIG,newConfig.get(field).asText());
            } else if(PERSIST_DOCS_CONFIG.equals(name)) {
                LOG.warn("Setting "+PERSIST_DOCS_CONFIG+" not supported.");
            } else if(FULL_REFRESH_CONFIG.equals(name)) {
                config.put(FULL_REFRESH_CONFIG,newConfig.get(field).asBoolean());
            } else if(META_CONFIG.equals(name)) {
                LOG.warn("Setting "+META_CONFIG+" not supported.");
            } else if(GRANTS_CONFIG.equals(name)) {
                LOG.warn("Setting "+GRANTS_CONFIG+" not supported.");
            }
        }
    }

    public Map<String,Object> getConfig() { return config; }

}
