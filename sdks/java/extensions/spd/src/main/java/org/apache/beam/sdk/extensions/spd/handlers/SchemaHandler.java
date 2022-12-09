package org.apache.beam.sdk.extensions.spd.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.extensions.spd.NodeHandler;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;
import org.apache.beam.sdk.schemas.Schema;

public class SchemaHandler implements NodeHandler {

    private String name = null;
    private Schema schema = null;

    @Override
    public String tagName() {
        return "schema";
    }

    @Override
    public void internalVisit(JsonNode node, StructuredPipelineDescription description) throws Exception {

    }


}
