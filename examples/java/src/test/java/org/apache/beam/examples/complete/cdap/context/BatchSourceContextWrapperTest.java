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
package org.apache.beam.examples.complete.cdap.context;

import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import org.apache.beam.examples.complete.cdap.ConfigWrapper;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.sql.Timestamp;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test class for {@link BatchSourceContextWrapper}.
 */
public class BatchSourceContextWrapperTest {
    private static final ImmutableMap<String, Object> TEST_SALESFORCE_PARAMS_MAP =
            ImmutableMap.<String, java.lang.Object>builder()
                    .put("sObjectName", "sObject")
                    .put("datetimeAfter", "datetime")
                    .put("consumerKey", "key")
                    .put("consumerSecret", "secret")
                    .put("username", "user")
                    .put("password", "password")
                    .put("loginUrl", "https://www.google.com")
                    .put("referenceName", "oldReference")
                    .build();


    @Test
    public void getFailureCollector() {
        /** arrange */
        BatchSourceContext context = new BatchSourceContextWrapper();

        String newReferenceName = "new reference name";
        SalesforceSourceConfig config = new ConfigWrapper<>(SalesforceSourceConfig.class)
                .withParams(TEST_SALESFORCE_PARAMS_MAP)
                .setParam("referenceName", newReferenceName)
                .build();

        SalesforceBatchSource salesforceBatchSource = new SalesforceBatchSource(config);

        /** act && assert */
        ValidationException e = assertThrows(ValidationException.class, () -> salesforceBatchSource.prepareRun(context));
        List<ValidationFailure> failures = e.getFailures();
        assertEquals(1, failures.size());
        assertEquals("Error encountered while establishing connection: Connection to salesforce with plugin configurations failed", failures.get(0).getMessage());
    }

    @Test
    public void getLogicalStartTime() {
        /** arrange */
        BatchSourceContext context = new BatchSourceContextWrapper();
        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        /** act && assert */
        // Using a range of 100 milliseconds between the creation of 2 objects  to check the correct work of the method
        assertTrue(startTime.getTime() - context.getLogicalStartTime() <= 100);
    }
}
