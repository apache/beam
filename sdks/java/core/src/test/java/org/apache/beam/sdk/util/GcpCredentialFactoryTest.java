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
package org.apache.beam.sdk.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcpCredentialFactory}. */
@RunWith(JUnit4.class)
public class GcpCredentialFactoryTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testP12KeyFileNeedsAccountName() throws Exception {
    GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
    options.setServiceAccountKeyfile("key.p12");

    thrown.expect(IOException.class);
    thrown.expectMessage(new StringContains("You need an accountName"));
    GcpCredentialFactory.fromOptions(options).getCredential();
  }

  @Test
  public void testJSONKeyFileDoesntAllowAccountName() throws Exception {
    GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
    options.setServiceAccountKeyfile("key.json");
    options.setServiceAccountName("test@gcloud");

    thrown.expect(IOException.class);
    thrown.expectMessage(new StringContains("Only use an accountName"));
    GcpCredentialFactory.fromOptions(options).getCredential();
  }

  @Test
  public void testAccountNameWithoutKeyFile() throws Exception {
    GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
    options.setServiceAccountName("test@gcloud");

    thrown.expect(IOException.class);
    thrown.expectMessage(new StringContains("also supply a keyFile"));
    GcpCredentialFactory.fromOptions(options).getCredential();
  }

  @Test
  public void testCorrectP12() throws Exception {
    GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
    options.setServiceAccountKeyfile("key.p12");
    options.setServiceAccountName("test@gcloud");

    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage(new StringContains("No such file or directory"));
    GcpCredentialFactory.fromOptions(options).getCredential();
  }

  @Test
  public void testCorrectJSON() throws Exception {
    GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
    options.setServiceAccountKeyfile("key.json");

    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage(new StringContains("No such file or directory"));
    GcpCredentialFactory.fromOptions(options).getCredential();
  }
}
