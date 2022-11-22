/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Templates. It wraps around tests that extend it to stage the Templates when
 * <strong>-DspecPath</strong> isn't provided.
 *
 * <p>It is required to use @TemplateIntegrationTest to specify which template is under test.
 */
public abstract class TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TemplateTestBase.class);

  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();

  protected String specPath;
  protected Credentials credentials;
  protected CredentialsProvider credentialsProvider;

  /** Cache to avoid staging the same template multiple times on the same execution. */
  private static final Map<String, String> stagedTemplates = new HashMap<>();

  @Before
  public void setUpBase() throws IOException {
    TemplateIntegrationTest annotation = getClass().getAnnotation(TemplateIntegrationTest.class);
    if (annotation == null) {
      LOG.warn(
          "{} did not specify which template is tested using @TemplateIntegrationTest, skipping.",
          getClass());
      return;
    }

    Class<?> templateClass = annotation.value();
    Template template = templateClass.getAnnotation(Template.class);

    if (TestProperties.hasAccessToken()) {
      credentials = TestProperties.googleCredentials();
    } else {
      credentials = buildCredentialsFromEnv();
    }

    credentialsProvider = FixedCredentialsProvider.create(credentials);

    if (TestProperties.specPath() != null && !TestProperties.specPath().isEmpty()) {
      LOG.info("A spec path was given, not staging template {}", template.name());
      specPath = TestProperties.specPath();
    } else if (stagedTemplates.containsKey(template.name())) {
      specPath = stagedTemplates.get(template.name());
    } else {
      LOG.info("Preparing test for {} ({})", template.name(), templateClass);

      String prefix = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()) + "_IT";

      File pom = new File("unified-templates.xml");
      if (!pom.exists()) {
        pom = new File("pom.xml");
      }
      if (!pom.exists()) {
        throw new IllegalArgumentException(
            "To use tests staging templates, please run in the Maven module directory containing"
                + " the template.");
      }
      String bucketName = TestProperties.stageBucket();
      if (bucketName == null || bucketName.isEmpty()) {
        bucketName = TestProperties.artifactBucket();
      }

      String[] mavenCmd =
          new String[] {
            "mvn",
            "package",
            "-q",
            "-f",
            pom.getAbsolutePath(),
            "-PtemplatesStage",
            "-DskipTests",
            "-Dcheckstyle.skip",
            "-Dmdep.analyze.skip",
            "-Dspotless.check.skip",
            "-DprojectId=" + TestProperties.project(),
            "-DbucketName=" + bucketName,
            "-DstagePrefix=" + prefix,
            "-DtemplateName=" + template.name()
          };

      LOG.info("Running command to stage templates: {}", String.join(" ", mavenCmd));
      try {
        Process exec = Runtime.getRuntime().exec(mavenCmd);
        int ret = exec.waitFor();

        if (ret == 0) {

          boolean flex =
              template.flexContainerName() != null && !template.flexContainerName().isEmpty();
          specPath =
              String.format(
                  "gs://%s/%s/%s%s", bucketName, prefix, flex ? "flex/" : "", template.name());
          LOG.info("Template staged successfully! Path: {}", specPath);

          stagedTemplates.put(template.name(), specPath);
        } else {

          throw new RuntimeException(
              "Error staging template: "
                  + new String(exec.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                  + "\n"
                  + new String(exec.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
        }

      } catch (Exception e) {
        throw new IllegalArgumentException("Error staging template", e);
      }
    }
  }

  @After
  public void tearDownBase() {}

  public static Credentials buildCredentialsFromEnv() throws IOException {

    // if on Compute Engine, return default credentials.
    try {
      if (ServiceAccountCredentials.getApplicationDefault() instanceof ComputeEngineCredentials) {
        return ServiceAccountCredentials.getApplicationDefault();
      }
    } catch (Exception e) {
      // no problem
    }

    InputStream credentialsStream = getCredentialsStream();
    if (credentialsStream == null) {
      return ServiceAccountCredentials.getApplicationDefault();
    }
    return ServiceAccountCredentials.fromStream(credentialsStream);
  }

  public static InputStream getCredentialsStream() throws FileNotFoundException {
    String credentialFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

    if (credentialFile == null || credentialFile.isEmpty()) {
      LOG.warn(
          "Not found Google Cloud credentials: GOOGLE_APPLICATION_CREDENTIALS, assuming application"
              + " default");
      return null;
    }

    InputStream is = null;

    File credentialFileRead = new File(credentialFile);
    if (credentialFileRead.exists()) {
      is = new FileInputStream(credentialFile);
    }

    if (is == null) {
      is = TemplateTestBase.class.getResourceAsStream(credentialFile);
    }

    if (is == null) {
      is = TemplateTestBase.class.getResourceAsStream("/" + credentialFile);
    }

    if (is == null) {
      LOG.warn("Not found credentials with file name " + credentialFile);
      return null;
    }
    return is;
  }
}
