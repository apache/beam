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

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.common.IORedirectUtil;
import com.google.cloud.teleport.it.dataflow.ClassicTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
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

  @Rule public final TestName testName = new TestName();

  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();
  protected static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();

  protected String specPath;
  protected Credentials credentials;
  protected CredentialsProvider credentialsProvider;

  /** Cache to avoid staging the same template multiple times on the same execution. */
  private static final Map<String, String> stagedTemplates = new HashMap<>();

  protected Template template;
  protected GcsArtifactClient artifactClient;

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
    template = templateClass.getAnnotation(Template.class);

    if (TestProperties.hasAccessToken()) {
      credentials = TestProperties.googleCredentials();
    } else {
      credentials = buildCredentialsFromEnv();
    }

    Storage gcsClient = createGcsClient(credentials);
    artifactClient =
        GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, getClass().getSimpleName()).build();

    credentialsProvider = FixedCredentialsProvider.create(credentials);

    if (TestProperties.specPath() != null && !TestProperties.specPath().isEmpty()) {
      LOG.info("A spec path was given, not staging template {}", template.name());
      specPath = TestProperties.specPath();
    } else if (stagedTemplates.containsKey(template.name())) {
      specPath = stagedTemplates.get(template.name());
    } else {
      LOG.info("Preparing test for {} ({})", template.name(), templateClass);

      String prefix = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()) + "_IT";

      File pom = new File("pom.xml").getAbsoluteFile();
      if (!pom.exists()) {
        throw new IllegalArgumentException(
            "To use tests staging templates, please run in the Maven module directory containing"
                + " the template.");
      }
      String bucketName = TestProperties.stageBucket();
      if (bucketName == null || bucketName.isEmpty()) {
        bucketName = TestProperties.artifactBucket();
      }

      String[] mavenCmd = buildMavenStageCommand(prefix, pom, bucketName);
      LOG.info("Running command to stage templates: {}", String.join(" ", mavenCmd));

      try {
        Process exec = Runtime.getRuntime().exec(mavenCmd);
        IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
        IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

        if (exec.waitFor() != 0) {
          throw new RuntimeException("Error staging template, check Maven logs.");
        }

        boolean flex =
            template.flexContainerName() != null && !template.flexContainerName().isEmpty();
        specPath =
            String.format(
                "gs://%s/%s/%s%s", bucketName, prefix, flex ? "flex/" : "", template.name());
        LOG.info("Template staged successfully! Path: {}", specPath);

        stagedTemplates.put(template.name(), specPath);

      } catch (Exception e) {
        throw new IllegalArgumentException("Error staging template", e);
      }
    }
  }

  private String[] buildMavenStageCommand(String prefix, File pom, String bucketName) {
    String pomPath = pom.getAbsolutePath();
    String moduleBuild;

    // Classic templates run on parent pom and -pl v1
    if (pomPath.endsWith("/v1/pom.xml")) {
      pomPath = new File(pom.getParentFile().getParentFile(), "pom.xml").getAbsolutePath();
      moduleBuild = "v1";
    } else {
      // Flex templates run on parent pom and -pl {path-to-folder}
      moduleBuild = pomPath.substring(pomPath.indexOf("v2/")).replace("/pom.xml", "");
      pomPath = pomPath.replaceAll("/v2/.*", "/pom.xml");
    }

    return new String[] {
      "mvn",
      "package",
      "-q",
      "-f",
      pomPath,
      "-pl",
      moduleBuild,
      "-am",
      "-PtemplatesStage",
      "-DskipShade",
      "-Dmaven.test.skip",
      "-Dcheckstyle.skip",
      "-Dmdep.analyze.skip",
      "-Dspotless.check.skip",
      "-Denforcer.skip",
      "-DprojectId=" + TestProperties.project(),
      "-DbucketName=" + bucketName,
      "-DstagePrefix=" + prefix,
      "-DtemplateName=" + template.name()
    };
  }

  @After
  public void tearDownBase() {
    artifactClient.cleanupRun();
  }

  protected DataflowTemplateClient getDataflowClient() {
    if (template.flexContainerName() != null && !template.flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder().setCredentials(credentials).build();
    } else {
      return ClassicTemplateClient.builder().setCredentials(credentials).build();
    }
  }

  protected JobInfo launchTemplate(LaunchConfig.Builder options) throws IOException {

    // Property allows testing with Runner v2 / Unified Worker
    if (System.getProperty("unifiedWorker") != null) {
      options.addEnvironment("experiments", "use_runner_v2");
    }

    return getDataflowClient().launchTemplate(PROJECT, REGION, options.build());
  }

  protected String getGcsBasePath() {
    return getFullGcsPath(ARTIFACT_BUCKET, getClass().getSimpleName(), artifactClient.runId());
  }

  protected String getGcsPath(String testMethod) {
    return getFullGcsPath(
        ARTIFACT_BUCKET, getClass().getSimpleName(), artifactClient.runId(), testMethod);
  }

  protected DataflowOperator.Config createConfig(JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

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
