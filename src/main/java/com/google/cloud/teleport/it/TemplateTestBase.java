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
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.common.IORedirectUtil;
import com.google.cloud.teleport.it.dataflow.ClassicTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowUtils;
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
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

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
  protected static final String HOST_IP = TestProperties.hostIp();

  protected String specPath;
  protected Credentials credentials;
  protected CredentialsProvider credentialsProvider;
  protected String artifactBucketName;
  protected String testId = DataflowUtils.createJobName("");

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
    template = getTemplateAnnotation(annotation, templateClass);
    if (template == null) {
      return;
    }
    if (TestProperties.hasAccessToken()) {
      credentials = TestProperties.googleCredentials();
    } else {
      credentials = buildCredentialsFromEnv();
    }

    // Prefer artifactBucket, but use the staging one if none given
    if (TestProperties.hasArtifactBucket()) {
      artifactBucketName = TestProperties.artifactBucket();
    } else if (TestProperties.hasStageBucket()) {
      artifactBucketName = TestProperties.stageBucket();
    }
    if (artifactBucketName != null) {
      Storage gcsClient = createGcsClient(credentials);
      artifactClient =
          GcsArtifactClient.builder(gcsClient, artifactBucketName, getClass().getSimpleName())
              .build();
    } else {
      LOG.warn(
          "Both -DartifactBucket and -DstageBucket were not given. ArtifactClient will not be"
              + " created automatically.");
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

      File pom = new File("pom.xml").getAbsoluteFile();
      if (!pom.exists()) {
        throw new IllegalArgumentException(
            "To use tests staging templates, please run in the Maven module directory containing"
                + " the template.");
      }

      // Use bucketName unless only artifactBucket is provided
      String bucketName;
      if (TestProperties.hasStageBucket()) {
        bucketName = TestProperties.stageBucket();
      } else if (TestProperties.hasArtifactBucket()) {
        bucketName = TestProperties.artifactBucket();
        LOG.warn(
            "-DstageBucket was not specified, using -DartifactBucket ({}) for stage step",
            bucketName);
      } else {
        throw new IllegalArgumentException(
            "-DstageBucket was not specified, so Template can not be staged. Either give a"
                + " -DspecPath or provide a proper -DstageBucket for automatic staging.");
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

  private Template getTemplateAnnotation(
      TemplateIntegrationTest annotation, Class<?> templateClass) {
    String templateName = annotation.template();
    Template[] templateAnnotations = templateClass.getAnnotationsByType(Template.class);
    if (templateAnnotations.length == 0) {
      LOG.warn(
          "Template mentioned in @TemplateIntegrationTest for {} does not contain a @Template"
              + " annotation, skipping.",
          getClass());
      return null;
    } else if (templateAnnotations.length == 1) {
      return templateAnnotations[0];
    } else if (templateName.isEmpty()) {
      LOG.warn(
          "Template mentioned in @TemplateIntegrationTest for {} contains multiple @Template"
              + " annotations. Please provide templateName field in @TemplateIntegrationTest,"
              + " skipping.",
          getClass());
      return null;
    }
    for (Template template : templateAnnotations) {
      if (template.name().equals(templateName)) {
        return template;
      }
    }
    LOG.warn(
        "templateName does not match any Template annotations. Please recheck"
            + " @TemplateIntegrationTest, skipping");
    return null;
  }

  /**
   * Create the Maven command line used to stage the specific template using the Templates Plugin.
   * It identifies whether the template is v1 (Classic) or v2 (Flex) to setup the Maven reactor
   * accordingly.
   */
  private String[] buildMavenStageCommand(String prefix, File pom, String bucketName) {
    String pomPath = pom.getAbsolutePath();
    String moduleBuild;

    // Classic templates run on parent pom and -pl v1
    if (pomPath.endsWith("v1/pom.xml")) {
      pomPath = new File(pom.getParentFile().getParentFile(), "pom.xml").getAbsolutePath();
      moduleBuild = "it,v1";
    } else if (pomPath.contains("v2/")) {
      // Flex templates run on parent pom and -pl {path-to-folder}
      moduleBuild =
          "it,v2/common," + pomPath.substring(pomPath.indexOf("v2/")).replace("/pom.xml", "");
      pomPath = pomPath.replaceAll("/v2/.*", "/pom.xml");
    } else {
      LOG.warn(
          "Specific module POM was not found, so scanning all modules... Stage step may take a"
              + " little longer.");
      moduleBuild = ".";
    }

    return new String[] {
      "mvn",
      "compile",
      "package",
      "-q",
      "-f",
      pomPath,
      "-pl",
      moduleBuild,
      // Do not make all dependencies every time. Faster but requires prior `mvn install`.
      //      "-am",
      "-PtemplatesStage,pluginOutputDir",
      "-DpluginRunId=" + RandomStringUtils.randomAlphanumeric(0, 20),
      "-DskipShade",
      "-DskipTests",
      "-Dcheckstyle.skip",
      "-Dmdep.analyze.skip",
      "-Dspotless.check.skip",
      "-Denforcer.skip",
      "-DprojectId=" + TestProperties.project(),
      "-Dregion=" + TestProperties.region(),
      "-DbucketName=" + bucketName,
      "-DstagePrefix=" + prefix,
      "-DtemplateName=" + template.name()
    };
  }

  @After
  public void tearDownBase() {
    if (artifactClient != null) {
      artifactClient.cleanupRun();
    }
  }

  protected DataflowClient getDataflowClient() {
    if (template.flexContainerName() != null && !template.flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder().setCredentials(credentials).build();
    } else {
      return ClassicTemplateClient.builder().setCredentials(credentials).build();
    }
  }

  /**
   * Launch the template job with the given options. By default, it will setup the hooks to avoid
   * jobs getting leaked.
   */
  protected JobInfo launchTemplate(LaunchConfig.Builder options) throws IOException {
    return this.launchTemplate(options, true);
  }

  /**
   * Launch the template with the given options and configuration for hook.
   *
   * @param options Options to use for launch.
   * @param setupShutdownHook Whether should setup a hook to cancel the job upon VM termination.
   *     This is useful to teardown resources if the VM/test terminates unexpectedly.
   * @return Job details.
   * @throws IOException Thrown when {@link DataflowClient#launch(String, String, LaunchConfig)}
   *     fails.
   */
  protected JobInfo launchTemplate(LaunchConfig.Builder options, boolean setupShutdownHook)
      throws IOException {

    // Property allows testing with Runner v2 / Unified Worker
    if (System.getProperty("unifiedWorker") != null) {
      options.addEnvironment("experiments", "use_runner_v2");
    }

    DataflowClient dataflowClient = getDataflowClient();
    JobInfo jobInfo = dataflowClient.launch(PROJECT, REGION, options.build());

    // if the launch succeeded and setupShutdownHook is enabled, setup a thread to cancel job
    if (setupShutdownHook && jobInfo.jobId() != null && !jobInfo.jobId().isEmpty()) {
      Runtime.getRuntime()
          .addShutdownHook(new Thread(new CancelJobShutdownHook(dataflowClient, jobInfo)));
    }

    return jobInfo;
  }

  /** Get the Cloud Storage base path for this test suite. */
  protected String getGcsBasePath() {
    return getFullGcsPath(artifactBucketName, getClass().getSimpleName(), artifactClient.runId());
  }

  /** Get the Cloud Storage base path for a specific testing method. */
  protected String getGcsPath(String testMethod) {
    return getFullGcsPath(
        artifactBucketName, getClass().getSimpleName(), artifactClient.runId(), testMethod);
  }

  /** Create the default configuration {@link DataflowOperator.Config} for a specific job info. */
  protected DataflowOperator.Config createConfig(JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  /**
   * Infers the {@link Credentials} to use with Google services from the current environment
   * settings.
   *
   * <p>First, checks if {@link ServiceAccountCredentials#getApplicationDefault()} returns Compute
   * Engine credentials, which means that it is running from a GCE instance and can use the Service
   * Account configured for that VM. Will use that
   *
   * <p>Secondly, it will try to get the environment variable
   * <strong>GOOGLE_APPLICATION_CREDENTIALS</strong>, and use that Service Account if configured to
   * doing so. The method {@link #getCredentialsStream()} will make sure to search for the specific
   * file using both the file system and classpath.
   *
   * <p>If <strong>GOOGLE_APPLICATION_CREDENTIALS</strong> is not configured, it will return the
   * application default, which is often setup through <strong>gcloud auth application-default
   * login</strong>.
   */
  protected static Credentials buildCredentialsFromEnv() throws IOException {

    // if on Compute Engine, return default credentials.
    GoogleCredentials applicationDefault = ServiceAccountCredentials.getApplicationDefault();
    try {
      if (applicationDefault instanceof ComputeEngineCredentials) {
        return applicationDefault;
      }
    } catch (Exception e) {
      // no problem
    }

    InputStream credentialsStream = getCredentialsStream();
    if (credentialsStream == null) {
      return applicationDefault;
    }
    return ServiceAccountCredentials.fromStream(credentialsStream);
  }

  protected static InputStream getCredentialsStream() throws FileNotFoundException {
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
      LOG.warn("Not found credentials with file name {}", credentialFile);
      return null;
    }
    return is;
  }

  /**
   * Convert a BigQuery TableId to a table spec string.
   *
   * @param table TableId to format.
   * @return String in the format {project}:{dataset}.{table}.
   */
  protected String toTableSpec(TableId table) {
    return String.format(
        "%s:%s.%s",
        table.getProject() != null ? table.getProject() : PROJECT,
        table.getDataset(),
        table.getTable());
  }

  /**
   * This {@link Runnable} class calls {@link DataflowClient#cancelJob(String, String, String)} for
   * a specific instance of client and given job information, which is useful to enforcing resource
   * termination using {@link Runtime#addShutdownHook(Thread)}.
   */
  static class CancelJobShutdownHook implements Runnable {

    private final DataflowClient dataflowClient;
    private final JobInfo jobInfo;

    public CancelJobShutdownHook(DataflowClient dataflowClient, JobInfo jobInfo) {
      this.dataflowClient = dataflowClient;
      this.jobInfo = jobInfo;
    }

    @Override
    public void run() {
      try {
        dataflowClient.cancelJob(jobInfo.projectId(), jobInfo.region(), jobInfo.jobId());
      } catch (Exception e) {
        LOG.info(
            "[CancelJobShutdownHook] Error shutting down job {}: {}",
            jobInfo.jobId(),
            e.getMessage());
      }
    }
  }
}
