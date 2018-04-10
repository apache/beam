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
package org.apache.beam.runners.apex;

import static com.google.common.base.Preconditions.checkArgument;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.api.Launcher.LauncherException;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.apex.api.YarnAppLauncher;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy to launch the YARN application through the hadoop script to run in the
 * pre-configured environment (class path, configuration, native libraries etc.).
 *
 * <p>The proxy takes the DAG and communicates with the Hadoop services to launch
 * it on the cluster.
 */
public class ApexYarnLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(ApexYarnLauncher.class);

  public AppHandle launchApp(StreamingApplication app, Properties configProperties)
      throws IOException {

    List<File> jarsToShip = getYarnDeployDependencies();
    StringBuilder classpath = new StringBuilder();
    for (File path : jarsToShip) {
      if (path.isDirectory()) {
        File tmpJar = File.createTempFile("beam-runners-apex-", ".jar");
        createJar(path, tmpJar);
        tmpJar.deleteOnExit();
        path = tmpJar;
      }
      if (classpath.length() != 0) {
        classpath.append(':');
      }
      classpath.append(path.getAbsolutePath());
    }

    EmbeddedAppLauncher<?> embeddedLauncher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    DAG dag = embeddedLauncher.getDAG();
    app.populateDAG(dag, new Configuration(false));

    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(YarnAppLauncher.LIB_JARS, classpath.toString().replace(':', ','));
    LaunchParams lp = new LaunchParams(dag, launchAttributes, configProperties);
    lp.cmd = "hadoop " + ApexYarnLauncher.class.getName();
    HashMap<String, String> env = new HashMap<>();
    env.put("HADOOP_USER_CLASSPATH_FIRST", "1");
    env.put("HADOOP_CLASSPATH", classpath.toString());
    lp.env = env;
    return launchApp(lp);
  }

  protected AppHandle launchApp(LaunchParams params) throws IOException {
    File tmpFile = File.createTempFile("beam-runner-apex", "params");
    tmpFile.deleteOnExit();
    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      SerializationUtils.serialize(params, fos);
    }
    if (params.getCmd() == null) {
      ApexYarnLauncher.main(new String[] {tmpFile.getAbsolutePath()});
    } else {
      String cmd = params.getCmd() + " " + tmpFile.getAbsolutePath();
      ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
      LOG.info("Executing: {} with {}", cmd, params.getEnv());

      ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
      Map<String, String> env = pb.environment();
      env.putAll(params.getEnv());
      Process p = pb.start();
      ProcessWatcher pw = new ProcessWatcher(p);
      InputStream output = p.getInputStream();
      InputStream error = p.getErrorStream();
      while (!pw.isFinished()) {
        IOUtils.copy(output, consoleOutput);
        IOUtils.copy(error, consoleOutput);
      }
      if (pw.rc != 0) {
        String msg = "The Beam Apex runner in non-embedded mode requires the Hadoop client"
            + " to be installed on the machine from which you launch the job"
            + " and the 'hadoop' script in $PATH";
        LOG.error(msg);
        throw new RuntimeException("Failed to run: " + cmd + " (exit code " + pw.rc + ")" + "\n"
            + consoleOutput.toString());
      }
    }
    return new AppHandle() {
      @Override
      public boolean isFinished() {
        // TODO (future PR): interaction with child process
        LOG.warn("YARN application runs asynchronously and status check not implemented.");
        return true;
      }
      @Override
      public void shutdown(ShutdownMode arg0) throws LauncherException {
        // TODO (future PR): interaction with child process
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * From the current classpath, find the jar files that need to be deployed
   * with the application to run on YARN. Hadoop dependencies are provided
   * through the Hadoop installation and the application should not bundle them
   * to avoid conflicts. This is done by removing the Hadoop compile
   * dependencies (transitively) by parsing the Maven dependency tree.
   *
   * @return list of jar files to ship
   * @throws IOException when dependency information cannot be read
   */
  public static List<File> getYarnDeployDependencies() throws IOException {
    try (InputStream dependencyTree = ApexRunner.class.getResourceAsStream("dependency-tree")) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(dependencyTree))) {
        String line;
        List<String> excludes = new ArrayList<>();
        int excludeLevel = Integer.MAX_VALUE;
        while ((line = br.readLine()) != null) {
          for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (Character.isLetter(c)) {
              if (i > excludeLevel) {
                excludes.add(line.substring(i));
              } else {
                if (line.substring(i).startsWith("org.apache.hadoop")) {
                  excludeLevel = i;
                  excludes.add(line.substring(i));
                } else {
                  excludeLevel = Integer.MAX_VALUE;
                }
              }
              break;
            }
          }
        }

        Set<String> excludeJarFileNames = Sets.newHashSet();
        for (String exclude : excludes) {
          String[] mvnc = exclude.split(":");
          String fileName = mvnc[1] + "-";
          if (mvnc.length == 6) {
            fileName += mvnc[4] + "-" + mvnc[3]; // with classifier
          } else {
            fileName += mvnc[3];
          }
          fileName += ".jar";
          excludeJarFileNames.add(fileName);
        }

        ClassLoader classLoader = ApexYarnLauncher.class.getClassLoader();
        URL[] urls = ((URLClassLoader) classLoader).getURLs();
        List<File> dependencyJars = new ArrayList<>();
        for (URL url : urls) {
          File f = new File(url.getFile());
          // dependencies can also be directories in the build reactor,
          // the Apex client will automatically create jar files for those.
          if (f.exists() && !excludeJarFileNames.contains(f.getName())) {
            dependencyJars.add(f);
          }
        }
        return dependencyJars;
      }
    }
  }

  /**
   * Create a jar file from the given directory.
   * @param dir source directory
   * @param jarFile jar file name
   * @throws IOException when file cannot be created
   */
  public static void createJar(File dir, File jarFile) throws IOException {

    final Map<String, ?> env = Collections.singletonMap("create", "true");
    if (jarFile.exists() && !jarFile.delete()) {
      throw new RuntimeException("Failed to remove " + jarFile);
    }
    URI uri = URI.create("jar:" + jarFile.toURI());
    try (final FileSystem zipfs = FileSystems.newFileSystem(uri, env)) {

      File manifestFile = new File(dir, JarFile.MANIFEST_NAME);
      Files.createDirectory(zipfs.getPath("META-INF"));
      try (final OutputStream out = Files.newOutputStream(zipfs.getPath(JarFile.MANIFEST_NAME))) {
        if (!manifestFile.exists()) {
          new Manifest().write(out);
        } else {
          FileUtils.copyFile(manifestFile, out);
        }
      }

      final java.nio.file.Path root = dir.toPath();
      Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<Path>() {
        String relativePath;

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
            throws IOException {
          relativePath = root.relativize(dir).toString();
          if (!relativePath.isEmpty()) {
            if (!relativePath.endsWith("/")) {
              relativePath += "/";
            }
            if (!"META-INF/".equals(relativePath)) {
              final Path dstDir = zipfs.getPath(relativePath);
              Files.createDirectory(dstDir);
            }
          }
          return super.preVisitDirectory(dir, attrs);
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          String name = relativePath + file.getFileName();
          if (!JarFile.MANIFEST_NAME.equals(name)) {
            try (final OutputStream out = Files.newOutputStream(zipfs.getPath(name))) {
              FileUtils.copyFile(file.toFile(), out);
            }
          }
          return super.visitFile(file, attrs);
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          relativePath = root.relativize(dir.getParent()).toString();
          if (!relativePath.isEmpty() && !relativePath.endsWith("/")) {
            relativePath += "/";
          }
          return super.postVisitDirectory(dir, exc);
        }
      });
    }
  }

  /**
   * Transfer the properties to the configuration object.
   */
  public static void addProperties(Configuration conf, Properties props) {
    for (final String propertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(propertyName);
      conf.set(propertyName, propertyValue);
    }
  }

  /**
   * The main method expects the serialized DAG and will launch the YARN application.
   * @param args location of launch parameters
   * @throws IOException when parameters cannot be read
   */
  public static void main(String[] args) throws IOException {
    checkArgument(args.length == 1, "exactly one argument expected");
    File file = new File(args[0]);
    checkArgument(file.exists() && file.isFile(), "invalid file path %s", file);
    final LaunchParams params = (LaunchParams) SerializationUtils.deserialize(
        new FileInputStream(file));
    StreamingApplication apexApp = (dag, conf) -> copyShallow(params.dag, dag);
    Configuration conf = new Configuration(); // configuration from Hadoop client
    addProperties(conf, params.configProperties);
    AppHandle appHandle = params.getApexLauncher().launchApp(apexApp, conf,
        params.launchAttributes);
    if (appHandle == null) {
      throw new AssertionError("Launch returns null handle.");
    }
    // TODO (future PR)
    // At this point the application is running, but this process should remain active to
    // allow the parent to implement the runner result.
  }

  /**
   * Launch parameters that will be serialized and passed to the child process.
   */
  @VisibleForTesting
  protected static class LaunchParams implements Serializable {
    private static final long serialVersionUID = 1L;
    private final DAG dag;
    private final Attribute.AttributeMap launchAttributes;
    private final Properties configProperties;
    private HashMap<String, String> env;
    private String cmd;

    protected LaunchParams(DAG dag, AttributeMap launchAttributes, Properties configProperties) {
      this.dag = dag;
      this.launchAttributes = launchAttributes;
      this.configProperties = configProperties;
    }

    protected Launcher<?> getApexLauncher() {
      return Launcher.getLauncher(LaunchMode.YARN);
    }

    protected String getCmd() {
      return cmd;
    }

    protected Map<String, String> getEnv() {
      return env;
    }

  }

  private static void copyShallow(DAG from, DAG to) {
    checkArgument(from.getClass() == to.getClass(), "must be same class %s %s",
        from.getClass(), to.getClass());
    Field[] fields = from.getClass().getDeclaredFields();
    AccessibleObject.setAccessible(fields, true);
    for (Field field : fields) {
      if (!Modifier.isStatic(field.getModifiers())) {
        try {
          field.set(to, field.get(from));
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Starts a command and waits for it to complete.
   */
  public static class ProcessWatcher implements Runnable {
    private final Process p;
    private volatile boolean finished = false;
    private volatile int rc;

    public ProcessWatcher(Process p) {
      this.p = p;
      new Thread(this).start();
    }

    public boolean isFinished() {
      return finished;
    }

    @Override
    public void run() {
      try {
        rc = p.waitFor();
      } catch (Exception e) {
        // ignore
      }
      finished = true;
    }
  }

}
