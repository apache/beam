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
package org.apache.beam.sdk.schemas.transforms.providers;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.annotation.Nullable;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringCompiler {
  private static final Logger LOG = LoggerFactory.getLogger(StringCompiler.class);

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static final Supplier<String> classpathSupplier =
      Suppliers.memoize(
          () -> {
            ClassLoader cl = StringCompiler.class.getClassLoader();
            if (cl == null) {
              cl = ClassLoader.getSystemClassLoader();
            }
            List<String> cp = buildClasspath(cl, System.getProperty("java.class.path"));
            return String.join(File.pathSeparator, cp);
          });

  @VisibleForTesting
  static List<String> buildClasspath(@Nullable ClassLoader cl, @Nullable String classPathProperty) {
    Set<String> cp = new LinkedHashSet<>();
    Set<File> visitedJars = new HashSet<>();

    if (classPathProperty != null) {
      for (String entry :
          Splitter.on(File.pathSeparator).omitEmptyStrings().split(classPathProperty)) {
        File file = new File(entry);
        if (file.exists()) {
          cp.add(file.getAbsolutePath());
          if (file.isFile() && isJarFile(file)) {
            extractManifestClasspath(file, cp, visitedJars);
          }
        }
      }
    }

    if (cl instanceof URLClassLoader) {
      for (URL url : ((URLClassLoader) cl).getURLs()) {
        File file;
        try {
          file = new File(url.toURI());
        } catch (Exception e) {
          file = new File(url.getPath());
        }
        if (file.exists()) {
          cp.add(file.getAbsolutePath());
          if (file.isFile() && isJarFile(file)) {
            extractManifestClasspath(file, cp, visitedJars);
          }
        }
      }
    }

    addClassLocation(Row.class, cp, visitedJars);
    addClassLocation(StringCompiler.class, cp, visitedJars);

    if (cl != null) {
      addResourceLocation(cl, "org/apache/beam/sdk/values/Row.class", cp, visitedJars);
      addResourceLocation(
          cl,
          "org/apache/beam/sdk/schemas/transforms/providers/StringCompiler.class",
          cp,
          visitedJars);
    }

    return new ArrayList<>(cp);
  }

  private static void extractManifestClasspath(
      File jarFile, Set<String> cp, Set<File> visitedJars) {
    File canonicalJar;
    try {
      canonicalJar = jarFile.getCanonicalFile();
    } catch (IOException e) {
      canonicalJar = jarFile.getAbsoluteFile();
    }
    if (!visitedJars.add(canonicalJar)) {
      return;
    }

    try (ZipFile zipFile = new ZipFile(jarFile)) {
      ZipEntry manifestEntry = zipFile.getEntry("META-INF/MANIFEST.MF");
      if (manifestEntry != null) {
        try (InputStream is = zipFile.getInputStream(manifestEntry)) {
          if (is != null) {
            Manifest manifest = new Manifest(is);
            String classPathAttr =
                manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
            if (classPathAttr != null) {
              for (String part :
                  Splitter.onPattern("\\s+").omitEmptyStrings().split(classPathAttr)) {
                File targetFile = resolveManifestEntry(jarFile.getParentFile(), part);
                if (targetFile != null && targetFile.exists()) {
                  cp.add(targetFile.getAbsolutePath());
                  if (targetFile.isFile() && isJarFile(targetFile)) {
                    extractManifestClasspath(targetFile, cp, visitedJars);
                  }
                }
              }
            }
          }
        }
      }
    } catch (IOException exn) {
      LOG.warn("Failed to read manifest from jar: {}", jarFile, exn);
    }
  }

  @VisibleForTesting
  static @Nullable File resolveManifestEntry(@Nullable File parentDir, @Nullable String entry) {
    if (entry == null || entry.isEmpty()) {
      return null;
    }
    try {
      String uriStr = entry;
      if (isWindowsAbsoluteDrivePath(uriStr)) {
        uriStr = "file:///" + uriStr.replace('\\', '/');
      }
      URI uri = URI.create(uriStr);
      String path = uri.getPath();
      if (path != null && !path.isEmpty()) {
        if (path.length() >= 3
            && path.charAt(0) == '/'
            && Character.isLetter(path.charAt(1))
            && path.charAt(2) == ':') {
          path = path.substring(1);
        }
        File f = new File(path);
        if (isRootedOrAbsolute(path, f)) {
          return f;
        }
        return parentDir != null ? new File(parentDir, path) : f;
      }
    } catch (IllegalArgumentException e) {
      // Fallback if entry is not a valid URI (e.g. unencoded special characters or raw Windows
      // path)
    }

    String rawPath = entry.startsWith("file:") ? entry.substring("file:".length()) : entry;
    File f = new File(rawPath);
    if (isRootedOrAbsolute(rawPath, f)) {
      return f;
    }
    return parentDir != null ? new File(parentDir, rawPath) : f;
  }

  private static boolean isRootedOrAbsolute(String path, File f) {
    if (f.isAbsolute()) {
      return true;
    }
    if (!path.isEmpty()) {
      char c0 = path.charAt(0);
      // Leading '/' (Unix root-relative or absolute) or '\' (Windows root-relative or UNC '\\')
      if (c0 == '/' || c0 == '\\') {
        return true;
      }
    }
    return isWindowsAbsoluteDrivePath(path);
  }

  private static boolean isWindowsAbsoluteDrivePath(String path) {
    return path.length() >= 3
        && Character.isLetter(path.charAt(0))
        && path.charAt(1) == ':'
        && (path.charAt(2) == '/' || path.charAt(2) == '\\');
  }

  private static boolean isJarFile(File file) {
    String name = file.getName().toLowerCase(Locale.ROOT);
    return name.endsWith(".jar") || name.endsWith(".zip");
  }

  private static void addClassLocation(Class<?> clazz, Set<String> cp, Set<File> visitedJars) {
    try {
      ProtectionDomain pd = clazz.getProtectionDomain();
      if (pd != null) {
        CodeSource cs = pd.getCodeSource();
        if (cs != null && cs.getLocation() != null) {
          URL location = cs.getLocation();
          File file;
          try {
            file = new File(location.toURI());
          } catch (Exception e) {
            file = new File(location.getPath());
          }
          if (file.exists()) {
            cp.add(file.getAbsolutePath());
            if (file.isFile() && isJarFile(file)) {
              extractManifestClasspath(file, cp, visitedJars);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.debug("Unable to determine code source location for {}", clazz, e);
    }
  }

  private static void addResourceLocation(
      ClassLoader cl, String resourcePath, Set<String> cp, Set<File> visitedJars) {
    try {
      URL res = cl.getResource(resourcePath);
      if (res != null && "jar".equalsIgnoreCase(res.getProtocol())) {
        String path = res.getPath();
        int bang = path.indexOf("!/");
        if (bang != -1) {
          String jarPath = path.substring(0, bang);
          if (jarPath.startsWith("file:")) {
            try {
              jarPath = URI.create(jarPath).getPath();
            } catch (Exception e) {
              jarPath = jarPath.substring("file:".length());
            }
          }
          File jarFile = new File(jarPath);
          if (jarFile.exists()) {
            cp.add(jarFile.getAbsolutePath());
            if (jarFile.isFile() && isJarFile(jarFile)) {
              extractManifestClasspath(jarFile, cp, visitedJars);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.debug("Unable to determine resource location for {}", resourcePath, e);
    }
  }

  public static class CompileException extends Exception {
    private final DiagnosticCollector<?> diagnostics;

    public CompileException(DiagnosticCollector<?> diagnostics) {
      super(diagnostics.getDiagnostics().toString());
      this.diagnostics = diagnostics;
    }

    public DiagnosticCollector<?> getDiagnostics() {
      return diagnostics;
    }
  }

  // TODO(XXX): swap args?
  public static <T> Class<T> getClass(String name, String source)
      throws CompileException, ClassNotFoundException {
    return getClass(name, source, classpathSupplier.get());
  }

  @VisibleForTesting
  static <T> Class<T> getClass(String name, String source, String classpath)
      throws CompileException, ClassNotFoundException {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    InMemoryFileManager fileManager =
        new InMemoryFileManager(compiler.getStandardFileManager(null, null, null));
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    JavaCompiler.CompilationTask task =
        compiler.getTask(
            null,
            fileManager,
            diagnostics,
            ImmutableList.of("-classpath", classpath),
            null,
            Collections.singletonList(new InMemoryFileManager.InputJavaFileObject(name, source)));
    boolean result = task.call();
    if (!result) {
      throw new CompileException(diagnostics);
    } else {
      return (Class<T>) fileManager.getClassLoader().loadClass(name);
    }
  }

  public static Object getInstance(String name, String source)
      throws CompileException, ReflectiveOperationException {
    return getClass(name, source).getDeclaredConstructor().newInstance();
  }

  public static Type guessExpressionType(String expression, Map<String, Type> inputTypes)
      throws StringCompiler.CompileException, ClassNotFoundException {

    String expectedError = "cannot be converted to __TypeGuesserHelper__.BadReturnType";

    try {
      StringCompiler.getClass(
          "__TypeGuesserHelper__", typeGuesserSource(expression, inputTypes, "BadReturnType"));
      // Must have returned null.
      return Void.class;
    } catch (StringCompiler.CompileException exn) {
      // Use the error message to derive the actual type.
      for (Diagnostic<?> d : exn.getDiagnostics().getDiagnostics()) {
        String msg = d.getMessage(Locale.ROOT);
        int expectedErrorIndex = msg.indexOf(expectedError);
        if (expectedErrorIndex != -1) {
          String typeSource =
              msg.substring(
                  1 + "incompatible types: ".length() + msg.lastIndexOf('\n', expectedErrorIndex),
                  expectedErrorIndex);
          Class<?> clazz =
              StringCompiler.getClass(
                  "__TypeGuesserHelper__", typeGuesserSource(expression, inputTypes, typeSource));
          for (Method method : clazz.getMethods()) {
            if (method.getName().equals("method")) {
              return method.getGenericReturnType();
            }
          }
          // We should never get here.
          throw new RuntimeException("Unable to locate declared method.");
        }
      }
      // Must have been some other error.
      throw exn;
    }
  }

  private static String typeGuesserSource(
      String expression, Map<String, Type> inputTypes, String returnType) {
    StringBuilder source = new StringBuilder();
    source.append("class __TypeGuesserHelper__ {\n");
    source.append("  private static class BadReturnType { private BadReturnType() {} }\n");
    source.append("  public static " + returnType + " method(\n");
    boolean first = true;
    for (Map.Entry<String, Type> arg : inputTypes.entrySet()) {
      if (first) {
        first = false;
      } else {
        source.append(", ");
      }
      source.append(arg.getValue().getTypeName() + " " + arg.getKey());
    }
    source.append("    ) {\n");
    source.append("    return " + expression + ";\n");
    source.append("  }\n");
    source.append("}\n");
    return source.toString();
  }

  private static class InMemoryFileManager
      extends ForwardingJavaFileManager<StandardJavaFileManager> {

    private Map<String, OutputJavaFileObject> outputFileObjects = new HashMap<>();

    public InMemoryFileManager(StandardJavaFileManager standardManager) {
      super(standardManager);
    }

    @Override
    public JavaFileObject getJavaFileForOutput(
        Location location, String className, JavaFileObject.Kind kind, FileObject sibling) {

      OutputJavaFileObject classAsBytes = new OutputJavaFileObject(className, kind);
      outputFileObjects.put(className, classAsBytes);
      return classAsBytes;
    }

    public ClassLoader getClassLoader() {
      return AccessController.<ClassLoader>doPrivileged(
          (PrivilegedAction<ClassLoader>)
              () ->
                  new SecureClassLoader() {
                    @Override
                    protected Class<?> findClass(String name) throws ClassNotFoundException {
                      OutputJavaFileObject fileObject = outputFileObjects.get(name);
                      if (fileObject == null) {
                        throw new ClassNotFoundException(name);
                      } else {
                        byte[] classBytes = fileObject.getBytes();
                        return defineClass(name, classBytes, 0, classBytes.length);
                      }
                    }
                  });
    }

    @Override
    public ClassLoader getClassLoader(Location location) {
      return getClassLoader();
    }

    private static class InputJavaFileObject extends SimpleJavaFileObject {
      private String source;

      public InputJavaFileObject(String name, String source) {
        super(
            URI.create("input:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
        this.source = source;
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) {
        return source;
      }
    }

    private static class OutputJavaFileObject extends SimpleJavaFileObject {

      private ByteArrayOutputStream content = new ByteArrayOutputStream();

      public OutputJavaFileObject(String name, Kind kind) {
        super(URI.create("output:///" + name.replace('.', '/') + kind.extension), kind);
      }

      public byte[] getBytes() {
        return content.toByteArray();
      }

      @Override
      public OutputStream openOutputStream() {
        return content;
      }
    }
  }
}
