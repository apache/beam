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
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class StringCompiler {
  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static final Supplier<String> classpathSupplier =
      Suppliers.memoize(
          () -> {
            List<String> cp = new ArrayList<>();
            cp.add(System.getProperty("java.class.path"));
            // Javac doesn't properly handle manifest classpath spec.
            ClassLoader cl = StringCompiler.class.getClassLoader();
            if (cl == null) {
              cl = ClassLoader.getSystemClassLoader();
            }
            if (cl instanceof URLClassLoader) {
              for (URL url : ((URLClassLoader) cl).getURLs()) {
                File file = new File(url.getFile());
                if (file.exists() && !file.isDirectory()) {
                  try (ZipFile zipFile = new ZipFile(new File(url.getFile()))) {
                    ZipEntry manifestEntry = zipFile.getEntry("META-INF/MANIFEST.MF");
                    if (manifestEntry != null) {
                      Manifest manifest = new Manifest(zipFile.getInputStream(manifestEntry));
                      cp.add(manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH));
                    }
                  } catch (IOException exn) {
                    throw new RuntimeException(exn);
                  }
                }
              }
            }
            return String.join(System.getProperty("path.separator"), cp);
          });

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
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    InMemoryFileManager fileManager =
        new InMemoryFileManager(compiler.getStandardFileManager(null, null, null));
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    JavaCompiler.CompilationTask task =
        compiler.getTask(
            null,
            fileManager,
            diagnostics,
            ImmutableList.of("-classpath", classpathSupplier.get()),
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
