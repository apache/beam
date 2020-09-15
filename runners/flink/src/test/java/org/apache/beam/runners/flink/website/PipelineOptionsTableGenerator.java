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
package org.apache.beam.runners.flink.website;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A main class which is called by the Gradle generatePipelineOptionsTable* tasks to update the list
 * of available pipeline options for the Beam website.
 */
public class PipelineOptionsTableGenerator {

  private static final List<String> supportedLanguages = ImmutableList.of("java", "python");

  private static class Option {
    String name;
    String description;
    @Nullable String defaultValue;

    public Option(String name, String description, @Nullable String defaultValue) {
      this.name = name;
      this.description = description;
      this.defaultValue = defaultValue;
    }
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new RuntimeException(
          "Please specify the language (" + supportedLanguages + ") as the only argument.");
    }
    String arg = args[0].toLowerCase();
    if (!supportedLanguages.contains(arg)) {
      throw new RuntimeException("The language is not supported: " + arg);
    }
    boolean isPython = arg.equals("python");

    printHeader();
    List<Option> options = extractOptions(isPython);
    printOptionsTable(options);
  }

  private static void printHeader() {
    System.out.println(
        "<!--\n"
            + "Licensed under the Apache License, Version 2.0 (the \"License\");\n"
            + "you may not use this file except in compliance with the License.\n"
            + "You may obtain a copy of the License at\n"
            + "\n"
            + "http://www.apache.org/licenses/LICENSE-2.0\n"
            + "\n"
            + "Unless required by applicable law or agreed to in writing, software\n"
            + "distributed under the License is distributed on an \"AS IS\" BASIS,\n"
            + "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
            + "See the License for the specific language governing permissions and\n"
            + "limitations under the License.\n"
            + "-->");
    System.out.println(
        "<!--\n"
            + "This is an auto-generated file.\n"
            + "Use generatePipelineOptionsTableJava and generatePipelineOptionsTablePython respectively.\n"
            + "Should be called before running the tests.\n"
            + "-->");
  }

  /**
   * Returns the extracted list of options via reflections on FlinkPipelineOptions. Options are
   * returned sorted in alphabetical order since Java does not guarantee any consistent order on the
   * class methods.
   */
  private static List<Option> extractOptions(boolean isPython) {
    List<Option> options = new ArrayList<>();
    for (Method method : FlinkPipelineOptions.class.getDeclaredMethods()) {
      String name;
      String description;
      String defaultValue = null;
      name = method.getName();
      if (name.matches("^(get|is).*")) {
        name = name.replaceFirst("^(get|is)", "");

        if (isPython) {
          name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
        } else {
          name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
        }

        Description descriptionAnnotation = method.getAnnotation(Description.class);
        if (descriptionAnnotation == null) {
          throw new RuntimeException(
              "All pipeline options should have a description. Please add one for " + name);
        }
        description = descriptionAnnotation.value();

        Optional<String> defaultValueFromAnnotation = getDefaultValueFromAnnotation(method);
        if (defaultValueFromAnnotation.isPresent()) {
          defaultValue = defaultValueFromAnnotation.get();
        }

        options.add(new Option(name, description, defaultValue));
      }
    }
    options.sort(Comparator.comparing(option -> option.name));
    return options;
  }

  private static void printOptionsTable(List<Option> options) {
    System.out.println("<table class=\"table table-bordered\">");
    for (Option option : options) {
      System.out.println("<tr>");
      System.out.println("  <td><code>" + option.name + "</code></td>");
      System.out.println("  <td>" + option.description + "</td>");
      if (option.defaultValue != null) {
        System.out.println("  <td>Default: <code>" + option.defaultValue + "</code></td>");
      } else {
        System.out.println("  <td></td>");
      }
      System.out.println("</tr>");
    }
    System.out.println("</table>");
  }

  /** Returns a string representation of the {@link Default} value on the passed in method. */
  private static Optional<String> getDefaultValueFromAnnotation(Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      if (annotation instanceof Default.Class) {
        return Optional.of(((Default.Class) annotation).value().getSimpleName());
      } else if (annotation instanceof Default.String) {
        return Optional.of(((Default.String) annotation).value());
      } else if (annotation instanceof Default.Boolean) {
        return Optional.of(Boolean.toString(((Default.Boolean) annotation).value()));
      } else if (annotation instanceof Default.Character) {
        return Optional.of(Character.toString(((Default.Character) annotation).value()));
      } else if (annotation instanceof Default.Byte) {
        return Optional.of(Byte.toString(((Default.Byte) annotation).value()));
      } else if (annotation instanceof Default.Short) {
        return Optional.of(Short.toString(((Default.Short) annotation).value()));
      } else if (annotation instanceof Default.Integer) {
        return Optional.of(Integer.toString(((Default.Integer) annotation).value()));
      } else if (annotation instanceof Default.Long) {
        return Optional.of(Long.toString(((Default.Long) annotation).value()));
      } else if (annotation instanceof Default.Float) {
        return Optional.of(Float.toString(((Default.Float) annotation).value()));
      } else if (annotation instanceof Default.Double) {
        return Optional.of(Double.toString(((Default.Double) annotation).value()));
      } else if (annotation instanceof Default.Enum) {
        return Optional.of(((Default.Enum) annotation).value());
      } else if (annotation instanceof Default.InstanceFactory) {
        return Optional.of(((Default.InstanceFactory) annotation).value().getSimpleName());
      }
    }
    return Optional.empty();
  }
}
