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

package org.apache.beam.sdk.schemas.utils.avro.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerDesUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerDesUtils.class);

  private static final Map<Schema, Long> SCHEMA_IDS_CACHE = new HashMap<>();
  private static final String SEP = "_";

  /**
   * This function will produce a fingerprint for the provided schema.
   *
   * @param schema a schema
   * @return fingerprint for the given schema
   */
  public static Long getSchemaFingerprint(Schema schema) {
    Long schemaId = SCHEMA_IDS_CACHE.get(schema);
    if (schemaId == null) {
      schemaId = SchemaNormalization.parsingFingerprint64(schema);
      SCHEMA_IDS_CACHE.put(schema, schemaId);
    }

    return schemaId;
  }

  public static String getSchemaKey(Schema writerSchema, Schema readerSchema) {
    return String.valueOf(Math.abs(getSchemaFingerprint(writerSchema))) + Math.abs(
        getSchemaFingerprint(readerSchema));
  }

  /**
   * Generates Java Source Path from package name.
   *
   * @param packageName java package name
   * @return
   */
  public static String generateSourcePathFromPackageName(String packageName) {
    StringBuilder pathBuilder = new StringBuilder(File.separator);
    Arrays.stream(packageName.split("\\."))
        .forEach(s -> pathBuilder.append(s).append(File.separator));
    return pathBuilder.toString();
  }

  /**
   * Return Type name based on schema type. For union schemas Type is last schema's Type.
   *
   * @param schema Avro schema.
   * @return
   */
  public static String getTypeName(Schema schema) {
    Schema.Type schemaType = schema.getType();
    if (Schema.Type.RECORD.equals(schemaType)) {
      return schema.getName();
    } else if (Schema.Type.ARRAY.equals(schemaType)) {
      return "Array_of_" + getTypeName(schema.getElementType());
    } else if (Schema.Type.MAP.equals(schemaType)) {
      return "Map_of_" + getTypeName(schema.getValueType());
    } else if (Schema.Type.UNION.equals(schemaType)) {
      Schema unionSchema = schema.getTypes().get(schema.getTypes().size() - 1);
      return "Union_of_" + getTypeName(unionSchema);
    } else {
      return schema.getType().name();
    }
  }

  /**
   * Generate Class Name for Deserialization/Serialization class.
   *
   * @param writerSchema Avro writer schema
   * @param readerSchema Avro Reader schema
   * @param description  Additional information to identify class's purpose.
   * @return
   */
  public static String getClassName(Schema writerSchema, Schema readerSchema, String description) {
    Long writerSchemaId = Math.abs(getSchemaFingerprint(writerSchema));
    Long readerSchemaId = Math.abs(getSchemaFingerprint(readerSchema));
    String typeName = getTypeName(readerSchema);
    return typeName + SEP + description + SEP + writerSchemaId + SEP + readerSchemaId;
  }

  public static String getClassName(Integer schemaHashCode,
                                    String description) {
    return description + SEP + Math.abs(schemaHashCode);
  }

  private static String replaceLast(String str, char target, char replacement) {
    if (str.indexOf(target) < 0) {
      // doesn't contain target char
      return str;
    }
    int lastOccurrence = str.lastIndexOf(target);
    StringBuilder sb = new StringBuilder();
    sb.append(str, 0, lastOccurrence)
        .append(replacement);
    if (lastOccurrence != str.length() - 1) {
      sb.append(str.substring(lastOccurrence + 1));
    }
    return sb.toString();
  }

  private static Class<?> loadClass(String className) throws ClassNotFoundException {
    try {
      // First try the current class loader
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      if (className.equals("byte[]")) {
        return byte[].class;
      }
      // If the required class couldn't be found,
      // here will try to use the class loader of current thread
      return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
    }
  }

  /**
   * This class is used to infer all the compilation dependencies.
   *
   * @param existingCompileClasspath            existing compile classpath
   * @param filePath                            java source file to compile
   * @param knownUsedFullyQualifiedClassNameSet known fully qualified class name when generating the
   *                                            serialization/de-serialization classes
   * @return classpath to compile given file
   * @throws IOException            on io issues
   * @throws ClassNotFoundException on classloading issues
   */
  public static String inferCompileDependencies(String existingCompileClasspath, String filePath,
      Set<String> knownUsedFullyQualifiedClassNameSet)
      throws IOException, ClassNotFoundException {
    Set<String> usedFullyQualifiedClassNameSet = new HashSet<>(knownUsedFullyQualifiedClassNameSet);
    Set<String> libSet = Arrays.stream(existingCompileClasspath.split(":"))
        .collect(Collectors.toSet());
    final String importPrefix = "import ";
    // collect all the necessary dependencies for compilation
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(importPrefix)) {
          // Get the qualified class name from "import" statements
          String qualifiedClassName = line.substring(importPrefix.length(), line.length() - 1);

          LOGGER.warn("qualifiedClassName: " + qualifiedClassName);

          if (StringUtils.isNotBlank(qualifiedClassName)) {
            usedFullyQualifiedClassNameSet.add(qualifiedClassName);
          }
        }
      }

      StringBuilder sb = new StringBuilder(existingCompileClasspath);
      for (String requiredClass : usedFullyQualifiedClassNameSet) {
        CodeSource codeResource;
        try {
          codeResource = loadClass(requiredClass).getProtectionDomain().getCodeSource();
        } catch (ClassNotFoundException e) {
          // Inner class couldn't be located directly by Class.forName in the formal way,
          // so we have to replace the last '.' by '$'.
          // For example, 'org.apache.avro.generic.GenericData.Record' could NOT be located by
          // Class#forName(String), but 'org.apache.avro.generic.GenericData#Record' can be located.
          // Here, we only try once since multiple layers of inner class is not expected
          // in the generated java class, and in theory, the inner class being used could only
          // be defined by Avro lib. If this assumption is not right,
          // we need to do recursive search to find the right library.
          codeResource = loadClass(replaceLast(requiredClass, '.', '$')).getProtectionDomain()
              .getCodeSource();
        }
        if (codeResource != null) {
          String libPath = codeResource.getLocation().getFile();
          LOGGER.warn("libPath: " + libPath);
          if (StringUtils.isNotBlank(libPath) && !libSet.contains(libPath)) {
            sb.append(":").append(libPath);
            libSet.add(libPath);
          }
        }
      }
      return sb.toString();
    }
  }

}
