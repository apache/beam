/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.playground.extract_symbols;

import com.esotericsoftware.yamlbeans.YamlConfig;
import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        final String sdkPath = args[0];
        final HashMap<String, ClassInfo> classInfoMap = getDirSymbols(sdkPath);
        final String yamlString = buildYamlString(classInfoMap);
        System.out.print(yamlString);
    }

    private static HashMap<String, ClassInfo> getDirSymbols(String sdkPathString) throws IOException {
        final HashMap<String, ClassInfo> classInfoMap = new HashMap<>();
        final Path sdkPath = new File(sdkPathString).toPath().toAbsolutePath();
        Files.walk(sdkPath).forEach(path -> {
            String stringPath = path.toString();
            final String relativePath = sdkPath.relativize(path).toString();
            if (isJavaNonTestFile(relativePath)) {
                String fileName = stringPath.substring(stringPath.lastIndexOf("/") + 1).replace(".java", "");
                try {
                    CompilationUnit unit = StaticJavaParser.parse(path);
                    if (unit.getClassByName(fileName).isPresent()) {
                        addClassSymbols(classInfoMap, unit.getClassByName(fileName).get());
                    }
                } catch (IOException | ParseProblemException ignored) {
                }
            }
        });

        return classInfoMap;
    }

    static boolean isJavaNonTestFile(String stringPath) {
        final boolean isInTestFolder = stringPath.contains("/test/") || stringPath.startsWith("test/");
        return stringPath.endsWith(".java") && !isInTestFolder;
    }

    private static void addClassSymbols(HashMap<String, ClassInfo> classInfoList, ClassOrInterfaceDeclaration cl) {
        if (!cl.isPublic()) {
            return;
        }

        ClassInfo classInfo;
        if (classInfoList.containsKey(cl.getNameAsString())) {
            classInfo = classInfoList.get(cl.getNameAsString());
        } else {
            classInfo = new ClassInfo();
            classInfoList.put(cl.getNameAsString(), classInfo);
        }

        cl.findAll(MethodDeclaration.class).forEach(method -> {
            if (method.isPublic()) {
                classInfo.publicMethods.add(method.getNameAsString());
            }
        });
        cl.findAll(FieldDeclaration.class).forEach(field -> {
            if (field.isPublic()) {
                classInfo.publicFields.add(field.getVariable(0).getNameAsString());
            }
        });
    }

    private static String buildYamlString(HashMap<String, ClassInfo> classInfoMap) throws YamlException {
        final StringWriter stringWriter = new StringWriter();
        final YamlWriter yamlWriter = new YamlWriter(stringWriter);
        yamlWriter.getConfig().writeConfig.setIndentSize(2);
        yamlWriter.getConfig().writeConfig.setWriteClassname(YamlConfig.WriteClassName.NEVER);
        final LinkedHashMap<String, Map<String, List<String>>> yamlMap = new LinkedHashMap<>();

        classInfoMap.forEach((key, value) -> yamlMap.put(key, value.toMap()));
        final LinkedHashMap<String, Map<String, List<String>>> sortedMap = sortMap(yamlMap);

        yamlWriter.write(sortedMap);

        yamlWriter.close();
        return stringWriter.toString();
    }

    private static LinkedHashMap<String, Map<String, List<String>>> sortMap(HashMap<String, Map<String, List<String>>> yamlMap) {
        final Comparator<Map.Entry<String, ?>> comparator = Comparator.comparing(Map.Entry::getKey);
        final ArrayList<Map.Entry<String, Map<String, List<String>>>> array = new ArrayList<>(yamlMap.entrySet());
        array.sort(comparator);

        final LinkedHashMap<String, Map<String, List<String>>> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, List<String>>> entry : array) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
}
