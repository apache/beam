package com.playground.extract_symbols;

import com.github.javaparser.ParseProblemException;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        var sdkPath = args[0];
        HashMap<String, ClassInfo> classInfoMap = collectClassInfo(sdkPath);
        String yamlString = buildYamlString(classInfoMap);
        System.out.println(yamlString);
    }

    private static HashMap<String, ClassInfo> collectClassInfo(String sdkPath) throws IOException {
        var classInfoList = new HashMap<String, ClassInfo>();
        var paths = new File(sdkPath).toPath().toAbsolutePath();
        Files.walk(paths).forEach(path -> {
            var stringPath = path.toString();
            if (stringPath.endsWith(".java") && !stringPath.contains("test")) {
                var fileName = stringPath.substring(stringPath.lastIndexOf("/") + 1).replace(".java", "");
                try {
                    var unit = StaticJavaParser.parse(path);
                    if (unit.getClassByName(fileName).isPresent()) {
                        buildClass(classInfoList, unit.getClassByName(fileName).get());
                    }
                } catch (IOException | ParseProblemException ignored) {
                }
            }
        });

        return classInfoList;
    }

    private static void buildClass(HashMap<String, ClassInfo> classInfoList, ClassOrInterfaceDeclaration cl) {
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

    private static String buildYamlString(HashMap<String, ClassInfo> classInfoList) {
        final String[] resultList = new String[classInfoList.size()];
        var i = 0;
        for (Map.Entry<String, ClassInfo> entry : classInfoList.entrySet()) {
            String name = entry.getKey();
            ClassInfo classInfo = entry.getValue();
            var yaml = toYaml(name, classInfo);
            resultList[i] = yaml;
            i++;
        }
        return String.join("\n", resultList);
    }

    private static String toYaml(String name, ClassInfo classInfo) {
        var methods = classInfo.publicMethods;
        var properties = classInfo.publicFields;

        var methodsString = methods.isEmpty() ? "" : String.join("\n  - ", methods);
        var propertiesString = properties.isEmpty() ? "" : String.join("\n  - ", properties);

        var methodsListString = methodsString.isEmpty() ? "" : " \n  methods: \n  - " + methodsString;
        var propertiesListString = propertiesString.isEmpty() ? "" : " \n  properties: \n  - " + propertiesString;

        return String.format("%s: %s%s", name, methodsListString, propertiesListString);
    }
}