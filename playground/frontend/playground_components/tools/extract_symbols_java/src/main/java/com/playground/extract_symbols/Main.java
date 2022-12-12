package com.playground.extract_symbols;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws IOException {
        var sdkPath = args[0];
        HashMap<String, ClassInfo> classInfoMap = collectClassInfo(sdkPath);
        String yamlString = buildYamlString(classInfoMap);
        System.out.println(yamlString);
    }

    private static HashMap<String, ClassInfo> collectClassInfo(String sdkPath) throws IOException {
        var classInfoMap = new HashMap<String, ClassInfo>();
        var paths = new File(sdkPath).toPath().toAbsolutePath();
        Files.walk(paths).forEach(path -> {
            var stringPath = path.toString();
            if (stringPath.endsWith(".java") && !stringPath.contains("test")) {
                var fileName = stringPath.substring(stringPath.lastIndexOf("/") + 1).replace(".java", "");
                try {
                    var unit = StaticJavaParser.parse(path);
                    if (unit.getClassByName(fileName).isPresent()) {
                        buildClass(classInfoMap, unit.getClassByName(fileName).get());
                    }
                } catch (IOException | ParseProblemException ignored) {
                }
            }
        });

        return classInfoMap;
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

        classInfo.name = cl.getNameAsString();
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

    private static String buildYamlString(HashMap<String, ClassInfo> classInfoMap) throws JsonProcessingException {
        ObjectMapper mapper = buildObjectMapper();
        StringBuilder result = new StringBuilder();

        for (var classInfo : classInfoMap.values()) {
            result.append(mapper.writeValueAsString(classInfo).substring(4).replace("\"", ""));
        }
        return result.toString();
    }

    private static ObjectMapper buildObjectMapper() {
        var classInfoSerializer = new ClassInfoSerializer(ClassInfo.class);
        var factory = new YAMLFactory();
        var mapper = new ObjectMapper(factory);
        var simpleModule = new SimpleModule("ClassInfoSerializer");
        simpleModule.addSerializer(classInfoSerializer);
        mapper.registerModule(simpleModule);
        return mapper;
    }
}