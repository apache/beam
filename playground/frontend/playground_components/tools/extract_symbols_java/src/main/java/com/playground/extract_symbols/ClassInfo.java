package com.playground.extract_symbols;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClassInfo {
    Set<String> publicMethods = new HashSet<>();
    Set<String> publicFields = new HashSet<>();

    Map<String, Set<String>> toMap() {
        Map<String, Set<String>> map = new HashMap<>();
        if (!publicMethods.isEmpty()) {
            map.put("methods", publicMethods);
        }
        if (!publicFields.isEmpty()) {
            map.put("properties", publicFields);
        }
        return map;
    }
}
