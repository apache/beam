package com.playground.extract_symbols;

import java.util.HashSet;
import java.util.Set;

public class ClassInfo {
    String name = "";
    Set<String> publicMethods = new HashSet<>();
    Set<String> publicFields = new HashSet<>();
}
