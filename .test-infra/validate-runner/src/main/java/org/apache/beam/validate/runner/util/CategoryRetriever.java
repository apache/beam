package org.apache.beam.validate.runner.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CategoryRetriever {
    public static List<String> getCategories(String className, String methodName) {
        List<String> categoryNames = new ArrayList<>();
        try {
            Class tempClass = Class.forName(className);
            for (Method method : tempClass.getDeclaredMethods()) {
                if(method.getName().equals(methodName)) {
                    Annotation[] annotations = method.getAnnotations();
                    for(Annotation annotation : annotations) {
                        if (annotation instanceof org.junit.experimental.categories.Category) {
                            Class<?>[] categories = null;
                            categories = ((org.junit.experimental.categories.Category) annotation).value();
                            Arrays.stream(categories).forEach(category -> categoryNames.add(category.getName()));
                        } else {
                            continue;
                        }
                        return categoryNames;
                    }
                } else {
                    continue;
                }
            }
        } catch (ClassNotFoundException e) {
            return categoryNames;
        }
        return new ArrayList<>();
    }
}
