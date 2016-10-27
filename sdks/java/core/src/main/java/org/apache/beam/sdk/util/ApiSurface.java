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
package org.apache.beam.sdk.util;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the API surface of a package prefix. Used for accessing public classes,
 * methods, and the types they reference, to control what dependencies are re-exported.
 *
 * <p>For the purposes of calculating the public API surface, exposure includes any public
 * or protected occurrence of:
 *
 * <ul>
 * <li>superclasses
 * <li>interfaces implemented
 * <li>actual type arguments to generic types
 * <li>array component types
 * <li>method return types
 * <li>method parameter types
 * <li>type variable bounds
 * <li>wildcard bounds
 * </ul>
 *
 * <p>Exposure is a transitive property. The resulting map excludes primitives
 * and array classes themselves.
 *
 * <p>It is prudent (though not required) to prune prefixes like "java" via the builder
 * method {@link #pruningPrefix} to halt the traversal so it does not uselessly catalog references
 * that are not interesting.
 */
@SuppressWarnings("rawtypes")
public class ApiSurface {
  private static Logger logger = LoggerFactory.getLogger(ApiSurface.class);

  /**
   * Returns an empty {@link ApiSurface}.
   */
  public static ApiSurface empty() {
    logger.debug("Returning an empty ApiSurface");
    return new ApiSurface(Collections.<Class<?>>emptySet(), Collections.<Pattern>emptySet());
  }

  /**
   * Returns an {@link ApiSurface} object representing the given package and all subpackages.
   */
  public static ApiSurface ofPackage(String packageName) throws IOException {
    return ApiSurface.empty().includingPackage(packageName);
  }

  /**
   * Returns an {@link ApiSurface} object representing just the surface of the given class.
   */
  public static ApiSurface ofClass(Class<?> clazz) {
    return ApiSurface.empty().includingClass(clazz);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but also including the named
   * package and all of its subpackages.
   */
  public ApiSurface includingPackage(String packageName) throws IOException {
    ClassPath classPath = ClassPath.from(ClassLoader.getSystemClassLoader());

    Set<Class<?>> newRootClasses = Sets.newHashSet();
    for (ClassInfo classInfo : classPath.getTopLevelClassesRecursive(packageName)) {
      Class clazz = classInfo.load();
      if (exposed(clazz.getModifiers())) {
        newRootClasses.add(clazz);
      }
    }
    logger.debug("Including package {} and subpackages: {}", packageName, newRootClasses);
    newRootClasses.addAll(rootClasses);

    return new ApiSurface(newRootClasses, patternsToPrune);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but also including the given class.
   */
  public ApiSurface includingClass(Class<?> clazz) {
    Set<Class<?>> newRootClasses = Sets.newHashSet();
    logger.debug("Including class {}", clazz);
    newRootClasses.add(clazz);
    newRootClasses.addAll(rootClasses);
    return new ApiSurface(newRootClasses, patternsToPrune);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning transitive
   * references from classes whose full name (including package) begins with the provided prefix.
   */
  public ApiSurface pruningPrefix(String prefix) {
    return pruningPattern(Pattern.compile(Pattern.quote(prefix) + ".*"));
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning references from the named
   * class.
   */
  public ApiSurface pruningClassName(String className) {
    return pruningPattern(Pattern.compile(Pattern.quote(className)));
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning references from the
   * provided class.
   */
  public ApiSurface pruningClass(Class<?> clazz) {
    return pruningClassName(clazz.getName());
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning transitive
   * references from classes whose full name (including package) begins with the provided prefix.
   */
  public ApiSurface pruningPattern(Pattern pattern) {
    Set<Pattern> newPatterns = Sets.newHashSet();
    newPatterns.addAll(patternsToPrune);
    newPatterns.add(pattern);
    return new ApiSurface(rootClasses, newPatterns);
  }

  /**
   * See {@link #pruningPattern(Pattern)}.
   */
  public ApiSurface pruningPattern(String patternString) {
    return pruningPattern(Pattern.compile(patternString));
  }

  /**
   * Returns all public classes originally belonging to the package
   * in the {@link ApiSurface}.
   */
  public Set<Class<?>> getRootClasses() {
    return rootClasses;
  }

  /**
   * Returns exposed types in this set, including arrays and primitives as
   * specified.
   */
  public Set<Class<?>> getExposedClasses() {
    return getExposedToExposers().keySet();
  }

  /**
   * Returns a path from an exposed class to a root class. There may be many, but this
   * gives only one.
   *
   * <p>If there are only cycles, with no path back to a root class, throws
   * IllegalStateException.
   */
  public List<Class<?>> getAnyExposurePath(Class<?> exposedClass) {
    Set<Class<?>> excluded = Sets.newHashSet();
    excluded.add(exposedClass);
    List<Class<?>> path = getAnyExposurePath(exposedClass, excluded);
    if (path == null) {
      throw new IllegalArgumentException(
          "Class " + exposedClass + " has no path back to any root class."
          + " It should never have been considered exposed.");
    } else {
      return path;
    }
  }

  /**
   * Returns a path from an exposed class to a root class. There may be many, but this
   * gives only one. It will not return a path that crosses the excluded classes.
   *
   * <p>If there are only cycles or paths through the excluded classes, returns null.
   *
   * <p>If the class is not actually in the exposure map, throws IllegalArgumentException
   */
  private List<Class<?>> getAnyExposurePath(Class<?> exposedClass, Set<Class<?>> excluded) {
    List<Class<?>> exposurePath = Lists.newArrayList();
    exposurePath.add(exposedClass);

    Collection<Class<?>> exposers = getExposedToExposers().get(exposedClass);
    if (exposers.isEmpty()) {
      throw new IllegalArgumentException("Class " + exposedClass + " is not exposed.");
    }

    for (Class<?> exposer : exposers) {
      if (excluded.contains(exposer)) {
        continue;
      }

      // A null exposer means this is already a root class.
      if (exposer == null) {
        return exposurePath;
      }

      List<Class<?>> restOfPath = getAnyExposurePath(
          exposer,
          Sets.union(excluded, Sets.newHashSet(exposer)));

      if (restOfPath != null) {
        exposurePath.addAll(restOfPath);
        return exposurePath;
      }
    }
    return null;
  }

  ////////////////////////////////////////////////////////////////////

  // Fields initialized upon construction
  private final Set<Class<?>> rootClasses;
  private final Set<Pattern> patternsToPrune;

  // Fields computed on-demand
  private Multimap<Class<?>, Class<?>> exposedToExposers = null;
  private Pattern prunedPattern = null;
  private Set<Type> visited = null;

  private ApiSurface(Set<Class<?>> rootClasses, Set<Pattern> patternsToPrune) {
    this.rootClasses = rootClasses;
    this.patternsToPrune = patternsToPrune;
  }

  /**
   * A map from exposed types to place where they are exposed, in the sense of being a part
   * of a public-facing API surface.
   *
   * <p>This map is the adjencency list representation of a directed graph, where an edge from type
   * {@code T1} to type {@code T2} indicates that {@code T2} directly exposes {@code T1} in its API
   * surface.
   *
   * <p>The traversal methods in this class are designed to avoid repeatedly processing types, since
   * there will almost always be cyclic references.
   */
  private Multimap<Class<?>, Class<?>> getExposedToExposers() {
    if (exposedToExposers == null) {
      constructExposedToExposers();
    }
    return exposedToExposers;
  }

  /**
   * See {@link #getExposedToExposers}.
   */
  private void constructExposedToExposers() {
    visited = Sets.newHashSet();
    exposedToExposers = Multimaps.newSetMultimap(
        Maps.<Class<?>, Collection<Class<?>>>newHashMap(),
        new Supplier<Set<Class<?>>>() {
          @Override
          public Set<Class<?>> get() {
            return Sets.newHashSet();
          }
        });

    for (Class<?> clazz : rootClasses) {
      addExposedTypes(clazz, null);
    }
  }

  /**
   * A combined {@code Pattern} that implements all the pruning specified.
   */
  private Pattern getPrunedPattern() {
    if (prunedPattern == null) {
      constructPrunedPattern();
    }
    return prunedPattern;
  }

  /**
   * See {@link #getPrunedPattern}.
   */
  private void constructPrunedPattern() {
    Set<String> prunedPatternStrings = Sets.newHashSet();
    for (Pattern patternToPrune : patternsToPrune) {
      prunedPatternStrings.add(patternToPrune.pattern());
    }
    prunedPattern = Pattern.compile("(" + Joiner.on(")|(").join(prunedPatternStrings) + ")");
  }

  /**
   * Whether a type and all that it references should be pruned from the graph.
   */
  private boolean pruned(Type type) {
    return pruned(TypeToken.of(type).getRawType());
  }

  /**
   * Whether a class and all that it references should be pruned from the graph.
   */
  private boolean pruned(Class<?> clazz) {
    return clazz.isPrimitive()
        || clazz.isArray()
        || getPrunedPattern().matcher(clazz.getName()).matches();
  }

  /**
   * Whether a type has already beens sufficiently processed.
   */
  private boolean done(Type type) {
    return visited.contains(type);
  }

  private void recordExposure(Class<?> exposed, Class<?> cause) {
    exposedToExposers.put(exposed, cause);
  }

  private void recordExposure(Type exposed, Class<?> cause) {
    exposedToExposers.put(TypeToken.of(exposed).getRawType(), cause);
  }

  private void visit(Type type) {
    visited.add(type);
  }

  /**
   * See {@link #addExposedTypes(Type, Class)}.
   */
  private void addExposedTypes(TypeToken type, Class<?> cause) {
    logger.debug(
        "Adding exposed types from {}, which is the type in type token {}", type.getType(), type);
    addExposedTypes(type.getType(), cause);
  }

  /**
   * Adds any references learned by following a link from {@code cause} to {@code type}.
   * This will dispatch according to the concrete {@code Type} implementation. See the
   * other overloads of {@code addExposedTypes} for their details.
   */
  private void addExposedTypes(Type type, Class<?> cause) {
    if (type instanceof TypeVariable) {
      logger.debug("Adding exposed types from {}, which is a type variable", type);
      addExposedTypes((TypeVariable) type, cause);
    } else if (type instanceof WildcardType) {
      logger.debug("Adding exposed types from {}, which is a wildcard type", type);
      addExposedTypes((WildcardType) type, cause);
    } else if (type instanceof GenericArrayType) {
      logger.debug("Adding exposed types from {}, which is a generic array type", type);
      addExposedTypes((GenericArrayType) type, cause);
    } else if (type instanceof ParameterizedType) {
      logger.debug("Adding exposed types from {}, which is a parameterized type", type);
      addExposedTypes((ParameterizedType) type, cause);
    } else if (type instanceof Class) {
      logger.debug("Adding exposed types from {}, which is a class", type);
      addExposedTypes((Class) type, cause);
    } else {
      throw new IllegalArgumentException("Unknown implementation of Type");
    }
  }

  /**
   * Adds any types exposed to this set. These will
   * come from the (possibly absent) bounds on the
   * type variable.
   */
  private void addExposedTypes(TypeVariable type, Class<?> cause) {
    if (done(type)) {
      return;
    }
    visit(type);
    for (Type bound : type.getBounds()) {
      logger.debug("Adding exposed types from {}, which is a type bound on {}", bound, type);
      addExposedTypes(bound, cause);
    }
  }

  /**
   * Adds any types exposed to this set. These will come from the (possibly absent) bounds on the
   * wildcard.
   */
  private void addExposedTypes(WildcardType type, Class<?> cause) {
    visit(type);
    for (Type lowerBound : type.getLowerBounds()) {
      logger.debug(
          "Adding exposed types from {}, which is a type lower bound on wildcard type {}",
          lowerBound,
          type);
      addExposedTypes(lowerBound, cause);
    }
    for (Type upperBound : type.getUpperBounds()) {
      logger.debug(
          "Adding exposed types from {}, which is a type upper bound on wildcard type {}",
          upperBound,
          type);
      addExposedTypes(upperBound, cause);
    }
  }

  /**
   * Adds any types exposed from the given array type. The array type itself is not added. The
   * cause of the exposure of the underlying type is considered whatever type exposed the array
   * type.
   */
  private void addExposedTypes(GenericArrayType type, Class<?> cause) {
    if (done(type)) {
      return;
    }
    visit(type);
    logger.debug(
        "Adding exposed types from {}, which is the component type on generic array type {}",
        type.getGenericComponentType(),
        type);
    addExposedTypes(type.getGenericComponentType(), cause);
  }

  /**
   * Adds any types exposed to this set. Even if the
   * root type is to be pruned, the actual type arguments
   * are processed.
   */
  private void addExposedTypes(ParameterizedType type, Class<?> cause) {
    // Even if the type is already done, this link to it may be new
    boolean alreadyDone = done(type);
    if (!pruned(type)) {
      visit(type);
      recordExposure(type, cause);
    }
    if (alreadyDone) {
      return;
    }

    // For a parameterized type, pruning does not take place
    // here, only for the raw class.
    // The type parameters themselves may not be pruned,
    // for example with List<MyApiType> probably the
    // standard List is pruned, but MyApiType is not.
    logger.debug(
        "Adding exposed types from {}, which is the raw type on parameterized type {}",
        type.getRawType(),
        type);
    addExposedTypes(type.getRawType(), cause);
    for (Type typeArg : type.getActualTypeArguments()) {
      logger.debug(
          "Adding exposed types from {}, which is a type argument on parameterized type {}",
          typeArg,
          type);
      addExposedTypes(typeArg, cause);
    }
  }

  /**
   * Adds a class and all of the types it exposes. The cause
   * of the class being exposed is given, and the cause
   * of everything within the class is that class itself.
   */
  private void addExposedTypes(Class<?> clazz, Class<?> cause) {
    if (pruned(clazz)) {
      return;
    }
    // Even if `clazz` has been visited, the link from `cause` may be new
    boolean alreadyDone = done(clazz);
    visit(clazz);
    recordExposure(clazz, cause);
    if (alreadyDone || pruned(clazz)) {
      return;
    }

    TypeToken<?> token = TypeToken.of(clazz);
    for (TypeToken<?> superType : token.getTypes()) {
      if (!superType.equals(token)) {
        logger.debug(
            "Adding exposed types from {}, which is a super type token on {}", superType, clazz);
        addExposedTypes(superType, clazz);
      }
    }
    for (Class innerClass : clazz.getDeclaredClasses()) {
      if (exposed(innerClass.getModifiers())) {
        logger.debug(
            "Adding exposed types from {}, which is an exposed inner class of {}",
            innerClass,
            clazz);
        addExposedTypes(innerClass, clazz);
      }
    }
    for (Field field : clazz.getDeclaredFields()) {
      if (exposed(field.getModifiers())) {
        logger.debug("Adding exposed types from {}, which is an exposed field on {}", field, clazz);
        addExposedTypes(field, clazz);
      }
    }
    for (Invokable invokable : getExposedInvokables(token)) {
      logger.debug(
          "Adding exposed types from {}, which is an exposed invokable on {}", invokable, clazz);
      addExposedTypes(invokable, clazz);
    }
  }

  private void addExposedTypes(Invokable<?, ?> invokable, Class<?> cause) {
    addExposedTypes(invokable.getReturnType(), cause);
    for (Annotation annotation : invokable.getAnnotations()) {
      logger.debug(
          "Adding exposed types from {}, which is an annotation on invokable {}",
          annotation,
          invokable);
     addExposedTypes(annotation.annotationType(), cause);
    }
    for (Parameter parameter : invokable.getParameters()) {
      logger.debug(
          "Adding exposed types from {}, which is a parameter on invokable {}",
          parameter,
          invokable);
      addExposedTypes(parameter, cause);
    }
    for (TypeToken<?> exceptionType : invokable.getExceptionTypes()) {
      logger.debug(
          "Adding exposed types from {}, which is an exception type on invokable {}",
          exceptionType,
          invokable);
      addExposedTypes(exceptionType, cause);
    }
  }

  private void addExposedTypes(Parameter parameter, Class<?> cause) {
    logger.debug(
        "Adding exposed types from {}, which is the type of parameter {}",
        parameter.getType(),
        parameter);
    addExposedTypes(parameter.getType(), cause);
    for (Annotation annotation : parameter.getAnnotations()) {
      logger.debug(
          "Adding exposed types from {}, which is an annotation on parameter {}",
          annotation,
          parameter);
      addExposedTypes(annotation.annotationType(), cause);
    }
  }

  private void addExposedTypes(Field field, Class<?> cause) {
    addExposedTypes(field.getGenericType(), cause);
    for (Annotation annotation : field.getDeclaredAnnotations()) {
      logger.debug(
          "Adding exposed types from {}, which is an annotation on field {}", annotation, field);
      addExposedTypes(annotation.annotationType(), cause);
    }
  }

  /**
   * Returns an {@link Invokable} for each public methods or constructors of a type.
   */
  private Set<Invokable> getExposedInvokables(TypeToken<?> type) {
    Set<Invokable> invokables = Sets.newHashSet();

    for (Constructor constructor : type.getRawType().getConstructors()) {
      if (0 != (constructor.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED))) {
        invokables.add(type.constructor(constructor));
      }
    }

    for (Method method : type.getRawType().getMethods()) {
      if (0 != (method.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED))) {
        invokables.add(type.method(method));
      }
    }

    return invokables;
  }

  /**
   * Returns true of the given modifier bitmap indicates exposure (public or protected access).
   */
  private boolean exposed(int modifiers) {
    return 0 != (modifiers & (Modifier.PUBLIC | Modifier.PROTECTED));
  }


  ////////////////////////////////////////////////////////////////////////////

  /**
   * All classes transitively reachable via only public method signatures of the SDK.
   *
   * <p>Note that our idea of "public" does not include various internal-only APIs.
   */
  public static ApiSurface getSdkApiSurface() throws IOException {
    return ApiSurface.ofPackage("org.apache.beam")
        .pruningPattern("org[.]apache[.]beam[.].*Test")

        // Exposes Guava, but not intended for users
        .pruningClassName("org.apache.beam.sdk.util.common.ReflectHelpers")
        .pruningPrefix("java");
  }

  public static void main(String[] args) throws Exception {
    List<String> names = Lists.newArrayList();
    for (Class clazz : getSdkApiSurface().getExposedClasses()) {
      names.add(clazz.getName());
    }
    List<String> sortedNames = Lists.newArrayList(names);
    Collections.sort(sortedNames);

    for (String name : sortedNames) {
      System.out.println(name);
    }
  }
}
