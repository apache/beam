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

import static org.hamcrest.Matchers.anyOf;

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
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimaps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.ClassPath;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.Invokable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.Parameter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.TypeToken;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the API surface of a package prefix. Used for accessing public classes, methods, and
 * the types they reference, to control what dependencies are re-exported.
 *
 * <p>For the purposes of calculating the public API surface, exposure includes any public or
 * protected occurrence of:
 *
 * <ul>
 *   <li>superclasses
 *   <li>interfaces implemented
 *   <li>actual type arguments to generic types
 *   <li>array component types
 *   <li>method return types
 *   <li>method parameter types
 *   <li>type variable bounds
 *   <li>wildcard bounds
 * </ul>
 *
 * <p>Exposure is a transitive property. The resulting map excludes primitives and array classes
 * themselves.
 *
 * <p>It is prudent (though not required) to prune prefixes like "java" via the builder method
 * {@link #pruningPrefix} to halt the traversal so it does not uselessly catalog references that are
 * not interesting.
 */
@SuppressWarnings("rawtypes")
@Internal
public class ApiSurface {
  private static final Logger LOG = LoggerFactory.getLogger(ApiSurface.class);

  /** A factory method to create a {@link Class} matcher for classes residing in a given package. */
  public static Matcher<Class<?>> classesInPackage(final String packageName) {
    return new Matchers.ClassInPackage(packageName);
  }

  /**
   * A factory method to create an {@link ApiSurface} matcher, producing a positive match if the
   * queried api surface contains ONLY classes described by the provided matchers.
   */
  public static Matcher<ApiSurface> containsOnlyClassesMatching(
      final Set<Matcher<Class<?>>> classMatchers) {
    return new Matchers.ClassesInSurfaceMatcher(classMatchers);
  }

  /** See {@link ApiSurface#containsOnlyClassesMatching(Set)}. */
  @SafeVarargs
  public static Matcher<ApiSurface> containsOnlyClassesMatching(
      final Matcher<Class<?>>... classMatchers) {
    return new Matchers.ClassesInSurfaceMatcher(Sets.newHashSet(classMatchers));
  }

  /** See {@link ApiSurface#containsOnlyPackages(Set)}. */
  public static Matcher<ApiSurface> containsOnlyPackages(final String... packageNames) {
    return containsOnlyPackages(Sets.newHashSet(packageNames));
  }

  /**
   * A factory method to create an {@link ApiSurface} matcher, producing a positive match if the
   * queried api surface contains classes ONLY from specified package names.
   */
  public static Matcher<ApiSurface> containsOnlyPackages(final Set<String> packageNames) {

    final Function<String, Matcher<Class<?>>> packageNameToClassMatcher =
        ApiSurface::classesInPackage;

    final ImmutableSet<Matcher<Class<?>>> classesInPackages =
        FluentIterable.from(packageNames).transform(packageNameToClassMatcher).toSet();

    return containsOnlyClassesMatching(classesInPackages);
  }

  /**
   * {@link Matcher}s for use in {@link ApiSurface} related tests that aim to keep the public API
   * conformant to a hard-coded policy by controlling what classes are allowed to be exposed by an
   * API surface.
   */
  // based on previous code by @kennknowles and others.
  private static class Matchers {

    private static class ClassInPackage extends TypeSafeDiagnosingMatcher<Class<?>> {

      private final String packageName;

      private ClassInPackage(final String packageName) {
        this.packageName = packageName;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("Classes in package \"");
        description.appendText(packageName);
        description.appendText("\"");
      }

      @Override
      protected boolean matchesSafely(final Class<?> clazz, final Description mismatchDescription) {
        return clazz.getName().startsWith(packageName + ".");
      }
    }

    private static class ClassesInSurfaceMatcher extends TypeSafeDiagnosingMatcher<ApiSurface> {

      private final Set<Matcher<Class<?>>> classMatchers;

      private ClassesInSurfaceMatcher(final Set<Matcher<Class<?>>> classMatchers) {
        this.classMatchers = classMatchers;
      }

      private boolean verifyNoAbandoned(
          final ApiSurface checkedApiSurface,
          final Set<Matcher<Class<?>>> allowedClasses,
          final Description mismatchDescription) {

        // <helper_lambdas>

        final Function<Matcher<Class<?>>, String> toMessage =
            abandonedClassMacther -> {
              final StringDescription description = new StringDescription();
              description.appendText("No ");
              abandonedClassMacther.describeTo(description);
              return description.toString();
            };

        final Predicate<Matcher<Class<?>>> matchedByExposedClasses =
            classMatcher ->
                FluentIterable.from(checkedApiSurface.getExposedClasses())
                    .anyMatch(classMatcher::matches);

        // </helper_lambdas>

        final ImmutableSet<Matcher<Class<?>>> matchedClassMatchers =
            FluentIterable.from(allowedClasses).filter(matchedByExposedClasses).toSet();

        final Sets.SetView<Matcher<Class<?>>> abandonedClassMatchers =
            Sets.difference(allowedClasses, matchedClassMatchers);

        final ImmutableList<String> messages =
            FluentIterable.from(abandonedClassMatchers)
                .transform(toMessage)
                .toSortedList(Ordering.natural());

        if (!messages.isEmpty()) {
          mismatchDescription.appendText(
              "The following allowed scopes did not have matching classes on the API surface:"
                  + "\n\t"
                  + Joiner.on("\n\t").join(messages));
        }

        return messages.isEmpty();
      }

      private boolean verifyNoDisallowed(
          final ApiSurface checkedApiSurface,
          final Set<Matcher<Class<?>>> allowedClasses,
          final Description mismatchDescription) {

        /* <helper_lambdas> */

        final Function<Class<?>, List<Class<?>>> toExposure = checkedApiSurface::getAnyExposurePath;

        final Maps.EntryTransformer<Class<?>, List<Class<?>>, String> toMessage =
            (aClass, exposure) ->
                aClass + " exposed via:\n\t\t" + Joiner.on("\n\t\t").join(exposure);

        final Predicate<Class<?>> disallowed = aClass -> !classIsAllowed(aClass, allowedClasses);

        /* </helper_lambdas> */

        final FluentIterable<Class<?>> disallowedClasses =
            FluentIterable.from(checkedApiSurface.getExposedClasses()).filter(disallowed);

        final ImmutableMap<Class<?>, List<Class<?>>> exposures =
            Maps.toMap(disallowedClasses, toExposure);

        final ImmutableList<String> messages =
            FluentIterable.from(Maps.transformEntries(exposures, toMessage).values())
                .toSortedList(Ordering.natural());

        if (!messages.isEmpty()) {
          mismatchDescription.appendText(
              "The following disallowed classes appeared on the API surface:\n\t"
                  + Joiner.on("\n\t").join(messages));
        }

        return messages.isEmpty();
      }

      @SuppressWarnings({"rawtypes", "unchecked"})
      private boolean classIsAllowed(
          final Class<?> clazz, final Set<Matcher<Class<?>>> allowedClasses) {
        // Safe cast inexpressible in Java without rawtypes
        return anyOf((Iterable) allowedClasses).matches(clazz);
      }

      @Override
      protected boolean matchesSafely(
          final ApiSurface apiSurface, final Description mismatchDescription) {
        final boolean noDisallowed =
            verifyNoDisallowed(apiSurface, classMatchers, mismatchDescription);

        final boolean noAbandoned =
            verifyNoAbandoned(apiSurface, classMatchers, mismatchDescription);

        return noDisallowed && noAbandoned;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("API surface to include only:" + "\n\t");
        for (final Matcher<Class<?>> classMatcher : classMatchers) {
          classMatcher.describeTo(description);
          description.appendText("\n\t");
        }
      }
    }
  }

  ///////////////

  /** Returns an empty {@link ApiSurface}. */
  public static ApiSurface empty() {
    LOG.debug("Returning an empty ApiSurface");
    return new ApiSurface(Collections.emptySet(), Collections.emptySet());
  }

  /** Returns an {@link ApiSurface} object representing the given package and all subpackages. */
  public static ApiSurface ofPackage(String packageName, ClassLoader classLoader)
      throws IOException {
    return ApiSurface.empty().includingPackage(packageName, classLoader);
  }

  /** Returns an {@link ApiSurface} object representing the given package and all subpackages. */
  public static ApiSurface ofPackage(Package aPackage, ClassLoader classLoader) throws IOException {
    return ofPackage(aPackage.getName(), classLoader);
  }

  /** Returns an {@link ApiSurface} object representing just the surface of the given class. */
  public static ApiSurface ofClass(Class<?> clazz) {
    return ApiSurface.empty().includingClass(clazz);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but also including the named package and all of
   * its subpackages.
   */
  public ApiSurface includingPackage(String packageName, ClassLoader classLoader)
      throws IOException {
    ClassPath classPath = ClassPath.from(classLoader);

    Set<Class<?>> newRootClasses = Sets.newHashSet();
    for (ClassPath.ClassInfo classInfo : classPath.getTopLevelClassesRecursive(packageName)) {
      Class clazz = null;
      try {
        clazz = classInfo.load();
      } catch (NoClassDefFoundError e) {
        // TODO: Ignore any NoClassDefFoundError errors as a workaround. (BEAM-2231)
        LOG.warn("Failed to load class: {}", classInfo.toString(), e);
        continue;
      }

      if (exposed(clazz.getModifiers())) {
        newRootClasses.add(clazz);
      }
    }
    LOG.debug("Including package {} and subpackages: {}", packageName, newRootClasses);
    newRootClasses.addAll(rootClasses);

    return new ApiSurface(newRootClasses, patternsToPrune);
  }

  /** Returns an {@link ApiSurface} like this one, but also including the given class. */
  public ApiSurface includingClass(Class<?> clazz) {
    Set<Class<?>> newRootClasses = Sets.newHashSet();
    LOG.debug("Including class {}", clazz);
    newRootClasses.add(clazz);
    newRootClasses.addAll(rootClasses);
    return new ApiSurface(newRootClasses, patternsToPrune);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning transitive references from classes
   * whose full name (including package) begins with the provided prefix.
   */
  public ApiSurface pruningPrefix(String prefix) {
    return pruningPattern(Pattern.compile(Pattern.quote(prefix) + ".*"));
  }

  /** Returns an {@link ApiSurface} like this one, but pruning references from the named class. */
  public ApiSurface pruningClassName(String className) {
    return pruningPattern(Pattern.compile(Pattern.quote(className)));
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning references from the provided class.
   */
  public ApiSurface pruningClass(Class<?> clazz) {
    return pruningClassName(clazz.getName());
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning transitive references from classes
   * whose full name (including package) begins with the provided prefix.
   */
  public ApiSurface pruningPattern(Pattern pattern) {
    Set<Pattern> newPatterns = Sets.newHashSet();
    newPatterns.addAll(patternsToPrune);
    newPatterns.add(pattern);
    return new ApiSurface(rootClasses, newPatterns);
  }

  /** See {@link #pruningPattern(Pattern)}. */
  public ApiSurface pruningPattern(String patternString) {
    return pruningPattern(Pattern.compile(patternString));
  }

  /** Returns all public classes originally belonging to the package in the {@link ApiSurface}. */
  public Set<Class<?>> getRootClasses() {
    return rootClasses;
  }

  /** Returns exposed types in this set, including arrays and primitives as specified. */
  public Set<Class<?>> getExposedClasses() {
    return getExposedToExposers().keySet();
  }

  /**
   * Returns a path from an exposed class to a root class. There may be many, but this gives only
   * one.
   *
   * <p>If there are only cycles, with no path back to a root class, throws IllegalStateException.
   */
  public List<Class<?>> getAnyExposurePath(Class<?> exposedClass) {
    Set<Class<?>> excluded = Sets.newHashSet();
    excluded.add(exposedClass);
    List<Class<?>> path = getAnyExposurePath(exposedClass, excluded);
    if (path == null) {
      throw new IllegalArgumentException(
          "Class "
              + exposedClass
              + " has no path back to any root class."
              + " It should never have been considered exposed.");
    } else {
      return path;
    }
  }

  /**
   * Returns a path from an exposed class to a root class. There may be many, but this gives only
   * one. It will not return a path that crosses the excluded classes.
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

      List<Class<?>> restOfPath =
          getAnyExposurePath(exposer, Sets.union(excluded, Sets.newHashSet(exposer)));

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
   * A map from exposed types to place where they are exposed, in the sense of being a part of a
   * public-facing API surface.
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

  /** See {@link #getExposedToExposers}. */
  private void constructExposedToExposers() {
    visited = Sets.newHashSet();
    exposedToExposers =
        Multimaps.newSetMultimap(
            Maps.<Class<?>, Collection<Class<?>>>newHashMap(), Sets::newHashSet);

    for (Class<?> clazz : rootClasses) {
      addExposedTypes(clazz, null);
    }
  }

  /** A combined {@code Pattern} that implements all the pruning specified. */
  private Pattern getPrunedPattern() {
    if (prunedPattern == null) {
      constructPrunedPattern();
    }
    return prunedPattern;
  }

  /** See {@link #getPrunedPattern}. */
  private void constructPrunedPattern() {
    Set<String> prunedPatternStrings = Sets.newHashSet();
    for (Pattern patternToPrune : patternsToPrune) {
      prunedPatternStrings.add(patternToPrune.pattern());
    }
    prunedPattern = Pattern.compile("(" + Joiner.on(")|(").join(prunedPatternStrings) + ")");
  }

  /** Whether a type and all that it references should be pruned from the graph. */
  private boolean pruned(Type type) {
    return pruned(TypeToken.of(type).getRawType());
  }

  /** Whether a class and all that it references should be pruned from the graph. */
  private boolean pruned(Class<?> clazz) {
    return clazz.isPrimitive()
        || clazz.isArray()
        || clazz.getCanonicalName().equals("jdk.internal.HotSpotIntrinsicCandidate")
        || getPrunedPattern().matcher(clazz.getName()).matches();
  }

  /** Whether a type has already beens sufficiently processed. */
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

  /** See {@link #addExposedTypes(Type, Class)}. */
  private void addExposedTypes(TypeToken type, Class<?> cause) {
    LOG.debug(
        "Adding exposed types from {}, which is the type in type token {}", type.getType(), type);
    addExposedTypes(type.getType(), cause);
  }

  /**
   * Adds any references learned by following a link from {@code cause} to {@code type}. This will
   * dispatch according to the concrete {@code Type} implementation. See the other overloads of
   * {@code addExposedTypes} for their details.
   */
  private void addExposedTypes(Type type, Class<?> cause) {
    if (type instanceof TypeVariable) {
      LOG.debug("Adding exposed types from {}, which is a type variable", type);
      addExposedTypes((TypeVariable) type, cause);
    } else if (type instanceof WildcardType) {
      LOG.debug("Adding exposed types from {}, which is a wildcard type", type);
      addExposedTypes((WildcardType) type, cause);
    } else if (type instanceof GenericArrayType) {
      LOG.debug("Adding exposed types from {}, which is a generic array type", type);
      addExposedTypes((GenericArrayType) type, cause);
    } else if (type instanceof ParameterizedType) {
      LOG.debug("Adding exposed types from {}, which is a parameterized type", type);
      addExposedTypes((ParameterizedType) type, cause);
    } else if (type instanceof Class) {
      LOG.debug("Adding exposed types from {}, which is a class", type);
      addExposedTypes((Class) type, cause);
    } else {
      throw new IllegalArgumentException("Unknown implementation of Type");
    }
  }

  /**
   * Adds any types exposed to this set. These will come from the (possibly absent) bounds on the
   * type variable.
   */
  private void addExposedTypes(TypeVariable type, Class<?> cause) {
    if (done(type)) {
      return;
    }
    visit(type);
    for (Type bound : type.getBounds()) {
      LOG.debug("Adding exposed types from {}, which is a type bound on {}", bound, type);
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
      LOG.debug(
          "Adding exposed types from {}, which is a type lower bound on wildcard type {}",
          lowerBound,
          type);
      addExposedTypes(lowerBound, cause);
    }
    for (Type upperBound : type.getUpperBounds()) {
      LOG.debug(
          "Adding exposed types from {}, which is a type upper bound on wildcard type {}",
          upperBound,
          type);
      addExposedTypes(upperBound, cause);
    }
  }

  /**
   * Adds any types exposed from the given array type. The array type itself is not added. The cause
   * of the exposure of the underlying type is considered whatever type exposed the array type.
   */
  private void addExposedTypes(GenericArrayType type, Class<?> cause) {
    if (done(type)) {
      return;
    }
    visit(type);
    LOG.debug(
        "Adding exposed types from {}, which is the component type on generic array type {}",
        type.getGenericComponentType(),
        type);
    addExposedTypes(type.getGenericComponentType(), cause);
  }

  /**
   * Adds any types exposed to this set. Even if the root type is to be pruned, the actual type
   * arguments are processed.
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
    LOG.debug(
        "Adding exposed types from {}, which is the raw type on parameterized type {}",
        type.getRawType(),
        type);
    addExposedTypes(type.getRawType(), cause);
    for (Type typeArg : type.getActualTypeArguments()) {
      LOG.debug(
          "Adding exposed types from {}, which is a type argument on parameterized type {}",
          typeArg,
          type);
      addExposedTypes(typeArg, cause);
    }
  }

  /**
   * Adds a class and all of the types it exposes. The cause of the class being exposed is given,
   * and the cause of everything within the class is that class itself.
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
        LOG.debug(
            "Adding exposed types from {}, which is a super type token on {}", superType, clazz);
        addExposedTypes(superType, clazz);
      }
    }
    for (Class innerClass : clazz.getDeclaredClasses()) {
      if (exposed(innerClass.getModifiers())) {
        LOG.debug(
            "Adding exposed types from {}, which is an exposed inner class of {}",
            innerClass,
            clazz);
        addExposedTypes(innerClass, clazz);
      }
    }
    for (Field field : clazz.getDeclaredFields()) {
      if (exposed(field.getModifiers())) {
        LOG.debug("Adding exposed types from {}, which is an exposed field on {}", field, clazz);
        addExposedTypes(field, clazz);
      }
    }
    for (Invokable invokable : getExposedInvokables(token)) {
      LOG.debug(
          "Adding exposed types from {}, which is an exposed invokable on {}", invokable, clazz);
      addExposedTypes(invokable, clazz);
    }
  }

  private void addExposedTypes(Invokable<?, ?> invokable, Class<?> cause) {
    addExposedTypes(invokable.getReturnType(), cause);
    for (Annotation annotation : invokable.getAnnotations()) {
      LOG.debug(
          "Adding exposed types from {}, which is an annotation on invokable {}",
          annotation,
          invokable);
      addExposedTypes(annotation.annotationType(), cause);
    }
    for (Parameter parameter : invokable.getParameters()) {
      LOG.debug(
          "Adding exposed types from {}, which is a parameter on invokable {}",
          parameter,
          invokable);
      addExposedTypes(parameter, cause);
    }
    for (TypeToken<?> exceptionType : invokable.getExceptionTypes()) {
      LOG.debug(
          "Adding exposed types from {}, which is an exception type on invokable {}",
          exceptionType,
          invokable);
      addExposedTypes(exceptionType, cause);
    }
  }

  private void addExposedTypes(Parameter parameter, Class<?> cause) {
    LOG.debug(
        "Adding exposed types from {}, which is the type of parameter {}",
        parameter.getType(),
        parameter);
    addExposedTypes(parameter.getType(), cause);
    for (Annotation annotation : parameter.getAnnotations()) {
      LOG.debug(
          "Adding exposed types from {}, which is an annotation on parameter {}",
          annotation,
          parameter);
      addExposedTypes(annotation.annotationType(), cause);
    }
  }

  private void addExposedTypes(Field field, Class<?> cause) {
    addExposedTypes(field.getGenericType(), cause);
    for (Annotation annotation : field.getDeclaredAnnotations()) {
      LOG.debug(
          "Adding exposed types from {}, which is an annotation on field {}", annotation, field);
      addExposedTypes(annotation.annotationType(), cause);
    }
  }

  /** Returns an {@link Invokable} for each public methods or constructors of a type. */
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

  /** Returns true of the given modifier bitmap indicates exposure (public or protected access). */
  private boolean exposed(int modifiers) {
    return 0 != (modifiers & (Modifier.PUBLIC | Modifier.PROTECTED));
  }

  ////////////////////////////////////////////////////////////////////////////

}
