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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

/** Helpers for extracting the name of objects and classes. */
@Internal
public class NameUtils {

  /** Classes may implement this interface to change how names are generated for their instances. */
  public interface NameOverride {
    /** Return the name to use for this instance. */
    String getNameOverride();
  }

  private static final String[] STANDARD_NAME_SUFFIXES = new String[] {"DoFn", "CombineFn", "Fn"};

  /**
   * Pattern to match a non-anonymous inner class. Eg, matches "Foo$Bar", or even "Foo$1$Bar", but
   * not "Foo$1" or "Foo$1$2".
   */
  private static final Pattern NAMED_INNER_CLASS = Pattern.compile(".+\\$(?<INNER>[^0-9].*)");

  private static final String ANONYMOUS_CLASS_REGEX = "\\$[0-9]+\\$";

  private static String approximateSimpleName(Class<?> clazz, boolean dropOuterClassNames) {
    checkArgument(!clazz.isAnonymousClass(), "Attempted to get simple name of anonymous class");
    return approximateSimpleName(clazz.getName(), dropOuterClassNames);
  }

  @VisibleForTesting
  static String approximateSimpleName(String fullName, boolean dropOuterClassNames) {
    String shortName = fullName.substring(fullName.lastIndexOf('.') + 1);

    // Drop common suffixes for each named component.
    String[] names = shortName.split("\\$");
    for (int i = 0; i < names.length; i++) {
      names[i] = simplifyNameComponent(names[i]);
    }
    shortName = Joiner.on('$').join(names);

    if (dropOuterClassNames) {
      // Simplify inner class name by dropping outer class prefixes.
      Matcher m = NAMED_INNER_CLASS.matcher(shortName);
      if (m.matches()) {
        shortName = m.group("INNER");
      }
    } else {
      // Dropping anonymous outer classes
      shortName = shortName.replaceAll(ANONYMOUS_CLASS_REGEX, ".");
      shortName = shortName.replaceAll("\\$", ".");
    }
    return shortName;
  }

  private static String simplifyNameComponent(String name) {
    for (String suffix : STANDARD_NAME_SUFFIXES) {
      if (name.endsWith(suffix) && name.length() > suffix.length()) {
        return name.substring(0, name.length() - suffix.length());
      }
    }
    return name;
  }

  /**
   * As {@link #approximateSimpleName(Object, String)} but returning {@code "Anonymous"} when {@code
   * object} is an instance of anonymous class.
   */
  public static String approximateSimpleName(Object object) {
    return approximateSimpleName(object, "Anonymous");
  }

  /**
   * Returns a simple name describing a class that is being used as a function (eg., a {@link DoFn}
   * or {@link CombineFn}, etc.).
   *
   * <p>Note: this is non-invertible - the name may be simplified to an extent that it cannot be
   * mapped back to the original class.
   *
   * <p>This can be used to generate human-readable names. It removes the package and outer classes
   * from the name, and removes common suffixes.
   *
   * <p>If the object is an instanceof {@link NameOverride}, the result of {@link
   * NameOverride#getNameOverride()} is returned. This allows classes that act as wrappers to
   * override the handling of names by delegating to the objects they wrap.
   *
   * <p>If the class is anonymous, the string {@code anonymousValue} is returned.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code some.package.Word.SummaryDoFn} becomes "Summary"
   *   <li>{@code another.package.PairingFn} becomes "Pairing"
   * </ul>
   */
  public static String approximateSimpleName(Object object, String anonymousValue) {
    if (object instanceof NameOverride) {
      return ((NameOverride) object).getNameOverride();
    }

    Class<?> clazz;
    if (object instanceof Class) {
      clazz = (Class<?>) object;
    } else {
      clazz = object.getClass();
    }
    if (clazz.isAnonymousClass()) {
      return anonymousValue;
    }

    return approximateSimpleName(clazz, /* dropOuterClassNames */ true);
  }

  /**
   * Returns a name for a PTransform class.
   *
   * <p>This can be used to generate human-readable transform names. It removes the package from the
   * name, and removes common suffixes.
   *
   * <p>It is different than approximateSimpleName:
   *
   * <ul>
   *   <li>1. It keeps the outer classes names.
   *   <li>2. It removes the common transform inner class: "Bound".
   *   <li>3. For classes generated by AutoValue, whose names start with AutoValue_, it delegates to
   *       the (parent) class declared in the user's source code.
   * </ul>
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code some.package.Word.Summary} becomes "Word.Summary"
   *   <li>{@code another.package.Pairing.Bound} becomes "Pairing"
   * </ul>
   */
  public static String approximatePTransformName(Class<?> clazz) {
    checkArgument(PTransform.class.isAssignableFrom(clazz));
    if (clazz.getSimpleName().startsWith("AutoValue_")) {
      return approximatePTransformName(clazz.getSuperclass());
    }
    return approximateSimpleName(clazz, /* dropOuterClassNames */ false)
        .replaceFirst("\\.Bound$", "");
  }
}
