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
package org.apache.beam.sdk.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s to use Regular Expressions to process elements in a {@link PCollection}.
 *
 * <p>{@link Regex#matches(String, int)} can be used to see if an entire line matches a Regex.
 * {@link Regex#matchesKV(String, int, int)} can be used to see if an entire line matches a Regex
 * and output certain groups as a {@link KV}.
 *
 * <p>{@link Regex#find(String, int)} can be used to see if a portion of a line matches a Regex.
 * {@link Regex#matchesKV(String, int, int)} can be used to see if a portion of a line matches a
 * Regex and output certain groups as a {@link KV}.
 *
 * <p>Lines that do not match the Regex will not be output.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class Regex {
  private Regex() {
    // do not instantiate
  }

  /**
   * Returns a {@link Regex.Matches} {@link PTransform} that checks if the entire line matches the
   * Regex. Returns the entire line (group 0) as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   */
  public static Matches matches(String regex) {
    return matches(regex, 0);
  }

  /**
   * Returns a {@link Regex.Matches} {@link PTransform} that checks if the entire line matches the
   * Regex. Returns the entire line (group 0) as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   */
  public static Matches matches(Pattern pattern) {
    return matches(pattern, 0);
  }

  /**
   * Returns a {@link Regex.Matches} {@link PTransform} that checks if the entire line matches the
   * Regex. Returns the group as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param group The Regex group to return as a PCollection
   */
  public static Matches matches(String regex, int group) {
    return matches(Pattern.compile(regex), group);
  }

  /**
   * Returns a {@link Regex.Matches} {@link PTransform} that checks if the entire line matches the
   * Regex. Returns the group as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param group The Regex group to return as a PCollection
   */
  public static Matches matches(Pattern pattern, int group) {
    return new Matches(pattern, group);
  }

  /**
   * Returns a {@link Regex.MatchesName} {@link PTransform} that checks if the entire line matches
   * the Regex. Returns the group as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param groupName The Regex group name to return as a PCollection
   */
  public static MatchesName matches(String regex, String groupName) {
    return matches(Pattern.compile(regex), groupName);
  }

  /**
   * Returns a {@link Regex.MatchesName} {@link PTransform} that checks if the entire line matches
   * the Regex. Returns the group as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param groupName The Regex group name to return as a PCollection
   */
  public static MatchesName matches(Pattern pattern, String groupName) {
    return new MatchesName(pattern, groupName);
  }

  /**
   * Returns a {@link Regex.AllMatches} {@link PTransform} that checks if the entire line matches
   * the Regex. Returns all groups as a List&lt;String&gt; in a {@link PCollection}.
   *
   * @param regex The regular expression to run
   */
  public static AllMatches allMatches(String regex) {
    return allMatches(Pattern.compile(regex));
  }

  /**
   * Returns a {@link Regex.AllMatches} {@link PTransform} that checks if the entire line matches
   * the Regex. Returns all groups as a List&lt;String&gt; in a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   */
  public static AllMatches allMatches(Pattern pattern) {
    return new AllMatches(pattern);
  }

  /**
   * Returns a {@link Regex.MatchesKV} {@link PTransform} that checks if the entire line matches the
   * Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param keyGroup The Regex group to use as the key
   * @param valueGroup The Regex group to use the value
   */
  public static MatchesKV matchesKV(String regex, int keyGroup, int valueGroup) {
    return matchesKV(Pattern.compile(regex), keyGroup, valueGroup);
  }

  /**
   * Returns a {@link Regex.MatchesKV} {@link PTransform} that checks if the entire line matches the
   * Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param keyGroup The Regex group to use as the key
   * @param valueGroup The Regex group to use the value
   */
  public static MatchesKV matchesKV(Pattern pattern, int keyGroup, int valueGroup) {
    return new MatchesKV(pattern, keyGroup, valueGroup);
  }

  /**
   * Returns a {@link Regex.MatchesNameKV} {@link PTransform} that checks if the entire line matches
   * the Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param keyGroupName The Regex group name to use as the key
   * @param valueGroupName The Regex group name to use the value
   */
  public static MatchesNameKV matchesKV(String regex, String keyGroupName, String valueGroupName) {
    return matchesKV(Pattern.compile(regex), keyGroupName, valueGroupName);
  }

  /**
   * Returns a {@link Regex.MatchesNameKV} {@link PTransform} that checks if the entire line matches
   * the Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param keyGroupName The Regex group name to use as the key
   * @param valueGroupName The Regex group name to use the value
   */
  public static MatchesNameKV matchesKV(
      Pattern pattern, String keyGroupName, String valueGroupName) {
    return new MatchesNameKV(pattern, keyGroupName, valueGroupName);
  }

  /**
   * Returns a {@link Regex.Find} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns the entire line (group 0) as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   */
  public static Find find(String regex) {
    return find(regex, 0);
  }

  /**
   * Returns a {@link Regex.Find} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns the entire line (group 0) as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   */
  public static Find find(Pattern pattern) {
    return find(pattern, 0);
  }

  /**
   * Returns a {@link Regex.Find} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns the group as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param group The Regex group to return as a PCollection
   */
  public static Find find(String regex, int group) {
    return find(Pattern.compile(regex), group);
  }

  /**
   * Returns a {@link Regex.Find} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns the group as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param group The Regex group to return as a PCollection
   */
  public static Find find(Pattern pattern, int group) {
    return new Find(pattern, group);
  }

  /**
   * Returns a {@link Regex.FindName} {@link PTransform} that checks if a portion of the line
   * matches the Regex. Returns the group as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param groupName The Regex group name to return as a PCollection
   */
  public static FindName find(String regex, String groupName) {
    return find(Pattern.compile(regex), groupName);
  }

  /**
   * Returns a {@link Regex.FindName} {@link PTransform} that checks if a portion of the line
   * matches the Regex. Returns the group as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param groupName The Regex group name to return as a PCollection
   */
  public static FindName find(Pattern pattern, String groupName) {
    return new FindName(pattern, groupName);
  }

  /**
   * Returns a {@link Regex.FindAll} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns all the groups as a List&lt;String&gt; in a {@link PCollection}.
   *
   * @param regex The regular expression to run
   */
  public static FindAll findAll(String regex) {
    return findAll(Pattern.compile(regex));
  }

  /**
   * Returns a {@link Regex.FindAll} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns all the groups as a List&lt;String&gt; in a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   */
  public static FindAll findAll(Pattern pattern) {
    return new FindAll(pattern);
  }

  /**
   * Returns a {@link Regex.FindKV} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param keyGroup The Regex group to use as the key
   * @param valueGroup The Regex group to use the value
   */
  public static FindKV findKV(String regex, int keyGroup, int valueGroup) {
    return findKV(Pattern.compile(regex), keyGroup, valueGroup);
  }

  /**
   * Returns a {@link Regex.FindKV} {@link PTransform} that checks if a portion of the line matches
   * the Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param keyGroup The Regex group to use as the key
   * @param valueGroup The Regex group to use the value
   */
  public static FindKV findKV(Pattern pattern, int keyGroup, int valueGroup) {
    return new FindKV(pattern, keyGroup, valueGroup);
  }

  /**
   * Returns a {@link Regex.FindNameKV} {@link PTransform} that checks if a portion of the line
   * matches the Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param keyGroupName The Regex group name to use as the key
   * @param valueGroupName The Regex group name to use the value
   */
  public static FindNameKV findKV(String regex, String keyGroupName, String valueGroupName) {
    return findKV(Pattern.compile(regex), keyGroupName, valueGroupName);
  }

  /**
   * Returns a {@link Regex.FindNameKV} {@link PTransform} that checks if a portion of the line
   * matches the Regex. Returns the specified groups as the key and value as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param keyGroupName The Regex group name to use as the key
   * @param valueGroupName The Regex group name to use the value
   */
  public static FindNameKV findKV(Pattern pattern, String keyGroupName, String valueGroupName) {
    return new FindNameKV(pattern, keyGroupName, valueGroupName);
  }

  /**
   * Returns a {@link Regex.ReplaceAll} {@link PTransform} that checks if a portion of the line
   * matches the Regex and replaces all matches with the replacement String. Returns the group as a
   * {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param replacement The string to be substituted for each match
   */
  public static ReplaceAll replaceAll(String regex, String replacement) {
    return replaceAll(Pattern.compile(regex), replacement);
  }

  /**
   * Returns a {@link Regex.ReplaceAll} {@link PTransform} that checks if a portion of the line
   * matches the Regex and replaces all matches with the replacement String. Returns the group as a
   * {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param replacement The string to be substituted for each match
   */
  public static ReplaceAll replaceAll(Pattern pattern, String replacement) {
    return new ReplaceAll(pattern, replacement);
  }

  /**
   * Returns a {@link Regex.ReplaceAll} {@link PTransform} that checks if a portion of the line
   * matches the Regex and replaces the first match with the replacement String. Returns the group
   * as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param replacement The string to be substituted for each match
   */
  public static ReplaceFirst replaceFirst(String regex, String replacement) {
    return replaceFirst(Pattern.compile(regex), replacement);
  }

  /**
   * Returns a {@link Regex.ReplaceAll} {@link PTransform} that checks if a portion of the line
   * matches the Regex and replaces the first match with the replacement String. Returns the group
   * as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param replacement The string to be substituted for each match
   */
  public static ReplaceFirst replaceFirst(Pattern pattern, String replacement) {
    return new ReplaceFirst(pattern, replacement);
  }

  /**
   * Returns a {@link Regex.Split} {@link PTransform} that splits a string on the regular expression
   * and then outputs each item. It will not output empty items. Returns the group as a {@link
   * PCollection}. a {@link PCollection}.
   *
   * @param regex The regular expression to run
   */
  public static Split split(String regex) {
    return split(Pattern.compile(regex), false);
  }

  /**
   * Returns a {@link Regex.Split} {@link PTransform} that splits a string on the regular expression
   * and then outputs each item. It will not output empty items. Returns the group as a {@link
   * PCollection}. a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   */
  public static Split split(Pattern pattern) {
    return split(pattern, false);
  }

  /**
   * Returns a {@link Regex.Split} {@link PTransform} that splits a string on the regular expression
   * and then outputs each item. Returns the group as a {@link PCollection}.
   *
   * @param regex The regular expression to run
   * @param outputEmpty Should empty be output. True to output empties and false if not.
   */
  public static Split split(String regex, boolean outputEmpty) {
    return split(Pattern.compile(regex), outputEmpty);
  }

  /**
   * Returns a {@link Regex.Split} {@link PTransform} that splits a string on the regular expression
   * and then outputs each item. Returns the group as a {@link PCollection}.
   *
   * @param pattern The regular expression to run
   * @param outputEmpty Should empty be output. True to output empties and false if not.
   */
  public static Split split(Pattern pattern, boolean outputEmpty) {
    return new Split(pattern, outputEmpty);
  }

  /**
   * {@code Regex.Matches<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} representing the value extracted from the Regex groups of the input {@code
   * PCollection} to the number of times that element occurs in the input.
   *
   * <p>This transform runs a Regex on the entire input line. If the entire line does not match the
   * Regex, the line will not be output. If it does match the entire line, the group in the Regex
   * will be used. The output will be the Regex group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.matches("myregex (mygroup)", 1));
   * }</pre>
   */
  public static class Matches extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    int group;

    public Matches(Pattern pattern, int group) {
      this.pattern = pattern;
      this.group = group;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.matches()) {
                    r.output(m.group(group));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.MatchesName<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} representing the value extracted from the Regex groups of the input {@code
   * PCollection} to the number of times that element occurs in the input.
   *
   * <p>This transform runs a Regex on the entire input line. If the entire line does not match the
   * Regex, the line will not be output. If it does match the entire line, the group in the Regex
   * will be used. The output will be the Regex group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.matches("myregex (?<namedgroup>mygroup)", "namedgroup"));
   * }</pre>
   */
  public static class MatchesName extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    String groupName;

    public MatchesName(Pattern pattern, String groupName) {
      this.pattern = pattern;
      this.groupName = groupName;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.matches()) {
                    r.output(m.group(groupName));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.MatchesName<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<List<String>>} representing the value extracted from all the Regex groups of the
   * input {@code PCollection} to the number of times that element occurs in the input.
   *
   * <p>This transform runs a Regex on the entire input line. If the entire line does not match the
   * Regex, the line will not be output. If it does match the entire line, the groups in the Regex
   * will be used. The output will be all of the Regex groups.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<List<String>> values =
   *     words.apply(Regex.allMatches("myregex (mygroup)"));
   * }</pre>
   */
  public static class AllMatches
      extends PTransform<PCollection<String>, PCollection<List<String>>> {
    final Pattern pattern;

    public AllMatches(Pattern pattern) {
      this.pattern = pattern;
    }

    @Override
    public PCollection<List<String>> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, List<String>>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<List<String>> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.matches()) {
                    ArrayList list = new ArrayList(m.groupCount());

                    // +1 because group 0 isn't included
                    for (int i = 0; i < m.groupCount() + 1; i++) {
                      list.add(m.group(i));
                    }

                    r.output(list);
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.MatchesKV<KV<String, String>>} takes a {@code PCollection<String>} and returns a
   * {@code PCollection<KV<String, String>>} representing the key and value extracted from the Regex
   * groups of the input {@code PCollection} to the number of times that element occurs in the
   * input.
   *
   * <p>This transform runs a Regex on the entire input line. If the entire line does not match the
   * Regex, the line will not be output. If it does match the entire line, the groups in the Regex
   * will be used. The key will be the key's group and the value will be the value's group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> keysAndValues =
   *     words.apply(Regex.matchesKV("myregex (mykeygroup) (myvaluegroup)", 1, 2));
   * }</pre>
   */
  public static class MatchesKV
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    final Pattern pattern;
    int keyGroup, valueGroup;

    public MatchesKV(Pattern pattern, int keyGroup, int valueGroup) {
      this.pattern = pattern;
      this.keyGroup = keyGroup;
      this.valueGroup = valueGroup;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(
                    @Element String element, OutputReceiver<KV<String, String>> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    r.output(KV.of(m.group(keyGroup), m.group(valueGroup)));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.MatchesNameKV<KV<String, String>>} takes a {@code PCollection<String>} and returns
   * a {@code PCollection<KV<String, String>>} representing the key and value extracted from the
   * Regex groups of the input {@code PCollection} to the number of times that element occurs in the
   * input.
   *
   * <p>This transform runs a Regex on the entire input line. If the entire line does not match the
   * Regex, the line will not be output. If it does match the entire line, the groups in the Regex
   * will be used. The key will be the key's group and the value will be the value's group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> keysAndValues =
   *     words.apply(Regex.matchesKV("myregex (?<keyname>mykeygroup) (?<valuename>myvaluegroup)",
   *       "keyname", "valuename"));
   * }</pre>
   */
  public static class MatchesNameKV
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    final Pattern pattern;
    String keyGroupName, valueGroupName;

    public MatchesNameKV(Pattern pattern, String keyGroupName, String valueGroupName) {
      this.pattern = pattern;
      this.keyGroupName = keyGroupName;
      this.valueGroupName = valueGroupName;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(
                    @Element String element, OutputReceiver<KV<String, String>> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    r.output(KV.of(m.group(keyGroupName), m.group(valueGroupName)));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.Find<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} representing the value extracted from the Regex groups of the input {@code
   * PCollection} to the number of times that element occurs in the input.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will not be output. If it does match a portion of the line, the group
   * in the Regex will be used. The output will be the Regex group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.find("myregex (mygroup)", 1));
   * }</pre>
   */
  public static class Find extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    int group;

    public Find(Pattern pattern, int group) {
      this.pattern = pattern;
      this.group = group;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    r.output(m.group(group));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.Find<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} representing the value extracted from the Regex groups of the input {@code
   * PCollection} to the number of times that element occurs in the input.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will not be output. If it does match a portion of the line, the group
   * in the Regex will be used. The output will be the Regex group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.find("myregex (?<namedgroup>mygroup)", "namedgroup"));
   * }</pre>
   */
  public static class FindName extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    String groupName;

    public FindName(Pattern pattern, String groupName) {
      this.pattern = pattern;
      this.groupName = groupName;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    r.output(m.group(groupName));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.Find<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<List<String>>} representing the value extracted from the Regex groups of the input
   * {@code PCollection} to the number of times that element occurs in the input.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will not be output. If it does match a portion of the line, the
   * groups in the Regex will be used. The output will be the Regex groups.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<List<String>> values =
   *     words.apply(Regex.findAll("myregex (mygroup)"));
   * }</pre>
   */
  public static class FindAll extends PTransform<PCollection<String>, PCollection<List<String>>> {
    final Pattern pattern;

    public FindAll(Pattern pattern) {
      this.pattern = pattern;
    }

    @Override
    public PCollection<List<String>> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, List<String>>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<List<String>> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    ArrayList list = new ArrayList(m.groupCount());

                    // +1 because group 0 isn't included
                    for (int i = 0; i < m.groupCount() + 1; i++) {
                      list.add(m.group(i));
                    }

                    r.output(list);
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.MatchesKV<KV<String, String>>} takes a {@code PCollection<String>} and returns a
   * {@code PCollection<KV<String, String>>} representing the key and value extracted from the Regex
   * groups of the input {@code PCollection} to the number of times that element occurs in the
   * input.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will not be output. If it does match a portion of the line, the
   * groups in the Regex will be used. The key will be the key's group and the value will be the
   * value's group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> keysAndValues =
   *     words.apply(Regex.findKV("myregex (mykeygroup) (myvaluegroup)", 1, 2));
   * }</pre>
   */
  public static class FindKV
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    final Pattern pattern;
    int keyGroup, valueGroup;

    public FindKV(Pattern pattern, int keyGroup, int valueGroup) {
      this.pattern = pattern;
      this.keyGroup = keyGroup;
      this.valueGroup = valueGroup;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(
                    @Element String element, OutputReceiver<KV<String, String>> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    r.output(KV.of(m.group(keyGroup), m.group(valueGroup)));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.MatchesKV<KV<String, String>>} takes a {@code PCollection<String>} and returns a
   * {@code PCollection<KV<String, String>>} representing the key and value extracted from the Regex
   * groups of the input {@code PCollection} to the number of times that element occurs in the
   * input.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will not be output. If it does match a portion of the line, the
   * groups in the Regex will be used. The key will be the key's group and the value will be the
   * value's group.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> keysAndValues =
   *     words.apply(Regex.findKV("myregex (?<keyname>mykeygroup) (?<valuename>myvaluegroup)",
   *       "keyname", "valuename"));
   * }</pre>
   */
  public static class FindNameKV
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    final Pattern pattern;
    String keyGroupName, valueGroupName;

    public FindNameKV(Pattern pattern, String keyGroupName, String valueGroupName) {
      this.pattern = pattern;
      this.keyGroupName = keyGroupName;
      this.valueGroupName = valueGroupName;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(
                    @Element String element, OutputReceiver<KV<String, String>> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);

                  if (m.find()) {
                    r.output(KV.of(m.group(keyGroupName), m.group(valueGroupName)));
                  }
                }
              }));
    }
  }

  /**
   * {@code Regex.ReplaceAll<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} with all Strings that matched the Regex being replaced with the
   * replacement string.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will be output without changes. If it does match a portion of the
   * line, all portions matching the Regex will be replaced with the replacement String.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.replaceAll("myregex", "myreplacement"));
   * }</pre>
   */
  public static class ReplaceAll extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    String replacement;

    public ReplaceAll(Pattern pattern, String replacement) {
      this.pattern = pattern;
      this.replacement = replacement;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);
                  r.output(m.replaceAll(replacement));
                }
              }));
    }
  }

  /**
   * {@code Regex.ReplaceFirst<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} with the first Strings that matched the Regex being replaced with the
   * replacement string.
   *
   * <p>This transform runs a Regex on the entire input line. If a portion of the line does not
   * match the Regex, the line will be output without changes. If it does match a portion of the
   * line, the first portion matching the Regex will be replaced with the replacement String.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.replaceFirst("myregex", "myreplacement"));
   * }</pre>
   */
  public static class ReplaceFirst extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    String replacement;

    public ReplaceFirst(Pattern pattern, String replacement) {
      this.pattern = pattern;
      this.replacement = replacement;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  Matcher m = pattern.matcher(element);
                  r.output(m.replaceFirst(replacement));
                }
              }));
    }
  }

  /**
   * {@code Regex.Split<String>} takes a {@code PCollection<String>} and returns a {@code
   * PCollection<String>} with the input string split into individual items in a list. Each item is
   * then output as a separate string.
   *
   * <p>This transform runs a Regex as part of a splint the entire input line. The split gives back
   * an array of items. Each item is output as a separate item in the {@code PCollection<String>}.
   *
   * <p>Depending on the Regex, a split can be an empty or "" string. You can pass in a parameter if
   * you want empty strings or not.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(Regex.split("\W*"));
   * }</pre>
   */
  public static class Split extends PTransform<PCollection<String>, PCollection<String>> {
    final Pattern pattern;
    boolean outputEmpty;
    int splitLimit;

    public Split(Pattern pattern, boolean outputEmpty) {
      this.pattern = pattern;
      this.outputEmpty = outputEmpty;
      // Use split with limit=0 iff this.outputEmpty is false, since it implicitly drops trailing
      // empty strings
      this.splitLimit = this.outputEmpty ? -1 : 0;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> r)
                    throws Exception {
                  String[] items = pattern.split(element, splitLimit);

                  for (String item : items) {
                    if (outputEmpty || !item.isEmpty()) {
                      r.output(item);
                    }
                  }
                }
              }));
    }
  }
}
