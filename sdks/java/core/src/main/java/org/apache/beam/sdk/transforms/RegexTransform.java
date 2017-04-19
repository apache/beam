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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransorm}s to use Regular Expressions to process elements in a
 * {@link PCollection}.
 *
 * <p>
 * {@link RegexTransform#matches(String, int)} can be used to see if an entire line matches
 * a Regex. {@link RegexTransform#matchesKV(String, int, int)} can be used to see if an entire
 * line matches a Regex and output certain groups as a {@link KV}.
 * </p>
 * <p>
 * {@link RegexTransform#find(String, int)} can be used to see if a portion of a line
 * matches a Regex. {@link RegexTransform#matchesKV(String, int, int)} can be used to see if a
 * portion of a line matches a Regex and output certain groups as a {@link KV}.
 * </p>
 * <p>
 * Lines that do not match the Regex will not be output.
 * </p>
 */
public class RegexTransform {
  private RegexTransform() {
    // do not instantiate
  }

  /**
   * Returns a {@link RegexTransform.Matches} {@link PTransform} that checks if
   * the entire line matches the Regex. Returns the entire line (group 0) as a
   * {@link PCollection}.
   * @param regex
   *          The regular expression to run
   */
  public static Matches matches(String regex) {
    return matches(regex, 0);
  }

  /**
   * Returns a {@link RegexTransform.Matches} {@link PTransform} that checks if
   * the entire line matches the Regex. Returns the group as a
   * {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param group
   *          The Regex group to return as a PCollection
   */
  public static Matches matches(String regex, int group) {
    return new Matches(regex, group);
  }

  /**
   * Returns a {@link RegexTransform.MatchesKV} {@link PTransform} that checks
   * if the entire line matches the Regex. Returns the specified groups as the
   * key and value as a {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param keyGroup
   *          The Regex group to use as the key
   * @param valueGroup
   *          The Regex group to use the value
   */
  public static MatchesKV matchesKV(String regex, int keyGroup,
      int valueGroup) {
    return new MatchesKV(regex, keyGroup, valueGroup);
  }

  /**
   * Returns a {@link RegexTransform.Find} {@link PTransform} that checks if a
   * portion of the line matches the Regex. Returns the entire line (group 0) as
   * a {@link PCollection}.
   * @param regex
   *          The regular expression to run
   */
  public static Find find(String regex) {
    return find(regex, 0);
  }

  /**
   * Returns a {@link RegexTransform.Find} {@link PTransform} that checks if a
   * portion of the line matches the Regex. Returns the group as a
   * {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param group
   *          The Regex group to return as a PCollection
   */
  public static Find find(String regex, int group) {
    return new Find(regex, group);
  }

  /**
   * Returns a {@link RegexTransform.FindKV} {@link PTransform} that checks if a
   * portion of the line matches the Regex. Returns the specified groups as the
   * key and value as a {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param keyGroup
   *          The Regex group to use as the key
   * @param valueGroup
   *          The Regex group to use the value
   */
  public static FindKV findKV(String regex, int keyGroup, int valueGroup) {
    return new FindKV(regex, keyGroup, valueGroup);
  }

  /**
   * Returns a {@link RegexTransform.ReplaceAll} {@link PTransform} that checks if a
   * portion of the line matches the Regex and replaces all matches with the replacement
   * String. Returns the group as a {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param replacement
   *          The string to be substituted for each match
   */
  public static ReplaceAll replaceAll(String regex, String replacement) {
    return new ReplaceAll(regex, replacement);
  }

  /**
   * Returns a {@link RegexTransform.ReplaceAll} {@link PTransform} that checks if a
   * portion of the line matches the Regex and replaces the first match with the replacement
   * String. Returns the group as a {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param replacement
   *          The string to be substituted for each match
   */
  public static ReplaceFirst replaceFirst(String regex, String replacement) {
    return new ReplaceFirst(regex, replacement);
  }

    /**
   * Returns a {@link RegexTransform.Split} {@link PTransform} that splits a string
   * on the regular expression and then outputs each item. It will not output empty
   * items. Returns the group as a {@link PCollection}.
   * a {@link PCollection}.
   * @param regex
   *          The regular expression to run
   */
  public static Split split(String regex) {
    return split(regex, false);
  }

  /**
   * Returns a {@link RegexTransform.Split} {@link PTransform} that splits a string
   * on the regular expression and then outputs each item. Returns the group as a
   * {@link PCollection}.
   * @param regex
   *          The regular expression to run
   * @param outputEmpty
   *          Should empty be output. True to output empties and false if not.
   */
  public static Split split(String regex, boolean outputEmpty) {
    return new Split(regex, outputEmpty);
  }

  /**
   * {@code RegexTransform.Matches<String>} takes a {@code PCollection<String>}
   * and returns a {@code PCollection<String>} representing the value
   * extracted from the Regex groups of the input {@code PCollection}
   * to the number of times that element occurs in the input.
   *
   * <p>
   * This transform runs a Regex on the entire input line. If the entire line
   * does not match the Regex, the line will not be output. If it does match the
   * entire line, the group in the Regex will be used. The output will be the
   * Regex group.
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(RegexTransform.matches("myregex (mygroup)", 1));
   * }
   * </pre>
   */
  public static class Matches
      extends PTransform<PCollection<String>, PCollection<String>> {
    Pattern pattern;
    int group;

    public Matches(String regex, int group) {
      this.pattern = Pattern.compile(regex);
      this.group = group;
    }

    public PCollection<String> apply(PCollection<String> in) {
      return in
          .apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              Matcher m = pattern.matcher((String) c.element());

              if (m.matches()) {
                c.output(m.group(group));
              }
            }
          }));
    }
  }

  /**
   * {@code RegexTransform.MatchesKV<KV<String, String>>} takes a
   * {@code PCollection<String>} and returns a
   * {@code PCollection<KV<String, String>>} representing the key and value
   * extracted from the Regex groups of the input {@code PCollection} to the
   * number of times that element occurs in the input.
   *
   * <p>
   * This transform runs a Regex on the entire input line. If the entire line
   * does not match the Regex, the line will not be output. If it does match the
   * entire line, the groups in the Regex will be used. The key will be the
   * key's group and the value will be the value's group.
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> keysAndValues =
   *     words.apply(RegexTransform.matchesKV("myregex (mykeygroup) (myvaluegroup)", 1, 2));
   * }
   * </pre>
   */
  public static class MatchesKV
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    Pattern pattern;
    int keyGroup, valueGroup;

    public MatchesKV(String regex, int keyGroup, int valueGroup) {
      this.pattern = Pattern.compile(regex);
      this.keyGroup = keyGroup;
      this.valueGroup = valueGroup;
    }

    public PCollection<KV<String, String>> apply(PCollection<String> in) {
      return in.apply(ParDo
          .of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              Matcher m = pattern.matcher((String) c.element());

              if (m.find()) {
                c.output(KV.of(m.group(keyGroup), m.group(valueGroup)));
              }
            }
          }));
    }
  }

  /**
   * {@code RegexTransform.Find<String>} takes a {@code PCollection<String>} and
   * returns a {@code PCollection<String>} representing the value extracted
   * from the Regex groups of the input {@code PCollection} to
   * the number of times that element occurs in the input.
   *
   * <p>
   * This transform runs a Regex on the entire input line. If a portion of the
   * line does not match the Regex, the line will not be output. If it does
   * match a portion of the line, the group in the Regex will be used. The
   * output will be the Regex group.
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(RegexTransform.find("myregex (mygroup)", 1));
   * }
   * </pre>
   */
  public static class Find
      extends PTransform<PCollection<String>, PCollection<String>> {
    Pattern pattern;
    int group;

    public Find(String regex, int group) {
      this.pattern = Pattern.compile(regex);
      this.group = group;
    }

    public PCollection<String> apply(PCollection<String> in) {
      return in.apply(ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          Matcher m = pattern.matcher((String) c.element());

          if (m.find()) {
            c.output(m.group(group));
          }
        }
      }));
    }
  }

  /**
   * {@code RegexTransform.MatchesKV<KV<String, String>>} takes a
   * {@code PCollection<String>} and returns a
   * {@code PCollection<KV<String, String>>} representing the key and value
   * extracted from the Regex groups of the input {@code PCollection} to the
   * number of times that element occurs in the input.
   *
   * <p>
   * This transform runs a Regex on the entire input line. If a portion of the
   * line does not match the Regex, the line will not be output. If it does
   * match a portion of the line, the groups in the Regex will be used. The key
   * will be the key's group and the value will be the value's group.
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> keysAndValues =
   *     words.apply(RegexTransform.findKV("myregex (mykeygroup) (myvaluegroup)", 1, 2));
   * }
   * </pre>
   */
  public static class FindKV
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    Pattern pattern;
    int keyGroup, valueGroup;

    public FindKV(String regex, int keyGroup, int valueGroup) {
      this.pattern = Pattern.compile(regex);
      this.keyGroup = keyGroup;
      this.valueGroup = valueGroup;
    }

    public PCollection<KV<String, String>> apply(PCollection<String> in) {
      return in.apply(
          ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              Matcher m = pattern.matcher((String) c.element());

              if (m.find()) {
                c.output(KV.of(m.group(keyGroup), m.group(valueGroup)));
              }
            }
          }));
    }
  }

  /**
   * {@code RegexTransform.ReplaceAll<String>} takes a {@code PCollection<String>} and
   * returns a {@code PCollection<String>} with all Strings that matched the
   * Regex being replaced with the replacement string.
   *
   * <p>
   * This transform runs a Regex on the entire input line. If a portion of the
   * line does not match the Regex, the line will be output without changes. If it does
   * match a portion of the line, all portions matching the Regex will be replaced
   * with the replacement String.
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(RegexTransform.replaceAll("myregex", "myreplacement"));
   * }
   * </pre>
   */
  public static class ReplaceAll
      extends PTransform<PCollection<String>, PCollection<String>> {
    Pattern pattern;
    String replacement;

    public ReplaceAll(String regex, String replacement) {
      this.pattern = Pattern.compile(regex);
      this.replacement = replacement;
    }

    public PCollection<String> apply(PCollection<String> in) {
      return in.apply(ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          Matcher m = pattern.matcher((String) c.element());
          c.output(m.replaceAll(replacement));
        }
      }));
    }
  }

  /**
   * {@code RegexTransform.ReplaceFirst<String>} takes a {@code PCollection<String>} and
   * returns a {@code PCollection<String>} with the first Strings that matched the
   * Regex being replaced with the replacement string.
   *
   * <p>
   * This transform runs a Regex on the entire input line. If a portion of the
   * line does not match the Regex, the line will be output without changes. If it does
   * match a portion of the line, the first portion matching the Regex will be replaced
   * with the replacement String.
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(RegexTransform.replaceFirst("myregex", "myreplacement"));
   * }
   * </pre>
   */
  public static class ReplaceFirst
      extends PTransform<PCollection<String>, PCollection<String>> {
    Pattern pattern;
    String replacement;

    public ReplaceFirst(String regex, String replacement) {
      this.pattern = Pattern.compile(regex);
      this.replacement = replacement;
    }

    public PCollection<String> apply(PCollection<String> in) {
      return in.apply(ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          Matcher m = pattern.matcher((String) c.element());
          c.output(m.replaceFirst(replacement));
        }
      }));
    }
  }

  /**
   * {@code RegexTransform.Split<String>} takes a {@code PCollection<String>} and
   * returns a {@code PCollection<String>} with the input string split into
   * individual items in a list. Each item is then output as a separate string.
   *
   * <p>
   * This transform runs a Regex as part of a splint the entire input line. The split
   * gives back an array of items. Each item is output as a separate item in the
   * {@code PCollection<String>}.
   * </p>
   *
   * <p>
   * Depending on the Regex, a split can be an empty or
   * "" string. You can pass in a parameter if you want empty strings or not.
   * </p>
   *
   * <p>
   * Example of use:
   * <pre>
   *  {@code
   * PCollection<String> words = ...;
   * PCollection<String> values =
   *     words.apply(RegexTransform.split("\W*"));
   * }
   * </pre>
   */
  public static class Split
      extends PTransform<PCollection<String>, PCollection<String>> {
    Pattern pattern;
    boolean outputEmpty;

    public Split(String regex, boolean outputEmpty) {
      this.pattern = Pattern.compile(regex);
      this.outputEmpty = outputEmpty;
    }

    public PCollection<String> apply(PCollection<String> in) {
      return in.apply(ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          String[] items = pattern.split(c.element());

          for (String item : items) {
            if (outputEmpty || !item.isEmpty()) {
              c.output(item);
            }
          }
        }
      }));
    }
  }
}
