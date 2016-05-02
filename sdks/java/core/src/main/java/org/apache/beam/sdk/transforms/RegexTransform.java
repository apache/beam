package org.apache.beam.sdk.transforms;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransorm}s to use Regular Expressions to process elements in a
 * {@link PCollection}.
 *
 * <p>
 * {@link RegexTransform#matches()} can be used to see if an entire line matches
 * a Regex. {@link RegexTransform#matchesKV()} can be used to see if an entire
 * line matches a Regex and output certain groups as a {@link KV}.
 * </p>
 *
 * <p>
 * {@link RegexTransform#find()} can be used to see if a portion of a line
 * matches a Regex. {@link RegexTransform#matchesKV()} can be used to see if a
 * portion of a line matches a Regex and output certain groups as a {@link KV}.
 * </p>
 *
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
   * 
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
   * 
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
   * 
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
   * 
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
   * 
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
   * 
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
   * {@code RegexTransform.Matches<String>} takes a {@code PCollection<String>}
   * and returns a {@code PCollection<KV<String, String>>} representing the key
   * and value extracted from the Regex groups of the input {@code PCollection}
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
   * 
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
          .apply(ParDo.named("MatchesRegex").of(new DoFn<String, String>() {
            @Override
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
   * 
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
      return in.apply(ParDo.named("MatchesKVRegex")
          .of(new DoFn<String, KV<String, String>>() {
            @Override
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
   * returns a {@code PCollection<KV<String, String>>} representing the key and
   * value extracted from the Regex groups of the input {@code PCollection} to
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
   * 
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
      return in.apply(ParDo.named("FindRegex").of(new DoFn<String, String>() {
        @Override
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
   * 
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
          ParDo.named("FindKVRegex").of(new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
              Matcher m = pattern.matcher((String) c.element());

              if (m.find()) {
                c.output(KV.of(m.group(keyGroup), m.group(valueGroup)));
              }
            }
          }));
    }
  }
}
