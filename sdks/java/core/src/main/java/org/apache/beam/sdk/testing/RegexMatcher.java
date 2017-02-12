package org.apache.beam.sdk.testing;

import java.util.regex.Pattern;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;


/** Hamcrest matcher to assert a string matches a pattern. */
public class RegexMatcher extends BaseMatcher<String> {
  private final Pattern pattern;

  public RegexMatcher(String regex) {
    this.pattern = Pattern.compile(regex);
  }

  @Override
  public boolean matches(Object o) {
    if (!(o instanceof String)) {
      return false;
    }
    return pattern.matcher((String) o).matches();
  }

  @Override
  public void describeTo(Description description) {
    description.appendText(String.format("matches regular expression %s", pattern));
  }

  public static RegexMatcher matches(String regex) {
    return new RegexMatcher(regex);
  }
}
