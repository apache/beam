package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

class ThrowableMessageMatcher extends BaseMatcher<Throwable> {
  private final Matcher<String> messageMatcher;

  public ThrowableMessageMatcher(String message) {
    this.messageMatcher = equalTo(message);
  }

  @Override
  public boolean matches(Object item) {
    if (!(item instanceof Throwable)) {
      return false;
    }
    Throwable that = (Throwable) item;
    return messageMatcher.matches(that.getMessage());
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("a throwable with a message ").appendDescriptionOf(messageMatcher);
  }
}
