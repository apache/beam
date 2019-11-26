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
package org.apache.beam.runners.dataflow.worker.profiler;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A wrapper around {@link Profiler} to support more idiomatic usage within Java. */
public class ScopedProfiler {

  /** A thin wrapper around {@link Profiler} to allow mocking in tests. */
  @VisibleForTesting
  static class ProfilerWrapper {
    /**
     * Registers a nonzero index to be used in a subsequent call to {@link #setAttribute}. The value
     * is permanently registered for the lifetime of the JVM. Registering the same string multiple
     * times will return the same value.
     *
     * @return a nonzero index representing the string, to be used on calls to setAttribute.
     * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
     */
    public int registerAttribute(String value) {
      return Profiler.registerAttribute(value);
    }

    /**
     * Sets attribute for the current thread. Can be called before profiling is enabled or while
     * profiling is active. Attributes must be either 0 (to clear attributes), or a value returned
     * by {@link #registerAttribute}.
     *
     * <p>Samples collected by this thread will be annotated with the string associated to this
     * value.
     *
     * @return the previous value of the thread attribute.
     * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
     */
    public int setAttribute(int value) {
      return Profiler.setAttribute(value);
    }

    /**
     * Gets attribute for the current thread. Can be called before profiling is enabled or while
     * profiling is active. Will return the value passed to the most recent call to {@link
     * #setAttribute} from this thread.
     *
     * @return the previous value of the thread attribute.
     * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
     */
    public int getAttribute() {
      return Profiler.getAttribute();
    }
  }

  /** Interface for a specific scope in the profile. */
  public interface ProfileScope {

    /** Sets the current thread to the given scope. */
    void activate();
  }

  private static final Logger LOG = LoggerFactory.getLogger(ScopedProfiler.class);

  private enum ProfilingState {
    PROFILING_PRESENT {
      @Override
      public ProfileScope createAttribute(ProfilerWrapper profiler, String attributeTag) {
        return new TaggedProfileScope(profiler, attributeTag);
      }

      @Override
      public ProfileScope currentScope(ProfilerWrapper profiler) {
        return new TaggedProfileScope(profiler, profiler.getAttribute());
      }

      @Override
      public ProfileScope emptyScope(ProfilerWrapper profiler) {
        return new TaggedProfileScope(profiler, 0);
      }
    },
    PROFILING_ABSENT {
      @Override
      public ProfileScope createAttribute(ProfilerWrapper profiler, String attributeTag) {
        return NoopProfileScope.NOOP;
      }

      @Override
      public ProfileScope currentScope(ProfilerWrapper profiler) {
        return NoopProfileScope.NOOP;
      }

      @Override
      public ProfileScope emptyScope(ProfilerWrapper profiler) {
        return NoopProfileScope.NOOP;
      }
    };

    public abstract ProfileScope createAttribute(ProfilerWrapper profiler, String attributeTag);

    public abstract ProfileScope currentScope(ProfilerWrapper profiler);

    public abstract ProfileScope emptyScope(ProfilerWrapper profiler);
  }

  private final ProfilerWrapper profiler;
  private final ProfilingState profilingState;

  public static final ScopedProfiler INSTANCE = new ScopedProfiler(new ProfilerWrapper());

  @VisibleForTesting
  ScopedProfiler(ProfilerWrapper wrapper) {
    profiler = wrapper;
    profilingState = checkProfilingState();
  }

  private ProfilingState checkProfilingState() {
    try {
      // Call a method from the profiler to see if it is available
      profiler.getAttribute();

      // If we make it here, then we successfully invoked the above method, which means the profiler
      // is available.
      LOG.info("Profiling Agent found. Per-step profiling is enabled.");
      return ProfilingState.PROFILING_PRESENT;
    } catch (UnsatisfiedLinkError e) {
      // If we make it here, then the profiling agent wasn't linked in.
      LOG.info("Profiling Agent not found. Profiles will not be available from this worker.");
      return ProfilingState.PROFILING_ABSENT;
    }
  }

  /**
   * Return an {@link ProfileScope} object to use for managing blocks associated with the given tag.
   *
   * <p>Registering a new scope should not happen within hot-loops since it is potentially
   * expensive. Instead, register a scope with a name outside the loop, and then enter the scope as
   * many times as necessary.
   */
  public ProfileScope registerScope(String scopeName) {
    return profilingState.createAttribute(profiler, scopeName);
  }

  public ProfileScope emptyScope() {
    return profilingState.emptyScope(profiler);
  }

  public ProfileScope currentScope() {
    return profilingState.currentScope(profiler);
  }

  /** The implementation of {@link ProfileScope} used when the profiling agent is absent. */
  public enum NoopProfileScope implements ProfileScope {
    NOOP {
      @Override
      public void activate() {
        // Do nothing
      }
    }
  }

  /** The implementation of {@link ProfileScope} used when the profiling agent is present. */
  private static class TaggedProfileScope implements ProfileScope {
    private final int attribute;
    private final ProfilerWrapper profiler;

    private TaggedProfileScope(ProfilerWrapper profiler, String attributeTag) {
      this.profiler = profiler;
      this.attribute = profiler.registerAttribute(attributeTag);
    }

    private TaggedProfileScope(ProfilerWrapper profiler, int attribute) {
      this.profiler = profiler;
      this.attribute = attribute;
    }

    @Override
    public void activate() {
      profiler.setAttribute(attribute);
    }
  }
}
