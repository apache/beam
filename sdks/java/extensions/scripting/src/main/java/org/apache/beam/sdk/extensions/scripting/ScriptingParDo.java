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
package org.apache.beam.sdk.extensions.scripting;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A transform to write simple ParDo transformations with a scripting language
 * supported by Java JSR-223.
 */
@Experimental
public abstract class ScriptingParDo<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private ValueProvider<String> language = StaticValueProvider.of("js");
  private ValueProvider<String> script = StaticValueProvider.of("");
  private Coder<OutputT> coder;

  public static <InputT, OutputT> ScriptingParDo<InputT, OutputT> of(final Coder<OutputT> coder) {
    final ScriptingParDo<InputT, OutputT> scripting = new ScriptingParDo<InputT, OutputT>() {};
    scripting.coder = coder;
    return scripting;
  }

  public ScriptingParDo<InputT, OutputT> withLanguage(final String language) {
    checkArgument(language != null, "language can not be null");
    return withLanguage(ValueProvider.StaticValueProvider.of(language));
  }

  public ScriptingParDo<InputT, OutputT> withLanguage(final ValueProvider<String> language) {
    checkArgument(language != null, "language can not be null");
    this.language = language;
    return this;
  }

  public ScriptingParDo<InputT, OutputT> withScript(final String script) {
    checkArgument(script != null, "script can not be null");
    return withScript(ValueProvider.StaticValueProvider.of(script));
  }

  public ScriptingParDo<InputT, OutputT> withScript(final ValueProvider<String> script) {
    checkArgument(script != null, "script can not be null");
    this.script = script;
    return this;
  }

  @Override
  public PCollection<OutputT> expand(final PCollection<InputT> apCollection) {
    if (language == null || script == null || script.get().isEmpty()) {
      throw new IllegalArgumentException("Language and Script must be set");
    }
    return apCollection.apply(ParDo.of(new ScriptingDoFn<>(language.get(), script.get())));
  }

  @Override // ensure we don't always need to set the coder
  public <T> Coder<T> getDefaultOutputCoder(
      final PCollection<InputT> input, final PCollection<T> output)
      throws CannotProvideCoderException {
    if (coder != null) {
      return (Coder<T>) coder;
    }
    final Type superclass = getClass().getGenericSuperclass();
    if (ParameterizedType.class.isInstance(superclass)) {
      final Type type = ParameterizedType.class.cast(superclass).getActualTypeArguments()[1];
      return (Coder<T>) output.getPipeline().getCoderRegistry().getCoder(TypeDescriptor.of(type));
    }
    return (Coder<T>) SerializableCoder.of(Serializable.class);
  }

  private static class ScriptingDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    private String language;
    private String script;

    private volatile ScriptEngine engine;
    private CompiledScript compiledScript;

    ScriptingDoFn(final String language, final String script) {
      this.language = language;
      this.script = script;
    }

    @Setup
    public void setup() {
      final ScriptEngineManager manager =
        new ScriptEngineManager(Thread.currentThread().getContextClassLoader());
      engine = manager.getEngineByExtension(language);
      if (engine == null) {
        engine = manager.getEngineByName(language);
        if (engine == null) {
          engine = manager.getEngineByMimeType(language);
        }
      }
      if (engine == null) {
        throw new IllegalArgumentException("Language [" + language + "] "
                + "not supported. Check that the needed depencencies are configured.");
      }

      if (Compilable.class.isInstance(engine)) {
        try {
          compiledScript = Compilable.class.cast(engine).compile(script);
        } catch (ScriptException e) {
          throw new IllegalStateException(e);
        }
      } else {
        compiledScript = new CompiledScript() {
          @Override
          public Object eval(final ScriptContext context) throws ScriptException {
            return engine.eval(script, context);
          }

          @Override
          public ScriptEngine getEngine() {
            return engine;
          }
        };
      }
    }

    @ProcessElement
    public void processElement(final ProcessContext context) {
      final Bindings bindings = engine.createBindings();
      bindings.put("context", context);

      final SimpleScriptContext scriptContext = new SimpleScriptContext();
      scriptContext.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

      try {
        final Object eval = compiledScript.eval(scriptContext);
        if (eval != null) {
          // if the script returns a value we put it in the context otherwise we asume the script
          // already did the output via context.output(...)
          context.output((OutputT) eval);
        }
      } catch (final ScriptException e) {
        throw new IllegalStateException(e);
      }
    }

    @Teardown
    public void tearDown() {
      //TODO compiledScript != null
      if (AutoCloseable.class.isInstance(compiledScript)) {
        try {
          AutoCloseable.class.cast(compiledScript).close();
        } catch (final Exception e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }
}
