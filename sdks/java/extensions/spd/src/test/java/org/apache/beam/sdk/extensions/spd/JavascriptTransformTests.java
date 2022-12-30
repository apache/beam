package org.apache.beam.sdk.extensions.spd;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavascriptTransformTests {
  private static final Logger LOG = LoggerFactory.getLogger(JavascriptTransformTests.class);

  @Test
  public void testJavascriptEval() throws Exception {
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByExtension("js");
    engine.eval("function returnJSON() { return {a:1,b:\"2\"}; }");
    LOG.info("JSON result:"+(((Invocable)engine).invokeFunction("returnJSON")).getClass().getName());
    throw new Exception("DIE!");
  }

}
