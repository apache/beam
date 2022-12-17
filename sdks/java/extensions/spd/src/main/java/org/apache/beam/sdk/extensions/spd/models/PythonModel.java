package org.apache.beam.sdk.extensions.spd.models;

public class PythonModel implements StructuredModel {

  private String path;
  private String name;
  private String rawPy;

  public PythonModel(String path,String name,String rawPy) {
    this.path = path;
    this.name = name;
    this.rawPy = rawPy;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getRawPy() {
    return rawPy;
  }
}
