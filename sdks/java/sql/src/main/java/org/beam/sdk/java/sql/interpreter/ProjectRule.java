package org.beam.sdk.java.sql.interpreter;

import java.io.Serializable;

public class ProjectRule implements Serializable {
  /**
  * 
  */
  private static final long serialVersionUID = 6166324769404546121L;
  private ProjectType type;
  private int sourceIndex;// for RexInputRef

  private String projectExp;

  public ProjectRule() {
  }

  public ProjectType getType() {
    return type;
  }

  public void setType(ProjectType type) {
    this.type = type;
  }

  public String getProjectExp() {
    return projectExp;
  }

  public void setProjectExp(String projectExp) {
    this.projectExp = projectExp;
  }

  public int getSourceIndex() {
    return sourceIndex;
  }

  public void setSourceIndex(int sourceIndex) {
    this.sourceIndex = sourceIndex;
  }

  @Override
  public String toString() {
    return "ProjectRule [type=" + type + ", sourceIndex=" + sourceIndex + ", projectExp="
        + projectExp + "]";
  }

}
