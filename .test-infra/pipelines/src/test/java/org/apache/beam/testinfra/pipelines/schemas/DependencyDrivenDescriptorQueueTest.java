package org.apache.beam.testinfra.pipelines.schemas;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.dataflow.v1beta3.Job;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

class DependencyDrivenDescriptorQueueTest {
  @Test
  void iterator_Job_isInDependencyOrder() {
    DependencyDrivenDescriptorQueue queue = new DependencyDrivenDescriptorQueue();
    queue.enqueue(Job.getDescriptor());
    Iterator<Descriptor> itr = queue.iterator();
    Descriptor previous = itr.next();
    int previousMessageCount = messageCount(previous);
    assertTrue(itr.hasNext());
    while (itr.hasNext()) {
      Descriptor current = itr.next();
      int currentMessageCount = messageCount(current);
      assertTrue(previousMessageCount <= currentMessageCount);
      boolean previousDependsOnCurrent = previous.getFields().stream().anyMatch(
          field -> field.getFullName().equals(current.getFullName())
      );
      assertFalse(previousDependsOnCurrent);
    }
  }

  private static int messageCount(Descriptor descriptor) {
    return (int) descriptor.getFields().stream().filter(field -> field.getJavaType().equals(
        JavaType.MESSAGE)).count();
  }
}