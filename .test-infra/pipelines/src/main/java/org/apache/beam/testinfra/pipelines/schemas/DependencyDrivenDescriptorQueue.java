package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;

class DependencyDrivenDescriptorQueue implements Iterable<Descriptor>, Comparator<Descriptor> {
  private final Map<@NonNull String, @NonNull Descriptor> DESCRIPTOR_MAP = new HashMap<>();
  private final Map<@NonNull String, @NonNull Set<String>> DEPENDENCY_MAP = new HashMap<>();

  private final Map<@NonNull String, @NonNull Integer> MESSAGE_FIELD_COUNTS = new HashMap<>();

  void enqueue(@NonNull Descriptor descriptor) {
    List<@NonNull Descriptor> descriptorStack = new ArrayList<>();
    descriptorStack.add(descriptor);
    while (!descriptorStack.isEmpty()) {
      Descriptor fromStack = descriptorStack.remove(0);
      if (DESCRIPTOR_MAP.containsKey(fromStack.getFullName())) {
        checkState(DEPENDENCY_MAP.containsKey(fromStack.getFullName()));
        checkState(MESSAGE_FIELD_COUNTS.containsKey(fromStack.getFullName()));
        continue;
      }
      int messageFieldCounts = 0;
      for (FieldDescriptor field : fromStack.getFields()) {
        if (!field.getJavaType().equals(JavaType.MESSAGE)) {
          continue;
        }
        messageFieldCounts++;
        Descriptor fieldDescriptor = field.getMessageType();
        if (!DEPENDENCY_MAP.containsKey(fieldDescriptor.getFullName())) {
          DEPENDENCY_MAP.put(fieldDescriptor.getFullName(), new HashSet<>());
        }
        Set<String> dependents = DEPENDENCY_MAP.get(fieldDescriptor.getFullName());
        dependents.add(descriptor.getFullName());
        descriptorStack.add(fieldDescriptor);
      }
      DESCRIPTOR_MAP.put(fromStack.getFullName(), fromStack);
      MESSAGE_FIELD_COUNTS.put(fromStack.getFullName(), messageFieldCounts);
      if (!DEPENDENCY_MAP.containsKey(fromStack.getFullName())) {
        DEPENDENCY_MAP.put(fromStack.getFullName(), new HashSet<>());
      }
    }
  }

  @Override
  public Iterator<Descriptor> iterator() {
    return DESCRIPTOR_MAP.values().stream().sorted(this).iterator();
  }

  @Override
  public int compare(@NonNull Descriptor a, @NonNull Descriptor b) {
    boolean aDependsOnB = checkStateNotNull(DEPENDENCY_MAP.get(b.getFullName())).contains(a.getFullName());
    boolean bDependsOnA = checkStateNotNull(DEPENDENCY_MAP.get(a.getFullName())).contains(b.getFullName());
    if (aDependsOnB) {
      return 1;
    }
    if (bDependsOnA) {
      return -1;
    }
    Integer aMessageFieldCount = checkStateNotNull(MESSAGE_FIELD_COUNTS.get(a.getFullName()));
    Integer bMessageFieldCount = checkStateNotNull(MESSAGE_FIELD_COUNTS.get(b.getFullName()));
    return aMessageFieldCount.compareTo(bMessageFieldCount);
  }
}
