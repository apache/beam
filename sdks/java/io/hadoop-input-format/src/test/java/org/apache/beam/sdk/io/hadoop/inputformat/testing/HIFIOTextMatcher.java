package org.apache.beam.sdk.io.hadoop.inputformat.testing;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * A matcher class to verify if the checksum of expected list of Strings matches with the checksum
 * of a file content.
 */
public class HIFIOTextMatcher extends TypeSafeMatcher<PipelineResult> implements
    SerializableMatcher<PipelineResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOTextMatcher.class);
  private String expectedChecksum;
  private String actualChecksum;
  private String filePath;
  private List<String> expectedList;

  public HIFIOTextMatcher(String filePath, List<String> expectedList) {
    this.filePath = filePath;
    this.expectedList = expectedList;
  }

  @Override
  public void describeTo(Description arg0) {
    // TODO Auto-generated method stub

  }

  /**
   * This method checks if the checksum of expected list of Strings matches with the checksum of
   * file content.
   */
  @Override
  protected boolean matchesSafely(PipelineResult arg0) {
    expectedChecksum = generateHash(expectedList);
    ArrayList<String> actualList = new ArrayList<String>();
    Scanner s;
    try {
      s = new Scanner(new File(filePath));
      while (s.hasNext()) {
        actualList.add(s.next());
      }
      s.close();
    } catch (FileNotFoundException e) {
      LOGGER.warn(e.getMessage());
    }
    actualChecksum = generateHash(actualList);
    return expectedChecksum.equals(actualChecksum);
  }

  /**
   * This method generates checksum for a list of Strings.
   * @param elements
   * @return
   */
  private String generateHash(@Nonnull List<String> elements) {
    List<HashCode> rowHashes = Lists.newArrayList();
    for (String element : elements) {
      rowHashes.add(Hashing.sha1().hashString(element, StandardCharsets.UTF_8));
    }
    return Hashing.combineUnordered(rowHashes).toString();
  }

}
