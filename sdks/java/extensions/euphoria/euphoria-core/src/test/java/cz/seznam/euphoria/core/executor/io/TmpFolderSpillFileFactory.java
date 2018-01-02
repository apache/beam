/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.rules.TemporaryFolder;

class TmpFolderSpillFileFactory implements SpillFileFactory {

  private final TemporaryFolder folder;
  private final List<File> created = new ArrayList<>();

  TmpFolderSpillFileFactory(TemporaryFolder folder) {
    this.folder = folder;
  }

  @Override
  public File newSpillFile() {
    try {
      File f = folder.newFile();
      created.add(f);
      return f;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<File> getCreatedFiles() {
    return created;
  }

}
