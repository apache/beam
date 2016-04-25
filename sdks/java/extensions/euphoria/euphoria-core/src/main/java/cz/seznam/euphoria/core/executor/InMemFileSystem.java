package cz.seznam.euphoria.core.executor;

import com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinkFactory;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class InMemFileSystem {

  public static final class SourceFactory implements DataSourceFactory {
    @Override
    public <T> DataSource<T> get(URI uri, Settings settings) {
      InMemFileSystem fs = InMemFileSystem.get();
      String path = uri.getPath();
      List<String> cpath = fs.toCanonicalPath(path);

      DirOrFile dirOrFile = fs.getDirOrFile(cpath);
      if (dirOrFile instanceof Directory) {
        Directory dir = (Directory) dirOrFile;
        List<Map.Entry<String, File>> files = dir.listFiles();
        List<List<String>> fpaths = new ArrayList<>(files.size());
        boolean bounded = true;
        for (Map.Entry<String, File> f : files) {
          if (f.getValue().delay != null) {
            bounded = false;
          }
          List<String> fpath = new ArrayList<>(fpaths.size() + 1);
          fpath.addAll(cpath);
          fpath.add(f.getKey());
          fpaths.add(fpath);
        }
        // ~ sort such that partitions are opened in lexicographical order (for the
        // purposes of determinism [in unit tests])
        fpaths.sort((a, b) -> a.get(a.size() - 1).compareTo(b.get(b.size() - 1)));
        return new FilesDataSource<>(fpaths, bounded);
      } else {
        File f = (File) dirOrFile;
        return new FilesDataSource<>(
            Collections.singletonList(cpath), f.delay == null);
      }
    }
  }

  private static final class FilePartition<T>
      implements Partition<T>
  {
    private static final class FileReader<T>
        extends AbstractIterator<T>
        implements Reader<T>
    {
      Iterator<T> content;
      Duration delay = null;
      boolean closed = false;

      public FileReader(Iterator<T> content, Duration delay) {
        this.content = requireNonNull(content);
        this.delay = delay;
      }

      @Override
      public void close() throws IOException {
        closed = true;
      }

      @Override
      protected T computeNext() {
        if (delay != null) {
          try {
            Thread.sleep(delay.toMillis());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted!", e);
          }
        }
        if (closed) {
          return endOfData();
        }
        if (content.hasNext()) {
          return content.next();
        }
        return endOfData();
      }
    }

    private final List<String> filePath;

    FilePartition(List<String> filePath) {
      this.filePath = requireNonNull(filePath);
    }

    @Override
    public Set<String> getLocations() {
      return Collections.singleton("localhost");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Reader<T> openReader() throws IOException {
      File f = (File) InMemFileSystem.get().getDirOrFile(filePath);
      return new FileReader<>(f.iterate(), f.delay);
    }
  }

  private static final class FilesDataSource<T> implements DataSource<T> {
    private final List<List<String>> paths;
    private final boolean bounded;

    FilesDataSource(List<List<String>> paths, boolean bounded) {
      this.paths = requireNonNull(paths);
      this.bounded = bounded;
    }

    @Override
    public List<Partition<T>> getPartitions() {
      return paths.stream()
          .map(FilePartition<T>::new)
          .collect(Collectors.toList());
    }

    @Override
    public boolean isBounded() {
      return bounded;
    }
  }

  // ~ -----------------------------------------------------------------------------

  public static final class SinkFactory implements DataSinkFactory {
    @Override
    public <T> DataSink<T> get(URI uri, Settings settings) {
      List<String> cpath = InMemFileSystem.get().toCanonicalPath(uri.getPath());
      return (DataSink<T>) new DirectoryDataSink<>(cpath);
    }
  }

  private static final class DirectoryDataSink<T> implements DataSink<T> {
    private final List<String> path;
    private final String stagingPath;

    DirectoryDataSink(List<String> path) {
      this.path = requireNonNull(path);
      this.stagingPath = UUID.randomUUID().toString();
    }

    @Override
    public Writer<T> openWriter(int partitionId) {
      return new FileWriter<>(this.stagingPath + "/" + partitionId);
    }

    @Override
    public void commit() throws IOException {
      InMemFileSystem fs = InMemFileSystem.get();
      Directory staging =
          fs.mkDirectory(Collections.singletonList(this.stagingPath));
      try {
        Directory targetDir = fs.mkParent(path);
        for (Map.Entry<String, File> e : staging.listFiles()) {
          targetDir.set(e.getKey(), e.getValue());
        }
      } finally {
        fs.remove(this.stagingPath);
      }
    }

    @Override
    public void rollback() {
      InMemFileSystem.get().remove(stagingPath);
    }
  }

  private static final class FileWriter<T> implements Writer<T> {
    private String path;
    private List<T> elems = new LinkedList<>();

    FileWriter(String path) {
      this.path = requireNonNull(path);
    }

    @Override
    public void write(T elem) throws IOException {
      elems.add(elem);
    }

    @Override
    public void commit() throws IOException {
      InMemFileSystem.get().setFile(path, new ArrayList<>(elems));
    }

    @Override
    public void rollback() throws IOException {
      // ~ no-op
    }

    @Override
    public void close() throws IOException {
      // ~ no-op
    }
  }

  // ~ -----------------------------------------------------------------------------

  private interface DirOrFile {}

  private static final class File implements DirOrFile {
    private final Collection storage;
    private final Duration delay;

    File(Collection storage, Duration delay /* optional */) {
      this.storage = requireNonNull(storage);
      this.delay = delay;
    }

    Iterator iterate() {
      return storage.iterator();
    }
  }

  private static final class Directory implements DirOrFile {
    private final Map<String, DirOrFile> files = new HashMap<>();

    void set(String name, DirOrFile dirOrFile) {
      files.put(requireNonNull(name), requireNonNull(dirOrFile));
    }

    DirOrFile get(String name) {
      return files.get(requireNonNull(name));
    }

    List<Map.Entry<String, File>> listFiles() {
      List<Map.Entry<String, File>> fs = new ArrayList<>();
      for (Map.Entry<String, DirOrFile> f : files.entrySet()) {
        if (f.getValue() instanceof File) {
          fs.add(Pair.of(f.getKey(), (File) f.getValue()));
        }
      }
      return fs;
    }

    public void remove(String name) {
      files.remove(requireNonNull(name));
    }
  }

  private static final InMemFileSystem INSTANCE = new InMemFileSystem();

  private final AtomicReference<Directory> root = new AtomicReference<>(new Directory());

  public static InMemFileSystem get() {
    return INSTANCE;
  }

  public InMemFileSystem reset() {
    root.set(new Directory());
    return this;
  }

  private DirOrFile getDirOrFile(List<String> path) {
    Directory p = mkParent(path);
    String fname = path.get(path.size() - 1);
    DirOrFile dirOrFile = p.get(fname);
    if (dirOrFile == null) {
      throw new RuntimeException(
          "No such file or directory: " + fromCanonicalPath(path));
    }
    return dirOrFile;
  }

  public void remove(String path) {
    List<String> cpath = toCanonicalPath(path);
    if (cpath == null || cpath.isEmpty()) {
      throw new IllegalArgumentException("Cannot remove directory root: " + path);
    }
    Directory dir = getParent(cpath);
    if (dir == null) {
      return;
    }
    dir.remove(cpath.get(cpath.size() - 1));
  }

  public InMemFileSystem setFile(String path, Collection content) {
    return setFile(path, null, content);
  }

  public InMemFileSystem setFile(String path, Duration readDelay, Collection content) {
    List<String> cpath = toCanonicalPath(path);
    if (cpath.isEmpty()) {
      throw new IllegalArgumentException("Invalid path: " + path);
    }
    Directory p = mkParent(cpath);
    String fname = cpath.get(cpath.size() - 1);
    if (p.get(fname) instanceof Directory) {
      throw new IllegalArgumentException("Already a directory: " + path);
    }
    p.set(fname, new File(content, readDelay));
    return this;
  }

  public Collection getFile(String path) {
    List<String> cpath = toCanonicalPath(path);
    if (cpath.isEmpty()) {
      throw new IllegalArgumentException("Invalid path: " + path);
    }
    List<String> dirPath = cpath.subList(0, cpath.size() - 1);
    Directory dir = getParent(dirPath);
    if (dir == null) {
      throw new IllegalArgumentException(
          "No such directory: " + fromCanonicalPath(dirPath));
    }
    DirOrFile f = dir.get(cpath.get(cpath.size() - 1));
    if (f == null) {
      throw new IllegalArgumentException("No such file: " + path);
    }
    if (!(f instanceof File)) {
      throw new IllegalArgumentException("No a file: " + path);
    }
    return ((File) f).storage;
  }

  private Directory getParent(List<String> path) {
    Directory curr = root.get();
    path = path.subList(0, path.size() - 1);
    StringBuilder currPath = new StringBuilder();
    for (String step : path) {
      currPath.append('/').append(step);
      DirOrFile dirOrFile = curr.get(step);
      if (dirOrFile instanceof File) {
        throw new IllegalArgumentException("Not a directory: " + currPath);
      }
      if (dirOrFile == null) {
        return null;
      }
      curr = (Directory) dirOrFile;
    }
    return curr;
  }

  /** Returns a the parent directory of the given path */
  private Directory mkParent(List<String> path) {
    return mkDirectory(path.subList(0, path.size() - 1));
  }

  private Directory mkDirectory(List<String> path) {
    Directory curr = root.get();
    for (String step : path) {
      DirOrFile dirOrFile = curr.get(step);
      if (dirOrFile == null) {
        curr.set(step, dirOrFile = new Directory());
      }
      if (!(dirOrFile instanceof Directory)) {
        throw new RuntimeException("Not a directory: " + fromCanonicalPath(path));
      }
      curr = (Directory) dirOrFile;
    }
    return curr;
  }

  private String fromCanonicalPath(List<String> steps) {
    return String.join("/", steps);
  }

  private List<String> toCanonicalPath(String path) {
    String[] names = path.split("/");
    return Arrays.asList(names)
        .stream().filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}