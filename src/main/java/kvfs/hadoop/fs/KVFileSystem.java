package kvfs.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import redis.clients.jedis.Jedis;

public class KVFileSystem extends FileSystem {

  private static final int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
  private static final String DEFAULT_PERSISTENT_PATH = "local://tmp";
  private static final String DEFAULT_GROUP = "defaultgroup";
  private static final String DEFAULT_USER = System.getProperty("user.name");


  private URI uri;
  private String host;
  private int port;
  private Path workingDir;

  // Currently Jedis
  private Jedis client;

  private int blockSize;
  private String persistentPath;
  private String group;
  private String user;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.host = uri.getHost();
    this.port = uri.getPort();

    // Redis
    this.client = new Jedis(host, port);

    this.uri = URI.create(String.format("kvfs://%s:%d", uri.getHost(), uri.getPort()));

    this.blockSize = conf.getInt("kvfs.blocksize", DEFAULT_BLOCK_SIZE);
    this.persistentPath = conf.get("kvfs.persistent_path", DEFAULT_PERSISTENT_PATH);
    this.group = conf.get("kvfs.group", DEFAULT_GROUP);
    this.user = conf.get("kvfs.user", DEFAULT_USER);

    String path = uri.getPath();
    if (path == null || path.equals("")) {
      path = "/fsdir/";
      mkdirs(new Path(path));
    }
    this.workingDir = new Path(path);
  }

  @Override
  public void close() throws IOException {
    client.close();
    super.close();
  }

  @Override
  public String getScheme() {
    return "kvfs";
  }

  @Override
  public int getDefaultPort() {
    return 9090;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    return new FSDataInputStream(new KVInputStream(client, pathStr, blockSize));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    return new FSDataOutputStream(new KVOutputStream(client, pathStr, (int)blockSize));
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) {
    throw new UnsupportedOperationException(
        "Append is not supported by " + KVFileSystem.class.getName());
  }



  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    String inputPath = makeAbsolute(path).toString();
    String outputPath = makeAbsolute(path).toString();

    // Get all keys associated with this file.
    Set<String> keys = client.keys(inputPath + "*");

    // Rename all the blocks of the key
    for(String key : keys) {
      // Replace inputPath with output path in each key.
      String newPath = key.replaceAll(inputPath, outputPath);
      client.rename(key, newPath);
    }

    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    String inputPath = makeAbsolute(path).toString();
    Set<String> keys = client.keys(inputPath + "*");

    for (String key : keys) {
      client.del(key);
    }

    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    throw new UnsupportedOperationException(
        "listStatus it not supported by " + KVFileSystem.class.getName()
    );
//    Path absolutePath = makeAbsolute(path);
//    FileStatus status = getFileStatus(absolutePath);
//
//    if (status.isDirectory()) {
//      try {
//        List<rpc_dir_entry> entries = client.fs().directoryEntries(absolutePath.toString());
//        FileStatus[] statuses = new FileStatus[entries.size()];
//        int i = 0;
//        for (rpc_dir_entry entry : entries) {
//          Path child = new Path(absolutePath, entry.name);
//          rpc_file_status fileStatus = entry.status;
//          // FIXME: Remove hardcoded parameter: permissions
//          FsPermission perm = new FsPermission("777");
//          long fileTS = 100;
//          if (fileStatus.getType() == rpc_file_type.rpc_regular) {
//            rpc_data_status dataStatus = client.fs().dstatus(child.toString());
//            // FIXME: Remove hardcoded parameter: access_time
//            // FIXME: Support storing username & groups in MemoryMUX
//            statuses[i] = new FileStatus(dataStatus.getDataBlocksSize(), false,
//                dataStatus.getChainLength(), blockSize, fileTS, fileTS, perm, user, group,
//                child);
//          } else {
//            statuses[i] = new FileStatus(0, true, 0, 0, fileTS, fileTS, perm, user,
//                group, child);
//          }
//          i++;
//        }
//        return statuses;
//      } catch (TException e) {
//        throw new FileNotFoundException(path.toUri().getRawPath());
//      }
//    }
//    return new FileStatus[]{status};
  }

  @Override
  public void setWorkingDirectory(Path path) {
    workingDir = makeAbsolute(path);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    String inputPath = makeAbsolute(path).toString();
    client.set(inputPath, "dir");
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    throw new UnsupportedOperationException(
        "getFileStatus it not supported by " + KVFileSystem.class.getName()
    );
//    try {
//      Path absolutePath = makeAbsolute(path);
//      rpc_file_status fileStatus = client.fs().status(absolutePath.toString());
//      // FIXME: Remove hardcoded parameter: permissions
//      FsPermission perm = new FsPermission("777");
//      long fileTS = 100;
//      if (fileStatus.getType() == rpc_file_type.rpc_regular) {
//        rpc_data_status dataStatus = client.fs().dstatus(absolutePath.toString());
//        return new FileStatus(dataStatus.getDataBlocksSize(), false, dataStatus.getChainLength(),
//            blockSize, fileTS, fileTS, perm, user, group, absolutePath);
//      } else {
//        return new FileStatus(0, true, 0, 0, fileTS, fileTS, perm, user, group, absolutePath);
//      }
//    } catch (TException e) {
//      throw new FileNotFoundException();
//    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
  }

  private String removeMmfsPrefix(String s) {
    URI uri = URI.create(s);
    return uri.getPath();
  }

  private Path makeAbsolute(Path path) {
    String pathString = removeMmfsPrefix(path.toString());

    if (path.isAbsolute()) {
      return new Path(pathString);
    } else if (pathString.equals("")) {
      return workingDir;
    } else {
      return new Path(workingDir, pathString);
    }
  }
}
