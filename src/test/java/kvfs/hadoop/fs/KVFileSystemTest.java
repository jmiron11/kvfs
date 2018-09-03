package mmux.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import kvfs.hadoop.fs.KVFileSystem;
import mmux.StorageServer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class KVFileSystemTest {


  @Rule
  public TestName testName = new TestName();

  private KVFileSystem kvfs;

  private static final int FILENAME_LENGTH = 8;
  private static final String TEST_STRING = "teststring";
  private static final String host = "localhost";
  private static long port = 40149;

  public KVFileSystemTest() throws IOException {
    Configuration c = new Configuration();
    c.set(FileSystem.FS_DEFAULT_NAME_KEY, "kvfs://" + host + ":" + port);
    kvfs = new KVFileSystem();
    kvfs.initialize(URI.create("kvfs://" + host + ":" + port), c);
  }

  @Before
  public void setUp() {
    System.out.println("=> Running " + testName.getMethodName());
    startServers();
  }

  @After
  public void tearDown() throws InterruptedException {
    stopServers();
  }

  private void stopServers() {
  }

  private void startServers() {

  }

  private String randomFilename() {
    return RandomStringUtils.randomAlphabetic(FILENAME_LENGTH);
  }

  private String randomData(int dataLength) {
    return RandomStringUtils.randomAlphabetic(dataLength);
  }

  @Test
  public void testCreateWriteRenameReadFile() throws InterruptedException, IOException {
    // Create file
    String originalFilename = randomFilename();
    String renamedFilename = randomFilename();

    Path filePath = new Path(originalFilename);
    FSDataOutputStream out = fs.create(filePath);

    // Write string to file
    byte[] dataBytes = TEST_STRING.getBytes();
    out.write(dataBytes, 0, dataBytes.length);
    out.close();

    Path renamePath = new Path(renamedFilename);
    kvfs.rename(filePath, renamePath);

    // Open created file
    FSDataInputStream in = fs.open(renamePath, dataBytes.length);

    // Read all data from the file
    byte[] readBytes = new byte[dataBytes.length];
    in.readFully(0, readBytes);

    // Ensure data read from file is the string we wrote to file.
    Assert.assertArrayEquals(dataBytes, readBytes);
    Assert.assertEquals(TEST_STRING, new String(readBytes));
    in.close();

    // Clean up test
    kvfs.delete(renamePath, false);
  }

  @Test
  public void testMakeAndDeleteDir() throws InterruptedException, IOException {
    Path expectedWorkingDirectory = new Path("/fsdir");
    Assert.assertEquals(kvfs.getWorkingDirectory(), expectedWorkingDirectory);

    // Create a new directory and ls the base directory
    String dirName = randomFilename();
    kvfs.mkdirs(new Path(dirName), FsPermission.getDefault());

    // Successfully list the directory
    FileStatus[] files = kvfs.listStatus(new Path(dirName));
    Assert.assertEquals(0, files.length);

    // Clean up test
    kvfs.delete(new Path(dirName), true);
  }

  @Test
  public void listStatusWithNestedDirectories() throws InterruptedException, IOException {
    String dirName = randomFilename();
    kvfs.mkdirs(new Path(dirName), FsPermission.getDefault());
    kvfs.mkdirs(new Path(dirName + "/" + randomFilename()),
        FsPermission.getDefault());

    /* Create a file */
    Path filePath = new Path(dirName + "/" + randomFilename());
    FSDataOutputStream out = kvfs.create(filePath);

    // Write string to file
    byte[] dataBytes = TEST_STRING.getBytes();
    out.write(dataBytes, 0, dataBytes.length);
    out.close();

    FileStatus[] files = kvfs.listStatus(new Path(dirName));
    Assert.assertEquals(2, files.length);

    kvfs.delete(new Path(dirName), true);
  }

  void createFileWithData(KVFileSystem fs, Path p, byte[] data) throws IOException {
    FSDataOutputStream out_stream = fs.create(p);
    out_stream.write(data, 0, data.length);
    out_stream.close();
  }

  @Test
  public void bufferedReadFile() throws InterruptedException, IOException {
    Path filePath = new Path(randomFilename());
    int dataLength = 80;
    byte[] data = randomData(dataLength).getBytes();
    createFileWithData(kvfs, filePath, data);

    int buffSize = 8;
    FSDataInputStream in = kvfs.open(filePath);
    byte[] buf = new byte[buffSize];
    int totalBytesRead = 0;
    int bytesRead = in.read(buf);
    totalBytesRead += bytesRead;
    byte[] targetSlice = ArrayUtils.subarray(data, 0, buffSize);
    Assert.assertArrayEquals(buf, targetSlice);

    while (bytesRead >= 0) {
      bytesRead = in.read(buf);
      if(bytesRead > 0) {
        targetSlice = ArrayUtils.subarray(data, totalBytesRead, totalBytesRead+bytesRead);
        Assert.assertArrayEquals(buf, targetSlice);
        totalBytesRead += bytesRead;
      }
    }

    Assert.assertEquals(data.length, totalBytesRead);

  }

  @Test
  public void readFileFully() throws InterruptedException, IOException {
    Path filePath = new Path(randomFilename());
    int dataLength = 80;
    byte[] data = randomData(dataLength).getBytes();
    createFileWithData(kvfs, filePath, data);

    FSDataInputStream in = kvfs.open(filePath);
    byte[] buf = new byte[dataLength];
    in.readFully(0, buf);
    Assert.assertArrayEquals(data, buf);
  }

  @Test
  public void fileDoesNotExist() throws InterruptedException, IOException {
      Assert.assertFalse(kvfs.exists(new Path(randomFilename())));
  }

  @Test
  public void seekThenRead() throws InterruptedException, IOException {
    Path filePath = new Path(randomFilename());
    int dataLength = 80;
    byte[] data = randomData(dataLength).getBytes();
    createFileWithData(kvfs, filePath, data);

    FSDataInputStream in = kvfs.open(filePath);
    in.seek(40);
    byte[] buf = new byte[40];
    in.read(buf, 0, 40);

    byte[] targetSlice = ArrayUtils.subarray(data, 40, 80);
    Assert.assertArrayEquals(buf, targetSlice);
  }
}
