package kvfs.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import redis.clients.jedis.Jedis;

public class KVOutputStream extends OutputStream {

  private boolean closed;
  private KVBlock block;
  private long blockNum;
  private Jedis client;
  private String key;
  private String lastBlockKey;
  private String metadataKey;

  KVOutputStream(Jedis client, String pathStr, int blockSize) throws IOException {
    this.blockNum = 0;
    this.client = client;
    this.block = new KVBlock(blockSize);
    this.key = pathStr;

    this.metadataKey = pathStr;
    if (!client.exists(metadataKey)) {
      client.set(metadataKey, "file");
    }

    this.lastBlockKey = pathPrefix("lastBlock");
    String lastBlockValue = String.valueOf(blockNum);
    if (!client.exists(lastBlockKey)) {
      client.set(lastBlockKey, lastBlockValue);
    }
  }

  private String pathPrefix(String postfix) {
    return key + "/" + postfix;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (block.remaining() == 0) {
      flush();
    }
    block.write((byte) b);
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    while (len > 0) {
      int toWrite = Math.min(block.remaining(), len);
      block.write(b, off, toWrite);
      off += toWrite;
      len -= toWrite;

      if (block.remaining() == 0) {
        flush();
      }
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    String key = String.valueOf(pathPrefix(String.valueOf(blockNum)));
    String value = ByteBufferUtils.toString(block.getData());
    if (client.exists(key)) {
      client.set(key, value);
    }
    if (block.remaining() == 0) {
      block.reset();
      blockNum++;
      client.set(lastBlockKey, String.valueOf(blockNum));
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    flush();
    super.close();
    closed = true;
  }

}
