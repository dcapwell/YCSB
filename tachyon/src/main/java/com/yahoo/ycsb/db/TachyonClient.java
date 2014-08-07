package com.yahoo.ycsb.db;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import tachyon.client.ReadType;
import tachyon.client.RemoteBlockInStream;
import tachyon.client.RemoteBlockInStreams;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public final class TachyonClient extends DB {

  public static final int Ok = 0;
  public static final int ServerError = -1;

  private TachyonFS fileSystem;

  @Override
  public void init() throws DBException {
    final String address = getProperties().getProperty("address");
    Preconditions.checkNotNull(address, "Error, must specify 'address'");
    try {
      fileSystem = TachyonFS.get(address);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    DataOutputStream stream = null;
    try {
      final TachyonFile file = fileSystem.getFile(table + "/" + key);
      stream = new DataOutputStream(file.getOutStream(WriteType.MUST_CACHE));
      writeTo(stream, values);
    } catch (IOException e) {
      System.err.println("Error accessing path " + table + "/" + key);
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  private void writeTo(DataOutputStream stream, HashMap<String, ByteIterator> values) throws IOException {
    stream.writeInt(values.size());

    for (final Map.Entry<String, ByteIterator> e : values.entrySet()) {
      byte[] data = e.getValue().toArray();

      stream.writeUTF(e.getKey());
      stream.writeInt(data.length);
      stream.write(data);
    }
  }

  @Override
  public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    DataInputStream stream = null;
    try {
      final TachyonFile file = fileSystem.getFile(table + "/" + key);
//      stream = new DataInputStream(file.getInStream(ReadType.NO_CACHE));
      stream = new DataInputStream(createStream(file, ReadType.NO_CACHE, 0));
      readInto(stream, result);
    } catch (IOException e) {
      System.err.println("Error accessing path " + table + "/" + key);
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  private static InputStream createStream(final TachyonFile file, final ReadType readType, final int blockIndex) throws IOException {
    // need to avoid the local read path, so use reflection to bypass this.
    // https://tachyon.atlassian.net/browse/TACHYON-53
    return RemoteBlockInStreams.create(file, readType, blockIndex);
  }

  private void readInto(DataInputStream stream, HashMap<String, ByteIterator> result) throws IOException {
    final int size = stream.readInt();
    for (int i = 0; i < size; i++) {
      String key = stream.readUTF();
      byte[] data = new byte[stream.readInt()];
      stream.read(data);
      result.put(key, new ByteArrayByteIterator(data));
    }
  }

  @Override
  public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Ok;
  }

  @Override
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    return Ok;
  }

  @Override
  public int delete(String table, String key) {
    return Ok;
  }
}
