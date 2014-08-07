package com.yahoo.ycsb.db;

import com.google.common.io.Closeables;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import tachyon.client.ReadType;
import tachyon.client.RemoteBlockInStreams;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TachyonClient extends DB {

  public static final int Ok = 0;
  public static final int ServerError = -1;
  public static final int HttpError = -2;
  public static final int NoMatchingRecord = -3;

  private TachyonFS fileSystem;

  @Override
  public void init() throws DBException {
    final String uri = prop("uri");
    try {
      fileSystem = TachyonFS.get(uri);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    DataOutputStream stream = null;
    try {
      final TachyonFile file = fileSystem.getFile(path(table, key));
      stream = new DataOutputStream(file.getOutStream(WriteType.MUST_CACHE));
      writeTo(stream, values);
    } catch (IOException e) {
      log(e, "Error accessing path " + path(table, key));
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  @Override
  public int read(
      final String table,
      final String key,
      final Set<String> fields,
      final HashMap<String, ByteIterator> result) {
    DataInputStream stream = null;
    try {
      final TachyonFile file = fileSystem.getFile(path(table, key));
      stream = new DataInputStream(createStream(file, ReadType.NO_CACHE));
      readInto(stream, result);
    } catch (IOException e) {
      log(e, "Error accessing path " + path(table, key));
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  @Override
  public int scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Ok;
  }

  @Override
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public int delete(String table, String key) {
    try {
      if (!fileSystem.delete(path(table, key), false)) {
        return NoMatchingRecord;
      }
    } catch (FileNotFoundException e) {
      log(e, "Error deleting path " + path(table, key));
      return NoMatchingRecord;
    } catch (IOException e) {
      log(e, "Error deleting path " + path(table, key));
      return ServerError;
    }
    return Ok;
  }

  private String prop(final String key) {
    String value = getProperties().getProperty(key);
    return checkNotNull(value, "Error, must specify '" + key + "'");
  }

  private static void writeTo(
      final DataOutputStream stream,
      final HashMap<String, ByteIterator> values) throws IOException {
    stream.writeInt(values.size());

    for (final Map.Entry<String, ByteIterator> e : values.entrySet()) {
      byte[] data = e.getValue().toArray();

      stream.writeUTF(e.getKey());
      stream.writeInt(data.length);
      stream.write(data);
    }
  }

  private static void readInto(
      final DataInputStream stream,
      final HashMap<String, ByteIterator> result) throws IOException {
    final int size = stream.readInt();
    for (int i = 0; i < size; i++) {
      String key = stream.readUTF();
      byte[] data = new byte[stream.readInt()];
      stream.read(data);
      result.put(key, new ByteArrayByteIterator(data));
    }
  }

  private static InputStream createStream(
      final TachyonFile file,
      final ReadType readType) throws IOException {
    // need to avoid the local read path
    // https://tachyon.atlassian.net/browse/TACHYON-53
    return RemoteBlockInStreams.create(file, readType, 0);
  }

  private static void log(final Exception e, final String msg) {
    System.err.println(msg + ": " + e.getMessage());
  }

  private static String path(final String parent, final String child) {
    return new StringBuilder().
        append(parent).
        append("/").
        append(child).
        toString();
  }
}
