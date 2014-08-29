package com.yahoo.ycsb.db;

import com.google.common.io.Closeables;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import tachyon.client.ReadType;
import tachyon.client.RemoteBlockInStreams;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.org.apache.thrift.TException;

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

  private static final int Ok = 0;
  private static final int ServerError = -1;
  private static final int HttpError = -2;
  private static final int NoMatchingRecord = -3;

  private static final char PathSeperator = '/';

  private TachyonFS fileSystem;

  @Override
  public void init() throws DBException {
    log("Running Init");

    final String uri = prop("uri");
    try {
      fileSystem = TachyonFS.get(uri);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    log("Running cleanup");

    try {
      fileSystem.close();
    } catch (TException e) {
      throw new DBException(e);
    }
  }

  @Override
  public int insert(final String table,
                    final String key,
                    final HashMap<String, ByteIterator> values) {
    final TachyonFile file;
    try {
      file = getFile(path(table, key));
      return write(file, values);
    } catch (IOException e) {
      log(e, "Error inserting path " + path(table, key));
      return ServerError;
    }
  }

  @Override
  public int read(final String table,
                  final String key,
                  final Set<String> fields,
                  final HashMap<String, ByteIterator> result) {
    DataInputStream stream = null;
    try {
      final TachyonFile file = getFile(path(table, key));
      stream = new DataInputStream(createStream(file, ReadType.NO_CACHE));
      readInto(stream);
    } catch (IOException e) {
      log(e, "Error reading path " + path(table, key));
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  @Override
  public int scan(final String table,
                  final String startkey,
                  final int recordcount,
                  final Set<String> fields,
                  final Vector<HashMap<String, ByteIterator>> result) {
    //TODO what makes sense for scan?
    return Ok;
  }

  @Override
  public int update(final String table,
                    final String key,
                    final HashMap<String, ByteIterator> values) {
    final TachyonFile file;
    try {
      // delete before writing
      fileSystem.delete(path(table, key), false);

      file = getFile(path(table, key));
      return write(file, values);
    } catch (IOException e) {
      log(e, "Error updating path " + path(table, key));
      return ServerError;
    }
  }

  @Override
  public int delete(final String table, final String key) {
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

  private int write(final TachyonFile file, final HashMap<String, ByteIterator> values) {
    DataOutputStream stream = null;
    try {
      stream = new DataOutputStream(file.getOutStream(WriteType.MUST_CACHE));
      writeTo(stream, values);
    } catch (IOException e) {
      log(e, "Error accessing path " + file.getPath());
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  private TachyonFile getFile(final String path) throws IOException {
    TachyonFile file = fileSystem.getFile(path);
    if (file == null) {
      file = createFile(path);
    }
    return file;
  }

  private TachyonFile createFile(final String path) throws IOException {
    int id = fileSystem.createFile(path);
    TachyonFile file = fileSystem.getFile(id);
    return file;
  }

  private String prop(final String key) {
    String value = getProperties().getProperty(key);
    return checkNotNull(value, "Error, must specify '" + key + "'");
  }

  private void writeTo(final DataOutputStream stream,
                       final HashMap<String, ByteIterator> values) throws IOException {
    stream.writeInt(values.size());

    for (final Map.Entry<String, ByteIterator> e : values.entrySet()) {
      byte[] data = e.getValue().toArray();

      stream.writeUTF(e.getKey());
      stream.writeInt(data.length);
      stream.write(data);
    }
  }

  private final byte[] readBuffer = new byte[8000000]; // 8mb

  private void readInto(final DataInputStream stream) throws IOException {
    final int size = stream.readInt();
    int length;
    for (int i = 0; i < size; i++) {
      stream.readUTF(); // key
      length = stream.readInt();
      while (length > readBuffer.length) {
        stream.read(readBuffer);
        length = length - readBuffer.length;
      }
      stream.read(readBuffer, 0, length);
    }
  }

  private static InputStream createStream(final TachyonFile file, final ReadType readType)
      throws IOException {
    // need to avoid the local read path
    // https://tachyon.atlassian.net/browse/TACHYON-53
    return RemoteBlockInStreams.create(file, readType, 0);
  }

  private static void log(final String msg) {
    System.out.println(msg);
  }

  private static void log(final Exception e, final String msg) {
    System.err.println(msg + ": " + e.getMessage());
    e.printStackTrace();
  }

  private static String path(final String parent, final String child) {
    return new StringBuilder(parent.length() + child.length() + 2).
        append(PathSeperator).
        append(parent).
        append(PathSeperator).
        append(child).
        toString();
  }
}
