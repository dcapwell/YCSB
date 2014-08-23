package com.yahoo.ycsb.db;

import com.google.common.base.Throwables;
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
  private int maxWrites = 4;

  @Override
  public void init() throws DBException {
    log("Running Init");

    maxWrites = prop("maxWrites", 4);
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
    log("Running insert");

    final TachyonFile file = getAndCreateFile(path(table, key));
    return write(file, values);
  }

  @Override
  public int read(final String table,
                  final String key,
                  final Set<String> fields,
                  final HashMap<String, ByteIterator> result) {
    log("Running read");

    DataInputStream stream = null;
    try {
      final TachyonFile file = getFile(path(table, key));
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
  public int scan(final String table,
                  final String startkey,
                  final int recordcount,
                  final Set<String> fields,
                  final Vector<HashMap<String, ByteIterator>> result) {
    //TODO what makes sense for scan?
    log("Running scan");
    return Ok;
  }

  @Override
  public int update(final String table,
                    final String key,
                    final HashMap<String, ByteIterator> values) {
    // tachyon overrides as the default...
    log("Running update");;
    final TachyonFile file = getAndUpdateFile(path(table, key));
    return write(file, values);
  }

  @Override
  public int delete(final String table, final String key) {
    log("Running delete");
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
    } catch(IOException e) {
      log(e, "Error accessing path " + file.getPath());
      return ServerError;
    } finally {
      Closeables.closeQuietly(stream);
    }
    return Ok;
  }

  private TachyonFile getFile(final String path) {
    try {
      TachyonFile file = fileSystem.getFile(path);
      return file;
    } catch(IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private TachyonFile getAndCreateFile(final String path) {
    try {
      int id = fileSystem.createFile(path);
      TachyonFile file = fileSystem.getFile(id);
      return file;
    } catch(IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private TachyonFile getAndUpdateFile(final String path) {
    try {
      fileSystem.delete(path, false);
      return getAndCreateFile(path);
    } catch(IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void log(final String msg) {
    System.out.println(msg);
  }

  private String prop(final String key) {
    String value = getProperties().getProperty(key);
    return checkNotNull(value, "Error, must specify '" + key + "'");
  }

  private int prop(final String key, int defaultValue) {
    String value = getProperties().getProperty(key);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private void writeTo(final DataOutputStream stream,
                              final HashMap<String, ByteIterator> values) throws IOException {
    stream.writeInt(values.size());

    int counter = 0;
    for (final Map.Entry<String, ByteIterator> e : values.entrySet()) {
      if (counter++  > maxWrites) break;

      byte[] data = e.getValue().toArray();

      stream.writeUTF(e.getKey());
      stream.writeInt(data.length);
      stream.write(data);
    }
  }

  private void readInto(final DataInputStream stream,
                               final HashMap<String, ByteIterator> result) throws IOException {
    final int size = stream.readInt();
    for (int i = 0; i < size && i < maxWrites; i++) {
      String key = stream.readUTF();
      byte[] data = new byte[stream.readInt()];
      stream.read(data);
      result.put(key, new ByteArrayByteIterator(data));
    }
  }

  private static InputStream createStream(final TachyonFile file, final ReadType readType)
      throws IOException {
    // need to avoid the local read path
    // https://tachyon.atlassian.net/browse/TACHYON-53
    return RemoteBlockInStreams.create(file, readType, 0);
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