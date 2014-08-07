package tachyon.client;

import java.io.IOException;

public final class RemoteBlockInStreams {
  private RemoteBlockInStreams() {}

  public static RemoteBlockInStream create(final TachyonFile file, final ReadType readType, final int blockIndex) throws IOException {
    return new RemoteBlockInStream(file, readType, blockIndex);
  }
}
