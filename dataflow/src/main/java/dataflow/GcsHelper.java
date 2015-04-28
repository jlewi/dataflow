/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dataflow;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;


/**
 * Helpful routines for working with GCS.
 *
 * The class takes a GcsUtil operation in the constructor and provides higher
 * level functions.
 */
public class GcsHelper {
  private GcsUtil gcsUtil;

  public GcsHelper(GcsUtil gcsUtil) {
    this.gcsUtil = gcsUtil;
  }

  public void copyToLocalFile(GcsPath gcsPath, String localFile)
      throws IOException {
    SeekableByteChannel byteChannel = gcsUtil.open(gcsPath);
    FileOutputStream outStream = new FileOutputStream(localFile);
    // Allocate a 32Kb buffer.
    int capacity = 2 << 14;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    long size = byteChannel.size();
    while (byteChannel.position() < size) {
      buffer.clear();
      int read = byteChannel.read(buffer);
      if (read <= 0) {
        throw new RuntimeException(
            "There was a problem reading from: " + gcsPath.toString() +
            " read din't return any bytes but we aren't at the end of the " +
            "stream");
      }
      outStream.write(buffer.array(), 0, read);
    }

    outStream.close();
    byteChannel.close();
  }

  /**
   * Copy the local file to the specified GCSPath.
   * @param localFile
   * @param gcsPath the type of object, eg "text/plain".
   */
  public void copyLocalFileToGcs(String localFile, GcsPath gcsPath, String type)
      throws IOException {
    WritableByteChannel byteChannel = gcsUtil.create(gcsPath, type);
    FileInputStream inStream = new FileInputStream(localFile);

    // Allocate a 32Kb buffer.
    int capacity = 2 << 14;
    //byte[] buffer = new byte
    //long size = byteChannel.size();
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    if (!buffer.hasArray()) {
      // This should not happen.
      throw new RuntimeException("ByteBuffer is not backed by an array.");
    }
    while (true) {
      buffer.clear();
      int read = inStream.read(buffer.array());
      if (read <= 0) {
        break;
      }
      buffer.limit(read);
      byteChannel.write(buffer);
    }

    inStream.close();
    byteChannel.close();
  }
}
