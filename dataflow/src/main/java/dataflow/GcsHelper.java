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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

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
}
