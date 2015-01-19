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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;


/**
 * A small example to reproduce the issue with GCS threads still running after we close
 * the channel.
 */
public class GCSRunningThreads {
  private static final Logger sLogger = LoggerFactory.getLogger(
      GCSRunningThreads.class);
  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to write to")
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    String outputPath = options.getOutput();
    if (outputPath == null) {
      throw new IllegalArgumentException("Must specify --output");
    }

    GcsUtil.GcsUtilFactory gcsUtilFactory = new GcsUtil.GcsUtilFactory();
    GcsUtil gcsUtil = gcsUtilFactory.create(options);

    GcsPath gcsOutputPath = GcsPath.fromUri(outputPath);
    WritableByteChannel outChannel = gcsUtil.create(gcsOutputPath, "text/plain");
    OutputStream outStream = Channels.newOutputStream(outChannel);
    PrintWriter writer = new PrintWriter(outStream);

    writer.append("Hello World!");
    writer.flush();
    sLogger.info("Done with write");
    outChannel.close();
    sLogger.info("Done with close");
  }
}
