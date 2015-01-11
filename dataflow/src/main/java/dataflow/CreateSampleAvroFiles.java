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
// Author: Jeremy Lewi (jeremy@lewi.us)
package dataflow;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

public class CreateSampleAvroFiles {
  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    String getInput();
    void setInput(String value);

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

    ArrayList<Left> lefts = new ArrayList<Left> ();
    for (int i = 0; i < 10; ++i) {
      Left left = new Left();
      left.setLeftValue(i);
      lefts.add(left);
    }

    ArrayList<Right> rights = new ArrayList<Right> ();
    for (int i = 11; i < 20; ++i) {
      Right right = new Right();
      right.setRightValue(i);
      rights.add(right);
    }

    GcsUtil.GcsUtilFactory gcsUtilFactory = new GcsUtil.GcsUtilFactory();
    GcsUtil gcsUtil = gcsUtilFactory.create(options);

    GcsPath gcsLeftOutputPath = GcsPath.fromUri(outputPath + "/left.avro");
    WritableByteChannel leftChannel = gcsUtil.create(gcsLeftOutputPath, "avro/binary");
    OutputStream leftStream = Channels.newOutputStream(leftChannel);
    AvroFileUtil.writeRecords(leftStream, lefts, Left.SCHEMA$);

    GcsPath gcsRightOutputPath = GcsPath.fromUri(outputPath + "/right.avro");
    WritableByteChannel rightChannel = gcsUtil.create(gcsRightOutputPath, "avro/binary");
    OutputStream rightStream = Channels.newOutputStream(rightChannel);
    AvroFileUtil.writeRecords(rightStream, rights, Right.SCHEMA$);
  }
}
