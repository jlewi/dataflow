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
package dataflow.examples;

import java.io.IOException;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

import dataflow.GcsHelper;

/**
 * An example of using the GcsHelper class.
 */
public class GcsHelperExample {
  /**
   * Options for the example.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the GCS file to read from")
    String getInput();
    void setInput(String value);

    @Description("Path of the local file to write to")
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(
        args).withValidation().as(Options.class);

    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(options);
    GcsHelper gcsHelper = new GcsHelper(gcsUtil);

    gcsHelper.copyToLocalFile(
        GcsPath.fromUri(options.getInput()), options.getOutput());
  }
}
