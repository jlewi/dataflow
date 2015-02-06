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

import java.util.List;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ListContainersParam;
import com.spotify.docker.client.messages.Container;

import dataflow.UnionExample.Options.OutputFactory;

/**
 * A simple Dataflow to illustrate connecting to the docker client.
 */
public class DockerClientDataflow {
  public static class ListContainers extends DoFn<String, String> {
    @Override
    public void processElement(DoFn<String, String>.ProcessContext context)
        throws Exception {
      String dockerAddress = "unix:///var/run/docker.sock";
      DockerClient docker = new DefaultDockerClient(dockerAddress);

      ListContainersParam param;

      List<Container> containers = docker.listContainers(ListContainersParam.allContainers());
      for (Container c : containers) {
        context.output("Container c:" + c.id() + " image:" + c.image());
      }
    }
  }

  public static interface Options extends PipelineOptions {
    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    if (options.getOutput() == null) {
      throw new IllegalArgumentException("output must be non null.");
    }
    Pipeline p = Pipeline.create(options);

    // Dummy input to drive the pipeline.
    PCollection<String> inputs = p.apply(Create.of("input"));

    inputs.apply(ParDo.of(new ListContainers()))
    .apply(TextIO.Write.named("Write")
        .to(options.getOutput()));
    p.run();
  }
}
