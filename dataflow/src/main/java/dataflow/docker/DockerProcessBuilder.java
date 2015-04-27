/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.LogsParameter;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;

/**
 * Build a proccess to run in a shell process.
 */
public class DockerProcessBuilder {
  private ProcessBuilder builder;
  private final List<String> command;
  private String imageName;

  private final List<VolumeMapping> volumeMappings;
  private final DockerClient docker;

  // Information about how to map a local file system path to a path in
  // the docker filesystem.
  private class VolumeMapping {
    public final String local;
    public final String container;

    public VolumeMapping(String local, String container) {
      this.local = local;
      this.container = container;
    }

    /**
     * Return a string representing the value to pass along with the -v
     * option in the docker run command.
     */
    public String toArgument() {
      return local + ":" + container;
    }
  }

  public DockerProcessBuilder(List<String> command, DockerClient docker) {
    this.command = command;
    volumeMappings = new ArrayList<VolumeMapping>();
    this.docker = docker;
  }

  /**
   * Add a mapping from localDir on the local filesystem to the directory
   * containerDir in the filesystem.
   *
   * @param localDir
   * @param containerDir
   */
  public void addVolumeMapping(String localDir, String containerDir) {
    volumeMappings.add(new VolumeMapping(localDir, containerDir));
  }

  public void setImage(String imageName) {
    this.imageName = imageName;
  }

  public DockerProcess start() throws IOException, DockerException, InterruptedException {
    ContainerConfig config = ContainerConfig.builder()
        .image(imageName)
        .cmd(command)
        .attachStderr(true)
        .attachStderr(true)
        .build();

    ContainerCreation creation = docker.createContainer(config);
    String id = creation.id();
    ContainerInfo info = docker.inspectContainer(id);
    docker.startContainer(id);
    docker.waitContainer(id);

    LogStream stdOut = docker.logs(id, LogsParameter.STDOUT);
    System.out.println(stdOut.readFully());
    LogStream stdErr = docker.logs(id, LogsParameter.STDERR);
    System.out.println(stdErr.readFully());

    // Remove the container.
    docker.removeContainer(id);

    return null;
  }
}
