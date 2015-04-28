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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;

import dataflow.docker.DockerProcessBuilder;

/**
 * A simple example of using the spotify docker client.
 */
public class DockerClientExample {
  private static final Logger logger =
      LoggerFactory.getLogger(DockerClientExample.class.getName());
  static private String createLocalTempDir() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    // TODO(jlewi): Is there a java function we could use?
    File temp = null;
    try {
      temp = File.createTempFile("temp-" + timestamp + "-", "");
    } catch (IOException exception) {
      logger.error("Could not create temporary file.", exception);
      System.exit(-1);
    }

    if(!(temp.delete())){
      throw new RuntimeException(
          "Could not delete temp file: " + temp.getAbsolutePath());
    }

    if(!(temp.mkdir())) {
      throw new RuntimeException(
          "Could not create temp directory: " + temp.getAbsolutePath());
    }
    return temp.getPath();
  }

  public static void main(String[] args) throws IOException, DockerException, InterruptedException {
    String localTempDir = createLocalTempDir();
    String dockerAddress = "unix:///var/run/docker.sock";
    String dockerImage = "ubuntu:latest";
    DockerClient dockerClient = new DefaultDockerClient(dockerAddress);
    String localInput = FilenameUtils.concat(localTempDir, "file_on_host.txt");

    PrintStream stream = new PrintStream(localInput);
    stream.append("\nHello from the host machine.\n");
    stream.close();

    // Run a simple command in the container to demonstrate we can read the
    // file mounted from the host.
    ArrayList<String> command = new ArrayList<String>();
    command.add("cat");
    command.add("/mounted/file_on_host.txt");

    DockerProcessBuilder builder = new DockerProcessBuilder(
        command, dockerClient);
    builder.addVolumeMapping(localTempDir, "/mounted");
    builder.setImage(dockerImage);

    // Start and run the container.
    builder.start();
  }
}
