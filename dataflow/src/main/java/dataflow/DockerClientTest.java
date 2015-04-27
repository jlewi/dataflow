package dataflow;

import java.util.List;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ListContainersParam;
import com.spotify.docker.client.messages.Container;

/**
 * A simple program to test connecting to Docker using the spotify client.
 */
public class DockerClientTest {
  public static void main(String[] args) throws Exception {
    String dockerAddress = "unix:///var/run/docker.sock";
    DockerClient docker = new DefaultDockerClient(dockerAddress);

    ListContainersParam param;

    List<Container> containers = docker.listContainers(ListContainersParam.allContainers());
    for (Container c : containers) {
      System.out.println("Container c:" + c.id() + " image:" + c.image());
    }
  }
}
