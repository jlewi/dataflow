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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

/**
 * A process running inside a docker container.
 */
// TODO(jlewi): Do we need to delete the docker container after the process
// finishes.
//
//
public class DockerProcess {
  private final Process process;
  private final String containerName;
  private final List<String> command;
  /**
   * Create a docker process.
   * @param name: Name of the docker container the process is running in.
   * @param process: Java process in which the docker command is running.
   */
  public DockerProcess(String containerName, Process process,
      List<String> command) {
    this.containerName = containerName;
    this.process = process;
    this.command = command;
  }

  /**
   * Wait for the process to finish and periodically grab the output.
   *
   * @param builder
   * @param prefix
   * @param command
   * @param logger
   * @param outStream
   * @return
   */
  public int waitForAndLogProcess(Logger logger, PrintStream outStream) {
    String commandText = StringUtils.join(command, " ");
    try{
      synchronized(process) {
        BufferedReader stdInput = new BufferedReader(
            new InputStreamReader(process.getInputStream()));

        BufferedReader stdError = new BufferedReader(
            new InputStreamReader(process.getErrorStream()));

        // In milliseconds.
        final long TIMEOUT = 1000;

        boolean wait = true;
        while (wait) {
          // We periodically check if the process has terminated and if not,
          // print out all the processes output
          process.wait(TIMEOUT);

          // Print all the output
          String line;

          while ((stdInput.ready()) && ((line = stdInput.readLine()) != null)) {
            if (outStream != null) {
              outStream.println(line);
            } else {
              logger.info(line);
            }
          }
          while ((stdError.ready()) && ((line = stdError.readLine()) != null)) {
            // TODO(jlewi): We should use logger.log and use function arguments
            // to specify what priority the output should be logged at.
            logger.error(line);
          }
          try {
            process.exitValue();
            // Process is done.
            wait = false;
          } catch (IllegalThreadStateException e) {
            // Process hasn't completed yet.
          }
        }

        logger.info("Exit Value: " + process.exitValue());
        if (process.exitValue() != 0) {
          logger.error(
              "Command: " + commandText + " exited with non-zero status.");
        }
      }
      return process.exitValue();
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem executing the command:\n" +
              commandText + "\n. The Exception was:\n" + e.getMessage());
    } catch (InterruptedException e) {
      throw new RuntimeException(
           "Execution was interupted. The command was:\n" +
              commandText + "\n. The Exception was:\n" + e.getMessage());
    }
  }

  /**
   * Remove the container and do other cleanup actions.
   */
  public void close() {
    // TODO(jlewi): Should we communicate with docker over the socket using
    // the docker API?
  }
}
