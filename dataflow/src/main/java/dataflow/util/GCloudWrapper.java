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
package dataflow.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.util.IOUtils;
/**
 * A wrapper class for the GCloud tool.
 */
public class GCloudWrapper {
  private static final Logger logger =
      LoggerFactory.getLogger(GCloudWrapper.class.getName());

  private static final String DEFAULT_GCLOUD_BINARY = "gcloud";
  private final String binary;

  public GCloudWrapper() {
    this(DEFAULT_GCLOUD_BINARY);
  }

  /**
   * Path to the GCloud binary.
   */
  public GCloudWrapper(String binary) {
    this.binary = binary;
  }

  private String readStream(InputStream stream) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IOUtils.copy(stream, baos);
    return baos.toString("UTF-8");
  }

  /**
   * Return the token produced by the given command.
   * @param command
   * @return
   * @throws IOException
   */
  protected String getToken(String command) {
    TokenResponse response = new TokenResponse();

    ProcessBuilder builder = new ProcessBuilder();
    // ProcessBuilder will search the path automatically for the binary
    // GCLOUD_BINARY.
    builder.command(Arrays.asList(binary, "auth", command));
    Process process;
    try {
      process = builder.start();
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud.", e);
    }

    try {
      process.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud; timed out waiting " +
          "for gcloud.");
    }

    if (process.exitValue() != 0) {
      String output;
      try {
        output = readStream(process.getErrorStream());
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not obtain an access token using gcloud.", e);
      }

      throw new RuntimeException(
          "Could not obtain an access token using gcloud. Result of " +
          "invoking gcloud was:\n" + output);
    }

    String output;
    try {
      output = readStream(process.getInputStream());
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud. We encountered an " +
          "an error trying to read stdout.", e);
    }
    String[] lines = output.split("\n");

    if (lines.length != 1) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud. Result of " +
          "invoking gcloud was:\n" + output);
    }

    return output.trim();
  }

  public String getAccessToken() {
    return getToken("print-access-token");
  }

  public String getRefreshToken() {
    return getToken("print-refresh-token");
  }

  public void pullDockerImage(String image) {
    ProcessBuilder builder = new ProcessBuilder();
    // ProcessBuilder will search the path automatically for the binary
    // GCLOUD_BINARY.
    builder.command(Arrays.asList(binary, "preview", "docker", "pull", image));
    Process process;
    try {
      process = builder.start();
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not pull docker image using gcloud.", e);
    }

    try {
      process.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Could not pull the image; timed out waiting for gcloud.");
    }

    if (process.exitValue() != 0) {
      String output;
      try {
        output = readStream(process.getErrorStream());
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not pull the image using gcloud.", e);
      }

      throw new RuntimeException(
          "Could not pull the image using gcloud. Result of " +
              "invoking gcloud was:\n" + output);
    }

    try {
      String output = readStream(process.getInputStream());
      if (!output.isEmpty()) {
        logger.info(output);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not pull the image, We encountered an " +
              "an error trying to read stdout.", e);
    }

    try {
      String output = readStream(process.getErrorStream());
      if (!output.isEmpty()) {
        logger.error(output);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not pull the image, We encountered an " +
              "an error trying to read stderr.", e);
    }

  }
}
