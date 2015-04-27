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
package dataflow.docker;

import org.apache.commons.io.FilenameUtils;

/**
 * Represent a path on the host that's mapped into a container.
 */
public class HostMountedPath {
  private String hostPath;
  private String containerPath;
  /**
   *
   * @param hostPath path on the host.
   * @param containerPath path inside the container.
   */
  public HostMountedPath(String hostPath, String containerPath) {
    this.hostPath = hostPath;
    this.containerPath = containerPath;
  }

  public String getHostPath() {
    return this.hostPath;
  }

  public String getContainerPath() {
    return this.containerPath;
  }

  /**
   * Return a new path by appending name to both the host and container paths.
   *
   * @param name
   * @return
   */
  public HostMountedPath append(String name) {
    return new HostMountedPath(
        FilenameUtils.concat(hostPath, name),
        FilenameUtils.concat(containerPath, name));
  }
}
