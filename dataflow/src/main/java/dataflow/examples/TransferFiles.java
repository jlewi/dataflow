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
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;


/**
 * Copy files from an FTP site to GCS.
 */
public class TransferFiles {
  private static final Logger logger =
      LoggerFactory.getLogger(TransferFiles.class.getName());
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

    FTPClient ftp = new FTPClient();
    ftp.connect("ftp.ncbi.nlm.nih.gov");

    // After connection attempt, you should check the reply code to verify
    // success.
    int reply = ftp.getReplyCode();

    if(!FTPReply.isPositiveCompletion(reply)) {
      ftp.disconnect();
      logger.error("FTP server refused connection.");
      System.exit(-1);
    }

    ftp.login("anonymous", "someemail@gmail.com");

    FTPFile[] files = ftp.listFiles(
        "genomes/H_sapiens/Assembled_chromosomes/seq/");

    for (FTPFile f : files) {
      System.out.println("File: " + f.getName());
    }

    boolean success = ftp.setFileType(FTP.BINARY_FILE_TYPE);
    if (!success) {
      logger.error("Failed to set mode to binary.");
      System.exit(-1);
    }
    // FileOutputStream localOutStream = new FileOutputStream("/tmp/hs_ref_GRCh38.p2_unplaced.mfa.gz");
    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(options);
    GcsPath gcsPath = GcsPath.fromUri("gs://contrail_tmp/hs_ref_GRCh38.p2_unplaced.mfa.gz");
    String gcsType = "application/octet-stream";
    WritableByteChannel gcsChannel = gcsUtil.create(gcsPath, gcsType);

    OutputStream gcsStream = Channels.newOutputStream(gcsChannel);
    String remoteFile = "genomes/H_sapiens/Assembled_chromosomes/seq/hs_ref_GRCh38.p2_unplaced.mfa.gz";
    logger.info("Starting download of: {}", remoteFile);
    ftp.retrieveFile(remoteFile, gcsStream);
    logger.info("Completed download of: {}", remoteFile);

    gcsStream.close();
    ftp.logout();
  }
}
