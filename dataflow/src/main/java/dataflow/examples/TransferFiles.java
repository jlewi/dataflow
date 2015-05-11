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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;

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
    @Description("Path of the ftp files to match. e.g. ftp://com.ftp/dir/files*")
    String getInput();
    void setInput(String value);

    @Description("GCS path to write the files to.")
    String getOutput();
    void setOutput(String value);
  }

  @DefaultCoder(AvroCoder.class)
  public static class FilePair {
    public FilePair () {
      server = "";
      ftpPath = "";
      gcsPath = "";
    }
    public String server;
    public String ftpPath;
    public String gcsPath;
  }

  public static class FTPToGCS extends DoFn<FilePair, FilePair> {
    @Override
    public void processElement(DoFn<FilePair, FilePair>.ProcessContext c)
        throws Exception {
      FilePair pair = c.element();
      FTPClient ftp = new FTPClient();
      ftp.connect(pair.server);

      // After connection attempt, you should check the reply code to verify
      // success.
      int reply = ftp.getReplyCode();

      if(!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect();
        logger.error("FTP server refused connection.");
        throw new RuntimeException("FTP server refused connection.");
      }

      if (!ftp.login("anonymous", "someemail@gmail.com")) {
        throw new RuntimeException(
            "Failed to login. Reply: " + ftp.getReplyString());
      }

      boolean success = ftp.setFileType(FTP.BINARY_FILE_TYPE);
      if (!success) {
        logger.error("Failed to set mode to binary.");
        throw new RuntimeException("Failed to set mode to binary.");
      }

      ftp.enterLocalPassiveMode();

      GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(
          c.getPipelineOptions());
      GcsPath gcsPath = GcsPath.fromUri(pair.gcsPath);
      String gcsType = "application/octet-stream";
      WritableByteChannel gcsChannel = gcsUtil.create(gcsPath, gcsType);

      OutputStream gcsStream = Channels.newOutputStream(gcsChannel);
      logger.info("Starting download of: {}", pair.ftpPath);
      boolean result = ftp.retrieveFile(pair.ftpPath, gcsStream);
      if (result) {
        logger.info("Successfully completed download of: {}", pair.ftpPath);
      } else {
        logger.info("Failed to download of: {}. Reply was {}", pair.ftpPath,
            ftp.getReplyString());
      }

      gcsStream.close();
      ftp.logout();
    }
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    Options options = PipelineOptionsFactory.fromArgs(
        args).withValidation().as(Options.class);
    List<FilePair> filePairs = new ArrayList<FilePair>();

    URI ftpInput = new URI(options.getInput());

    FTPClient ftp = new FTPClient();
    ftp.connect(ftpInput.getHost());

    // After connection attempt, you should check the reply code to verify
    // success.
    int reply = ftp.getReplyCode();

    if(!FTPReply.isPositiveCompletion(reply)) {
      ftp.disconnect();
      logger.error("FTP server refused connection.");
      throw new RuntimeException("FTP server refused connection.");
    }

    ftp.login("anonymous", "someemail@gmail.com");

    String ftpPath = ftpInput.getPath();
    FTPFile[] files = ftp.listFiles(ftpPath);

    URI gcsUri = null;
    if (options.getOutput().endsWith("/")) {
      gcsUri = new URI(options.getOutput());
    } else {
      gcsUri = new URI(options.getOutput() + "/");
    }

    for (FTPFile f : files) {
      logger.info("File: " + f.getName());
      FilePair p = new FilePair();
      p.server = ftpInput.getHost();
      p.ftpPath = f.getName();

      // URI ftpURI = new URI("ftp", p.server, f.getName(), "");
      p.gcsPath = gcsUri.resolve(FilenameUtils.getName(f.getName())).toString();

      filePairs.add(p);
    }

    ftp.logout();

    Pipeline p = Pipeline.create(options);
    PCollection<FilePair> inputs = p.apply(Create.of(filePairs));
    inputs.apply(ParDo.of(new FTPToGCS()).named("CopyToGCS"))
      .apply(AvroIO.Write.withSchema(FilePair.class).to(options.getOutput()));
    p.run();
  }
}
