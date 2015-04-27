package dataflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

/**
 * Pretty print avro files to json.
 *
 * TODO(jeremy@lewi.us):
 * 1. Allow the inputpath to specify a directory or glob path.
 * 2. Use the file URI to determine if its a local file.
 */
public class PrettyPrintAvro {
  private static final Logger sLogger = LoggerFactory.getLogger(
      PrettyPrintAvro.class);

  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to read. Can be a glob.")
    @Default.String("")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    String getOutput();
    void setOutput(String value);
  }

  // TODO(jeremy@lewi.us): Use AvroFileUtil.readFileToPrettyJson.
  /**
   * Read the specified file and pretty print it to outstream.
   * @param inputFile
   * @param outstream
   */
  private static void readFile(InputStream inStream, OutputStream outStream) {
    try {
      GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
      DataFileStream<Object> fileReader =
          new DataFileStream<Object>(inStream, reader);

      Schema schema = fileReader.getSchema();
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, generator);

      for (Object datum : fileReader) {
        writer.write(datum, encoder);
      }
      encoder.flush();

      outStream.flush();
      fileReader.close();
    } catch(IOException e){
      sLogger.error("IOException.", e);
    }
  }


  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    String input = options.getInput();
    if (input == null) {
      throw new IllegalArgumentException("Input can't be null.");
    }

    String output = options.getOutput();

    GcsUtil.GcsUtilFactory gcsUtilFactory = new GcsUtil.GcsUtilFactory();
    GcsUtil gcsUtil = gcsUtilFactory.create(options);

    List<GcsPath> matchingFiles = gcsUtil.expand(GcsPath.fromUri(input));
    try {
      WritableByteChannel channel = null;
      OutputStream outStream = System.out;
      if (output != null) {
        GcsPath gcsOutputPath = GcsPath.fromUri(output);
        channel = gcsUtil.create(gcsOutputPath, "application/json");
        outStream = Channels.newOutputStream(channel);
      }

      for (GcsPath inFile : matchingFiles) {
        sLogger.info("Dumping: " + inFile.toString());
        SeekableByteChannel inChannel = gcsUtil.open(inFile);
        InputStream inStream = Channels.newInputStream(inChannel);
        readFile(inStream, outStream);
        if (inChannel.isOpen()) {
          inChannel.close();
        }
      }
      if (output != null) {
        outStream.close();
      }
    } catch (IOException e) {
      sLogger.error("IOException", e);
    }

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    gcsOptions.getExecutorService().shutdown();
    try {
      gcsOptions.getExecutorService().awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      sLogger.error("Thread was interrupted waiting for execution service to shutdown.");
    }
  }
}
