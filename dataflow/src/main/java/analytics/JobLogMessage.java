package analytics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class JobLogMessage {
  @Nullable
  public String projectId;

  @Nullable
  public String jobId;

  @Nullable
  public String jobName;

  @Nullable
  public String jobStatus;

  @Nullable
  public Long timestampMs;

  @Nullable
  public String workflowStatus;

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof JobLogMessage)) {
      return false;
    }

    JobLogMessage other = (JobLogMessage) obj;
    AvroCoder<JobLogMessage> coder = AvroCoder.of(JobLogMessage.class);

    Coder.Context context = new Coder.Context(true);
    ByteArrayOutputStream thisStream = new ByteArrayOutputStream();
    try {
      coder.encode(this, thisStream, context);
    } catch (IOException e) {
      throw new RuntimeException("There was a problem encoding the object.", e);
    }

    ByteArrayOutputStream otherStream = new ByteArrayOutputStream();
    try {
      coder.encode(other, otherStream, context);
    } catch (IOException e) {
      throw new RuntimeException("There was a problem encoding the object.", e);
    }

    byte[] theseBytes = thisStream.toByteArray();
    byte[] otherBytes = otherStream.toByteArray();

    if (theseBytes.length != otherBytes.length) {
      return false;
    }
    for (int i = 0; i < theseBytes.length; ++i) {
      if (theseBytes[i] != otherBytes[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    // Return the json representation of the class.
    try {
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      DatumWriter<JobLogMessage> writer = new ReflectDatumWriter<JobLogMessage>(JobLogMessage.class);

      Schema schema = ReflectData.get().getSchema(JobLogMessage.class);
      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          schema, generator);

      writer.write(this, encoder);
      encoder.flush();

      return outStream.toString();
    } catch(IOException e){
      throw new RuntimeException(
          "There was a problem writing the record. Exception: " + e.getMessage(), e);
    }
  }
}
