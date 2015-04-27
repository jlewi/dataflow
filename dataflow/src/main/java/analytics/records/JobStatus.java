package analytics.records;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class JobStatus {
  public Long timestampMs;
  public String status;
}
