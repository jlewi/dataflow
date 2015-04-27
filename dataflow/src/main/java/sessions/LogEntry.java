package sessions;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;


@DefaultCoder(AvroCoder.class)
public class LogEntry {

  public String sessionId;
  public String status;

  // True indicates the start of a session.
  // Should be true for the first entry in a session.
  public boolean sessionStart;

  // time of the log entry.
  public long timeStamp;

  public LogEntry() {
    sessionStart = false;
  }
}