package sessions;

import org.joda.time.Duration;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * A Dataflow to create sessions of activity.
 */
public class SessionsDataflow {
  @DefaultCoder(AvroCoder.class)
  public static class Session {
    public String sessionId;
    public String status;
    // Start of the session time since the epoch.
    public long startTime;
    public long endTime;

    public Session() {
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Session)) {
        return false;
      }
      Session s = (Session) o;
      if (!sessionId.equals(s.sessionId)) {
        return false;
      }
      if (!status.equals(s.status)) {
        return false;
      }
      if (startTime != s.startTime) {
        return false;
      }
      if (endTime != s.endTime) {
        return false;
      }
      return true;
    }
  }

  public static class KeyBySessionId extends DoFn<LogEntry, KV<String, LogEntry>> {

    @Override
    public void processElement(
        DoFn<LogEntry, KV<String, LogEntry>>.ProcessContext c)
            throws Exception {
      LogEntry entry = c.element();
      c.output(KV.of(entry.sessionId, entry));
    }
  }

  public static class BuildSession extends DoFn<KV<String, Iterable<LogEntry>>, Session> {
    @Override
    public void processElement(
        DoFn<KV<String, Iterable<LogEntry>>, Session>.ProcessContext c)
            throws Exception {
      Session session = new Session();
      session.sessionId = c.element().getKey();

      session.startTime = Long.MAX_VALUE;
      session.endTime = Long.MIN_VALUE;

      boolean hasSessionStart = false;
      for (LogEntry entry : c.element().getValue()) {
        if (session.startTime > entry.timeStamp) {
          session.startTime = entry.timeStamp;
        }
        if (session.endTime < entry.timeStamp) {
          session.endTime = entry.timeStamp;
          session.status = entry.status;
        }

        hasSessionStart = hasSessionStart || entry.sessionStart;
      }

      if (!hasSessionStart) {
        // Since the window doesn't include the start of the session don't output
        // anything.
        return;
      }

      BoundedWindow window = c.window();
      System.out.println(String.format(
          "Max timestamp: %s", window.maxTimestamp().toString()));
      c.output(session);
    }
  }

  public static class ComputeSessions extends
  PTransform<PCollection<LogEntry>, PCollection<Session>> {
    private Duration duration;
    private Duration period;

    public ComputeSessions(Duration duration, Duration period) {
      this.duration = duration;
      this.period = period;
    }

    @Override
    public PCollection<Session> apply(PCollection<LogEntry> logEntries) {
      PCollection<KV<String, Iterable<LogEntry>>> sessionEntries =
          logEntries
          .apply(ParDo.of(new KeyBySessionId()))
          .apply(Window.<KV<String, LogEntry>>into(
              SlidingWindows.of(duration).every(period)))
              .apply(GroupByKey.<String, LogEntry>create());

      // TODO(jlewi): filter out sessions which don't include start of the session.
      PCollection<Session> sessions = sessionEntries.apply(ParDo.of(new BuildSession()));

      return sessions;
    }
  }
}
