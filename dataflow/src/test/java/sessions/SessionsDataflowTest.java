package sessions;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import sessions.SessionsDataflow.BuildSession;
import sessions.SessionsDataflow.Session;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class SessionsDataflowTest {
  @Test
  public void testBuildSessions() {
    BuildSession doFn = new BuildSession();

    DoFnTester<KV<String, Iterable<LogEntry>>, Session> fnTester = DoFnTester.of(doFn);

    ArrayList<LogEntry> entries = new ArrayList<LogEntry>();
    LogEntry entry = new LogEntry();
    entry.sessionId = "session";
    entry.sessionStart = true;
    entry.status = "start";
    entry.timeStamp = 0;
    entries.add(entry);

    entry = new LogEntry();
    entry.sessionId = "session";
    entry.sessionStart = true;
    entry.status = "running";
    entry.timeStamp = 1;
    entries.add(entry);

    entry = new LogEntry();
    entry.sessionId = "session";
    entry.sessionStart = true;
    entry.status = "done";
    entry.timeStamp = 2;
    entries.add(entry);

    KV<String, Iterable<LogEntry>> input = KV.of("session", (Iterable<LogEntry>)entries);

    List<Session> output = fnTester.processBatch(input);

    assertEquals(1, output.size());

    Session expectedSession = new Session();
    expectedSession.sessionId = "session";
    expectedSession.startTime = 0;
    expectedSession.endTime = 2;
    expectedSession.status = "done";

    assertEquals(expectedSession, output.get(0));
  }

  public static class AddTimeStamp extends DoFn<LogEntry, LogEntry> {
    @Override
    public void processElement(DoFn<LogEntry, LogEntry>.ProcessContext c)
        throws Exception {
      LogEntry entry =  c.element();
      Instant instant = new Instant(entry.timeStamp);
      c.outputWithTimestamp(c.element(), instant);
    }
  }

  @Test
  public void testComputeSessions() throws Exception {
    // TODO(jeremy@lewi.us): We should really verify the output and not just check
    // that the transform runs.
    ArrayList<LogEntry> entries = new ArrayList<LogEntry>();

    DateTime startTime = new DateTime("2015-03-14T00:00:00-0800");

    int numSessions = 1;
    for (int sessionIndex = 0; sessionIndex < numSessions; sessionIndex++) {
      for (int i = 0; i < 3; ++i) {
        LogEntry entry = new LogEntry();
        if (i == 0) {
          entry.sessionStart = true;
        }
        entry.sessionId = String.format("Session%d", sessionIndex);
        DateTime timeStamp = startTime.plusHours(i * 36);
        entry.timeStamp = timeStamp.getMillis();

        entry.status = String.format("Status%d", i);

        entries.add(entry);
      }
    }

    Pipeline p = TestPipeline.create();
    PCollection<LogEntry> inputs =
        p.apply(Create.of(entries)).apply(ParDo.of(new AddTimeStamp()));

    // We use a really large period and duration so that we get a single window
    // with all the values. This is a hack to make computing the expected values easier.
    PCollection<Session> outputs = inputs.apply(
        new SessionsDataflow.ComputeSessions(
            Duration.standardHours(48), Duration.standardHours(1)));

    // Run the pipeline.
    p.run();
  }
}
