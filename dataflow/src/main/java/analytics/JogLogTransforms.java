package analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

/**
 * Transforms and DoFns for working with JobLogMessages.
 */
public class JogLogTransforms {

  /**
   * DoFn to parse the json representation of a job logmessage into the
   * JobLogMessage class.
   */
  public static class ParsJsonDoFn extends DoFn<String, JobLogMessage> {

    @Override
    public void processElement(DoFn<String, JobLogMessage>.ProcessContext c)
        throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(c.element());

      JobLogMessage log = new JobLogMessage();
      JsonNode jobId = node.get("job_id");
      if (jobId != null) {
        log.jobId = jobId.asText();
      }

      JsonNode project = node.get("project_id");
      if (project != null) {
        log.projectId = project.asText();
      }

      JsonNode jobStatus = node.get("job_status");
      if (jobStatus != null) {
        log.jobStatus = jobStatus.asText();
      }

      JsonNode workflowStatus = node.get("workflow_status");
      if (jobStatus != null) {
        log.workflowStatus = workflowStatus.asText();
      }

      JsonNode timestampMs = node.get("timestamp_ms");
      if (timestampMs != null) {
        log.timestampMs = timestampMs.asLong();
      }

      c.output(log);
    }
  }
}
