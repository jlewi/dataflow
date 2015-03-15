package sessions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import dataflow.AvroFileUtil;
import dataflow.UnionExample.Options;

/**
 * A simple example to illustrate a sliding window.
 *
 * All this pipeline does is take a time series and apply a sliding window. For each window
 * we woutput the points in the window.
 */
public class SlidingWindowExample {
  @DefaultCoder(AvroCoder.class)
  static class Point {
    long timeStamp;

    public Point() {
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class WindowPoints implements Comparable<WindowPoints> {
    ArrayList<Long> timeStamps;

    long maxTimeStamp;

    public WindowPoints() {
    }

    @Override
    public int compareTo(WindowPoints other) {
      if (maxTimeStamp < other.maxTimeStamp) {
        return -1;
      } else if(maxTimeStamp > other.maxTimeStamp) {
        return 1;
      }
      return 0;
    }
  }


  // Assign the same key to each data point so we can apply a group by.
  public static class KeyById extends DoFn<Point, KV<String, Point>> {
    @Override
    public void processElement(
        DoFn<Point, KV<String, Point>>.ProcessContext c)
            throws Exception {
      Point point = c.element();
      Instant instant = new Instant(point.timeStamp);
      c.outputWithTimestamp(KV.of("", point), instant);
    }
  }

  public static class BuildWindows extends DoFn<KV<String, Iterable<Point>>, WindowPoints> {
    @Override
    public void processElement(
        DoFn<KV<String, Iterable<Point>>, WindowPoints>.ProcessContext c)
            throws Exception {
      WindowPoints window = new WindowPoints();
      window.timeStamps = new ArrayList<Long>();
      for (Point p : c.element().getValue()) {
        window.timeStamps.add(p.timeStamp);
        Instant instant = new Instant(p.timeStamp);
      }

      BoundedWindow boundedWindow = c.windows().iterator().next();
      window.maxTimeStamp = boundedWindow.maxTimestamp().getMillis();
      c.output(window);
    }
  }

  public static class ComputeWindows extends
  PTransform<PCollection<Point>, PCollection<WindowPoints>> {
    private Duration duration;
    private Duration period;

    public ComputeWindows(Duration duration, Duration period) {
      this.duration = duration;
      this.period = period;
    }

    @Override
    public PCollection<WindowPoints> apply(PCollection<Point> points) {
      PCollection<KV<String, Iterable<Point>>> windowEntries =
          points
          .apply(ParDo.of(new KeyById()))
          .apply(Window.<KV<String, Point>>into(SlidingWindows.of(duration).every(period)))
          .apply(GroupByKey.<String, Point>create());

      PCollection<WindowPoints> windows = windowEntries.apply(ParDo.of(new BuildWindows()));

      return windows;
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);


    Duration windowDuration = Duration.standardHours(2);

    DateTime startTime = new DateTime("2015-03-14T00:00:00-0800");
    ArrayList<Point> points = new ArrayList<Point>();
    for (int i = 0; i < 3; ++i) {
      Point point = new Point();
      DateTime timeStamp = startTime.plusHours(i);
      point.timeStamp = timeStamp.getMillis();

      points.add(point);
    }

    PCollection<Point> inputs = p.apply(Create.of(points));

    PCollection<WindowPoints> outputs = inputs.apply(
        new ComputeWindows(
            windowDuration, Duration.standardHours(1)));

    outputs.apply(AvroIO.Write.to("/tmp/points").withSchema(WindowPoints.class));
    p.run();

    // TODO(jeremy@lewi.us): Need to make sure we are using a blocking runner.
    FileInputStream inStream = null;
    try {
      inStream = new FileInputStream("/tmp/points-00000-of-00001");
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    ArrayList<WindowPoints> windows = AvroFileUtil.readRecordsReflect(inStream, WindowPoints.class);
    try {
      inStream.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    Collections.sort(windows);
    for (WindowPoints window : windows) {
      ArrayList<String> times = new ArrayList<String>();
      Collections.sort(window.timeStamps);
      for (Long t : window.timeStamps) {
        Instant instant = new Instant(t);
        times.add(instant.toString());
      }

      Instant windowEndInstant = new Instant(window.maxTimeStamp);
      String windowStartTime = windowEndInstant.minus(windowDuration).toString();
      String windowEndTime = windowEndInstant.toString();

      String line = String.format("[%s, %s):", windowStartTime, windowEndTime);
      line += " ";
      line += StringUtils.join(times, ",");

      System.out.println(line);
    }
  }
}
