package geohash.utils;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

public class StopWatchOwn {

  public static Stopwatch stopwatch = Stopwatch.createStarted();

  public static void start(String message) {
    System.out.println(message);
    stopwatch = Stopwatch.createStarted();
  }

  public static void stop(String message) {
    System.out.println(message);
    stopwatch.stop();
    System.out.println("Generate time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
    System.out.println("---------------------------------");
  }
}
