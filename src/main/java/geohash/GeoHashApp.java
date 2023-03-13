package geohash;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import geohash.utils.StopWatchOwn;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static geohash.utils.FileUtils.saveToFile;
import static geohash.utils.GeoUtils.generateRandomPoint;
import static geohash.utils.ScoringUtils.getTotalUsersSeenByRecursion;
import static geohash.utils.ScoringUtils.getTotalUsersSeenByWalk;

public class GeoHashApp {

  public static final String PREFIX = "/tmp/location/";
  public static final int characters = 7;
  static final int radiusAroundPoint = 60;
  static final int numberOfCoordinates = 200;
  static final int numberOfPois = numberOfCoordinates / 10;

  public static void main(String[] args) {

    StopWatchOwn.start("Generating " + numberOfCoordinates + " location signals started.");
    AtomicInteger counter = new AtomicInteger(0);
    IntStream.range(0, numberOfCoordinates)
        .parallel()
        .forEach(i -> {
          if (counter.addAndGet(1) % 100_000 == 0) {
            System.out.println("Generated " + counter.get() + " location signals.");
          }
          WGS84Point point = generateRandomPoint();
          GeoHash geoHash = GeoHash.withCharacterPrecision(point.getLatitude(),
              point.getLatitude(), characters);
          saveToFile(point.getLatitude(), point.getLongitude(), geoHash.toBase32());
        });
    StopWatchOwn.stop("Generating location signals finished.");

    List pois = IntStream.range(0, numberOfPois)
        .parallel()
        .mapToObj(i -> generateRandomPoint())
        .collect(Collectors.toList());

    StopWatchOwn.start("Scoring users seen around " + numberOfPois + " POI by walk");
    long usersExposed1 = pois.parallelStream()
        .mapToLong(poi -> getTotalUsersSeenByWalk((WGS84Point) poi, characters, radiusAroundPoint))
        .filter(e -> e > 0)
        .reduce(0, (a, b) -> a + b);
    StopWatchOwn.stop("Total amount of users exposed by walk: " + usersExposed1);

    StopWatchOwn.start("Scoring users seen around " + numberOfPois + " POI by recursion");
    long usersExposed2 = pois.parallelStream()
        .mapToLong(poi -> getTotalUsersSeenByRecursion((WGS84Point) poi, characters, radiusAroundPoint))
        .filter(e -> e > 0)
        .reduce(0, (a, b) -> a + b);
    StopWatchOwn.stop("Total amount of users exposed by recursion: " + usersExposed2);
  }
}
