package geohash.utils;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static geohash.GeoHashApp.PREFIX;
import static geohash.utils.FileUtils.listFiles;
import static geohash.utils.FileUtils.walkFiles;
import static geohash.utils.GeoUtils.*;

public class ScoringUtils {

  public static long getTotalUsersSeenByWalk(WGS84Point poi, int characters, int radius) {
    GeoHash geoHash = GeoHash.withCharacterPrecision(poi.getLatitude(),
        poi.getLatitude(),
        characters);
    final List<GeoHash> hashes = findEntriesInCircle(geoHash, poi.getLatitude(),
        poi.getLongitude(),
        radius);
    hashes.add(geoHash);
    return hashes
        .parallelStream()
        .map(hash -> {
          try {
            return walkFiles(PREFIX + hash.toBase32());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .flatMap(e -> e.parallelStream())
        .flatMap(f -> readCoordinates(f).parallelStream())
        .filter(geo ->
            distance(poi.getLatitude(),
                poi.getLongitude(),
                Double.valueOf(geo[0]),
                Double.valueOf(geo[1])) <= radius
        )
        .count();
  }

  public static long getTotalUsersSeenByRecursion(WGS84Point poi, int characters, int radius) {
    GeoHash geoHash = GeoHash.withCharacterPrecision(poi.getLatitude(),
        poi.getLatitude(),
        characters);
    final List<GeoHash> hashes = findEntriesInCircle(geoHash, poi.getLatitude(),
        poi.getLongitude(),
        radius);
    hashes.add(geoHash);
    return hashes
        .parallelStream()
        .map(hash -> listFiles(new File(PREFIX + hash.toBase32())))
        .flatMap(e -> e.parallelStream())
        .flatMap(f -> readCoordinates(f.toPath()).parallelStream())
        .filter(geo ->
            distance(poi.getLatitude(),
                poi.getLongitude(),
                Double.valueOf(geo[0]),
                Double.valueOf(geo[1])) <= radius
        )
        .count();
  }
}
