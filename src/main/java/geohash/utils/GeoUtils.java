package geohash.utils;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GeoUtils {

  private static final double corridorLat = 90.0;
  private static final double corridorLon = 180.0;
  private static final Random rand = new Random();
  private static final double maxLat = 180.0;
  private static final double maxLon = 360.0;

  public static byte[] transformCoors(double[][] coordinates) {
    // Create a ByteBuffer to hold the binary data
    ByteBuffer buffer = ByteBuffer.allocate(coordinates.length * 2 * Double.BYTES);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Write each coordinate to the buffer as a pair of doubles
    for (int i = 0; i < coordinates.length; i++) {
      double lat = coordinates[i][0];
      double lon = coordinates[i][1];
      buffer.putDouble(lat);
      buffer.putDouble(lon);
    }

    return buffer.array();
  }

  public static List<double[]> parseCoordinates(byte[] input) {
    List<double[]> coordinates = new ArrayList<>();

    ByteBuffer buffer = ByteBuffer.wrap(input);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    while (buffer.remaining() >= 2 * Double.BYTES) {
      double lat = buffer.getDouble();
      double lon = buffer.getDouble();
      coordinates.add(new double[]{lat, lon});
    }

    return coordinates;
  }

  public static List<double[]> readCoordinates(Path path) {
    List<double[]> coordinates = new ArrayList<>();

    try {
      byte[] data = Files.readAllBytes(path);
      ByteBuffer buffer = ByteBuffer.wrap(data);
      buffer.order(ByteOrder.LITTLE_ENDIAN);

      while (buffer.remaining() >= 2 * Double.BYTES) {
        double lat = buffer.getDouble();
        double lon = buffer.getDouble();
        coordinates.add(new double[]{lat, lon});
      }
    } catch (IOException e) {
      System.err.println("Error reading coordinates from file: " + e.getMessage());
    }

    return coordinates;
  }

  // Find all quad tree entries inside a circle around a lat-lon using GeoHash
  public static List<GeoHash> findEntriesInCircle(GeoHash geoHash, double lat,
                                                  double lon, double radius) {
    List results = new ArrayList<>();
    GeoHash[] geoHashes = geoHash.getAdjacent();
    BoundingBox bbox = geoHash.getBoundingBox();

    WGS84Point northEast = bbox.getNorthEastCorner();
    WGS84Point southEast = bbox.getSouthEastCorner();
    WGS84Point northWest = bbox.getNorthWestCorner();
    WGS84Point southWest = bbox.getSouthWestCorner();
    WGS84Point altitudeToEast = new WGS84Point(northEast.getLatitude(), lon);
    WGS84Point altitudeToWest = new WGS84Point(southWest.getLatitude(), lon);
    WGS84Point altitudeToNorth = new WGS84Point(lat, northWest.getLongitude());
    WGS84Point altitudeToSouth = new WGS84Point(lat, southWest.getLongitude());

    if (distance(lat, lon, altitudeToNorth.getLatitude(), altitudeToNorth.getLongitude()) < radius) {
      results.add(geoHashes[0]);
    }

    if (distance(lat, lon, northEast.getLatitude(), northEast.getLongitude()) < radius) {
      results.add(geoHashes[1]);
    }

    if (distance(lat, lon, altitudeToEast.getLatitude(), altitudeToEast.getLongitude()) < radius) {
      results.add(geoHashes[2]);
    }

    if (distance(lat, lon, southEast.getLatitude(), southEast.getLongitude()) < radius) {
      results.add(geoHashes[3]);
    }

    if (distance(lat, lon, altitudeToSouth.getLatitude(), altitudeToSouth.getLongitude()) < radius) {
      results.add(geoHashes[4]);
    }

    if (distance(lat, lon, southWest.getLatitude(), southWest.getLongitude()) < radius) {
      results.add(geoHashes[5]);
    }

    if (distance(lat, lon, altitudeToWest.getLatitude(), altitudeToWest.getLongitude()) < radius) {
      results.add(geoHashes[6]);
    }

    if (distance(lat, lon, northWest.getLatitude(), northWest.getLongitude()) < radius) {
      results.add(geoHashes[7]);
    }

    return results;
  }

  public static double distance(WGS84Point from, WGS84Point to) {
    return distance(from.getLatitude(), from.getLongitude(), to.getLatitude(), to.getLongitude());
  }

  public static double distance(double lat1, double lon1, double lat2, double lon2) {
    final double R = 6371.0; // Radius of the earth in km
    double dLat = Math.toRadians(lat2 - lat1);
    double dLon = Math.toRadians(lon2 - lon1);
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    double distance = R * c * 1000; // Distance in m
    return distance;
  }

  public static WGS84Point middlePoint(WGS84Point from, WGS84Point to) {
    // Convert latitude and longitude to radians
    double lat1 = Math.toRadians(from.getLatitude());
    double lat2 = Math.toRadians(to.getLatitude());
    double lon1 = Math.toRadians(from.getLongitude());
    double lon2 = Math.toRadians(to.getLongitude());

    double dLon = Math.toRadians(lon2 - lon1);

    // Convert to radians
    lat1 = Math.toRadians(lat1);
    lat2 = Math.toRadians(lat2);
    lon1 = Math.toRadians(lon1);

    double Bx = Math.cos(lat2) * Math.cos(dLon);
    double By = Math.cos(lat2) * Math.sin(dLon);
    double lat3 = Math.atan2(Math.sin(lat1) + Math.sin(lat2), Math.sqrt((Math.cos(lat1) + Bx) * (Math.cos(lat1) + Bx) + By * By));

    // Corrected longitude calculation
    double lon3 = lon1 + Math.atan2(By, Math.cos(lat1) + Bx);
    return new WGS84Point(Math.toDegrees(lat3), Math.toDegrees(lon3));
  }

  public static WGS84Point generateRandomPoint() {
    double lat = rand.nextDouble() * maxLat - corridorLat;
    double lon = rand.nextDouble() * maxLon - corridorLon;
    return new WGS84Point(lat, lon);
  }
}
