package geohash;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static geohash.utils.GeoUtils.*;

public class SparkApp {

  static final int CHARACTERS = 7;
  static final int RADIUS_AROUND_POINT = 60;
  static final int NUMBER_OF_COORDINATES = 100;
  static final int NUMBER_OF_POIS = NUMBER_OF_COORDINATES / 10;
  static final String SPARK_MASTER = "spark://127.0.0.1:7077";
  static final String HADOOP_MASTER = "hdfs://127.0.0.1:9000";
  static final String APP_NAME = "SparkApp";
  static final String DESTINATION_FOLDER = "output/location/";

  public static void main(String[] args) throws IOException, URISyntaxException {
    SparkConf sparkConf = new SparkConf()
        .setAppName(APP_NAME)
        .setMaster("local[*]");
    SparkSession sparkSession =
        SparkSession.builder().config(sparkConf).getOrCreate();

    JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
    sparkContext.setLogLevel("ERROR");

    System.out.println("Generating location signals");
    long start = System.currentTimeMillis();

    // Generate and save the RDD of location signals
    List<WGS84Point> points = IntStream.range(0, NUMBER_OF_COORDINATES)
        .parallel()
        .mapToObj(i -> generateRandomPoint())
        .collect(Collectors.toList());

    JavaRDD<byte[]> signalsRDD = sparkContext.parallelize(points)
        .map(point -> transformCoors(new double[][]{{point.getLatitude(), point.getLongitude()}}))
        .repartition(10)
        .cache();

    signalsRDD.zipWithIndex()
        .mapToPair(Tuple2::swap)
        .saveAsObjectFile(DESTINATION_FOLDER);

    // Generate and save the RDD of POIs
    List<WGS84Point> pois = IntStream.range(0, NUMBER_OF_POIS)
        .parallel()
        .mapToObj(i -> generateRandomPoint())
        .collect(Collectors.toList());

    JavaRDD<WGS84Point> poisRDD = sparkContext.parallelize(pois).cache();

    // Calculate the number of location signals within each POI's circle of influence
    JavaRDD<Tuple2<WGS84Point, Long>> resultsRDD = poisRDD.map(poi -> {
      GeoHash geoHash = GeoHash.withCharacterPrecision(poi.getLatitude(), poi.getLongitude(), CHARACTERS);
      List<GeoHash> hashes = findEntriesInCircle(geoHash, poi.getLatitude(), poi.getLongitude(), RADIUS_AROUND_POINT);
      hashes.add(geoHash);

      Long count = 0L;
      for (GeoHash hash : hashes) {
        count += sparkContext.binaryFiles(DESTINATION_FOLDER + hash.toBase32() + "/*")
            .values()
            .map(bytes -> parseCoordinates(bytes.toArray()))
            .flatMap(List::iterator)
            .filter(coor -> distance(poi.getLatitude(),
                poi.getLongitude(), coor[0], coor[1]) <= RADIUS_AROUND_POINT)
            .count();
      }
      return new Tuple2<>(poi, count);
    });

    List<Tuple2<WGS84Point, Long>> results = resultsRDD.collect();
    for (Tuple2<WGS84Point, Long> r : results) {
      System.out.println("POI: " + r._1() + ", count: " + r._2());
    }
  }
}
