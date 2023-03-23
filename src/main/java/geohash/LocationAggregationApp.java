package geohash;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class LocationAggregationApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("GeoHashApp")
                .config("spark.master", "local[*]")
                .getOrCreate();

        // Define the input schema
        final var inputSchema = new org.apache.spark.sql.types.StructType()
                .add("utc_timestamp", DataTypes.LongType)
                .add("device_id", DataTypes.StringType)
                .add("group", new StructType()
                        .add("latitude", DataTypes.DoubleType)
                        .add("longitude", DataTypes.DoubleType));

        final var inputPrefix = "/tmp/signals/raw";
        final var outputPrefix = "/tmp/signals/tiles/";

        // Read the input Parquet files
        Dataset<Row> input = spark.read()
                .schema(inputSchema)
                .parquet(inputPrefix + "/*");

        // Define a UDF to calculate the geo hash ID
        spark.udf()
                .register("geoHash", (Double lat, Double lon) ->
                        GeoHash.withCharacterPrecision(lat, lon, 7)
                                .toBase32(), DataTypes.StringType);

        // Define a UDF to compute the GeoHash of a latitude and longitude pair
        spark.udf().register("geoHash", (Double lat, Double lon) ->
                GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 6), DataTypes.StringType);

        // Calculate the geo hash ID and group the data by day
        Dataset<Row> output = input.withColumn("geo_hash",
                        functions.callUDF("geoHash", input.col("group.latitude"), input.col("group.longitude")))
                .groupBy(functions.from_unixtime(
                                input.col("utc_timestamp").divide(1000L), "yyyy-MM-dd").alias("date"),
                        input.col("geo_hash"))
                .agg(functions.collect_list(functions.struct(
                                input.col("utc_timestamp"),
                                input.col("device_id"),
                                input.col("group.latitude"),
                                input.col("group.longitude")))
                        .alias("locations"));

        // Write the output data to the parquet files grouped by day
        output.foreach(row -> {
            String geoHash = row.getString(1);
            String date = row.getString(0);
            String year = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd")).getYear() + "";
            String month = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd")).getMonthValue() + "";
            String day = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd")).getDayOfMonth() + "";
            String uuid = UUID.randomUUID().toString();

            Dataset<Row> locations = (Dataset<Row>) row.getStruct(2);
            String outputDir = String.format(outputPrefix + "/%s/%s/%s/%s/%s.parquet", geoHash, year, month, day, uuid);
            locations.write().parquet(outputDir);
        });

        spark.stop();
    }
}