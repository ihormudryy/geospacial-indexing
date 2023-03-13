package geohash.utils;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

import static geohash.GeoHashApp.PREFIX;
import static geohash.GeoHashApp.characters;
import static geohash.utils.FileUtils.saveToFile;

public class ParqueteSignal {

  private static void printGroup(Group g) {
    long timestamp = g.getLong("utc_timestamp", 0);
    String uuid = g.getString("device_id", 0);
    Group geo_location = g.getGroup("geo_location", 0);
    double lat = geo_location.getDouble("latitude", 0);
    double lon = geo_location.getDouble("longitude", 0);
    WGS84Point point = new WGS84Point(lat, lon);
    GeoHash geoHash = GeoHash.withCharacterPrecision(point.getLatitude(),
        point.getLatitude(), characters);
    saveToFile(point.getLatitude(), point.getLongitude(),
        PREFIX + "tiles/" + geoHash.toBase32(),
        uuid,
        timestamp / 1000);
  }

  public static void readParqueteSignals(String filename) throws IOException {
    String filePath = PREFIX + filename;
    Path path = new Path(filePath);
    var conf = new Configuration();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);
    PageReadStore pages = null;
    try {
      while (null != (pages = r.readNextRowGroup())) {
        final long rows = pages.getRowCount();
        System.out.println("Number of rows: " + rows);
        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
        for (int i = 0; i < rows; i++) {
          try {
            final Group g = (Group) recordReader.read();
            printGroup(g);
          } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
          }
        }
      }
    } finally {
      r.close();
    }
  }
}
