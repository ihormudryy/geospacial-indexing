package geohash.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static geohash.GeoHashApp.PREFIX;
import static geohash.utils.FileUtils.createFolder;

public class AvroSignal {

  private static final String schemaString = "{ \"type\": \"record\", \"name\": \"Location\", " +
      "\"fields\": [ { \"name\": \"lat\", \"type\": \"double\" }, " +
      "{ \"name\": \"lon\", \"type\": \"double\" } ] }";

  private static final Schema schema = new Schema.Parser().parse(schemaString);
  private static final GenericRecord record = new GenericData.Record(schema);

  public static void saveToAvroFile(double lat, double lon, String hash) throws IOException {
    final String uuid = UUID.randomUUID().toString();
    final String folderName = createFolder(PREFIX + hash);
    final String filePath = folderName + File.separator + uuid;
    final File folder = new File(folderName);
    if (!folder.exists()) {
      folder.mkdir();
    }
    record.put("lat", lat);
    record.put("lon", lon);
    DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>());
    writer.create(schema, new File(filePath + ".avro"));
    writer.append(record);
    writer.close();
  }

  public static List<double[]> readFromAvroFile(String file) throws IOException {
    // Read the record from the Avro file
    List<double[]> results = new ArrayList<>();
    DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(
        new File(file), new GenericDatumReader<GenericRecord>());
    while (reader.hasNext()) {
      GenericRecord result = reader.next();
      results.add(new double[]{(double) result.get("lat"), (double) result.get("lon")});
    }
    reader.close();
    return results;
  }
}
