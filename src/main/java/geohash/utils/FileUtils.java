package geohash.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static geohash.GeoHashApp.PREFIX;
import static geohash.utils.GeoUtils.transformCoors;

public class FileUtils {
  private static final DateTimeFormatter YEAR_FORMAT = DateTimeFormatter.ofPattern("yy");
  private static final DateTimeFormatter MONTH_FORMAT = DateTimeFormatter.ofPattern("MM");
  private static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("dd");
  private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("HH");

  public static List<Path> walkFiles(String folder) throws IOException {
    if (!Files.isDirectory(Paths.get(folder))) {
      return new ArrayList<>();
    }

    List<Path> files = new ArrayList<>();
    try {
      Files.walk(Paths.get(folder), 3)
          .parallel()
          .filter(p -> Files.isRegularFile(p))
          .forEach(files::add);
    } catch (Exception e) {

    }
    return files;
  }

  public static List<File> listFiles(File folder) {
    if (folder == null) {
      return new ArrayList<>();
    }

    List<File> fileList = new ArrayList<>();
    File[] files = folder.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          fileList.addAll(listFiles(file)); // Recursion
        } else {
          fileList.add(file);
        }
      }
    }
    return fileList;
  }

  public static String compressFileName(UUID uuid) {
    byte[] bytes = toByteArray(uuid);
    String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    return encoded;
  }

  private static byte[] toByteArray(UUID uuid) {
    long mostSigBits = uuid.getMostSignificantBits();
    long leastSigBits = uuid.getLeastSignificantBits();
    byte[] buffer = new byte[16];
    for (int i = 0; i < 8; i++) {
      buffer[i] = (byte) (mostSigBits >>> 8 * (7 - i));
      buffer[8 + i] = (byte) (leastSigBits >>> 8 * (7 - i));
    }
    return buffer;
  }

  public static String generateFolderName(String root) {
    long minMillis = LocalDate.now().minusYears(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    long maxMillis = Instant.now().toEpochMilli();
    long randomMillis = ThreadLocalRandom.current().nextLong(minMillis, maxMillis);
    LocalDateTime randomTime = Instant.ofEpochMilli(randomMillis).atZone(ZoneOffset.UTC).toLocalDateTime();
    String year = YEAR_FORMAT.format(randomTime);
    String month = MONTH_FORMAT.format(randomTime);
    String day = DAY_FORMAT.format(randomTime);
    String hour = HOUR_FORMAT.format(randomTime);
    Path path = Paths.get(root, year, String.format("%s.%s.%s", month, day, hour));
    //Path path = Paths.get(root, year, month, day, hour);
    return path.toString();
  }

  public static String createFolder(String root) {
    Path path = Paths.get(generateFolderName(root));
    try {
      Files.createDirectories(path);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return path.toString();
  }

  public static List<double[]> readFile(String filePath) throws IOException {
    List<double[]> results = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] latLon = line.split(";");
        results.add(new double[]{
            Double.valueOf(latLon[0]),
            Double.valueOf(latLon[0])
        });
      }
    }
    return results;
  }

  public static void saveToFile(double lat, double lon, String hash) {
    final String fileName = compressFileName(UUID.randomUUID()) + ".bin";
    final String folderName = createFolder(PREFIX + hash);
    final String filePath = folderName + File.separator + fileName;
    final File folder = new File(folderName);
    if (!folder.exists()) {
      folder.mkdir();
    }
    try {
      Path path = Paths.get(filePath);
      Files.write(path, transformCoors(new double[][]{{lat, lon}}));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void saveToFile(double lat, double lon, String hash, String name, long timestamp) {

    LocalDateTime dateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);

    int year = dateTime.getYear();
    int month = dateTime.getMonthValue();
    int day = dateTime.getDayOfMonth();
    int hour = dateTime.getHour();

    Path path = Paths.get(hash, String.valueOf(year), String.format("%s.%s.%s", month, day, hour));
    final String fileName = name + ".bin";
    final String filePath = path + File.separator + fileName;
    final File folder = new File(path.toString());

    if (!folder.exists()) {
      try {
        Files.createDirectories(path);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try {
      Path pathFile = Paths.get(filePath);
      Files.write(pathFile, transformCoors(new double[][]{{lat, lon}}));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
