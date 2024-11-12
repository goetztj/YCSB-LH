package site.ycsb.db;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.spark.sql.*;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.spark.SparkConf;

/**
 * A class that allows YCSB to interact with Spark using Iceberg tables
 * This class extends {@link DB} and implements the database interface used by
 * YCSB client.
 */
public class SparkIcebergClient extends DB {

  public static final String LAKEHOUSE_FORMAT = "spark.lakehouse";

  public static final String SPARK_URL = "spark.url";

  public static final String METASTORE_URL = "spark.meta_url";

  public static final String NAMESPACE_NAME = "spark.namespace";

  public static final String RESULT_FILE = "spark.resultFile";

  public static final String OBJECT_STORE_URI = "spark.objectStore";

  public static final String OBJECT_STORE_USER = "spark.objectStoreUser";

  public static final String OBJECT_STORE_PWD = "spark.objectStorePwd";

  public static final String KEY_NAME = "YCSB_KEY";
  public static final List<String> ATTRIBUTE_NAMES = Arrays.asList("FIELD0", "FIELD1", "FIELD2", "FIELD3", "FIELD4",
      "FIELD5", "FIELD6", "FIELD7", "FIELD8", "FIELD9");

  private String lakehouse;

  private ConcurrentLinkedQueue<MutablePair<Long, Long>> insertQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> updateQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> deleteQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> readQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> scanQueue;

  private ConcurrentLinkedQueue<MutablePair<Long, Long>> insertQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> updateQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> deleteQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> readQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> scanQueueError;

  private String namespace;
  private org.apache.spark.sql.SparkSession session;

  private String resultFile;

  private String objectStoreUri;

  private String objectStoreUser;

  private String objectStorePwd;

  private int redos;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String surl = props.getProperty(SPARK_URL, "spark://localhost:7077");
    lakehouse = props.getProperty(LAKEHOUSE_FORMAT, "iceberg");
    namespace = props.getProperty(NAMESPACE_NAME, "spark_catalog.ycsb");
    String metastore = props.getProperty(METASTORE_URL, "thrift://localhost:9083");
    resultFile = props.getProperty(RESULT_FILE, "./result");

    objectStoreUri = props.getProperty(OBJECT_STORE_URI, "http://localhost:9000");
    objectStoreUser = props.getProperty(OBJECT_STORE_USER, "user");
    objectStorePwd = props.getProperty(OBJECT_STORE_PWD, "password");

    redos = 0;

    System.out.println("connecting to " + surl);

    SparkConf conf = new SparkConf();

    try {

      if (lakehouse.equals("iceberg")) {
        System.out.println("using iceberg");
        SparkSession.Builder builder = SparkSession.builder().appName("YCSB - Lakebench")
            .master(surl)
            .config("hive.metastore.uris", metastore).enableHiveSupport()
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.sql.extensions"
            , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.spark_catalog.uri", metastore)
            .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/wh/")
            .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://localhost:9000")
            .config("spark.sql.defaultCatalog", "spark_catalog")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.driver.memory", "16g")
            .config("spark.executor.memory", "8g")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "16g")
            .config("spark.executorEnv.SPARK_PUBLIC_DNS", "localhost")
            .config("spark.executorEnv.SPARK_LOCAL_IP", "localhost")
            .config("spark.files.useFetchCache", "false")
            .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
            .config("spark.sql.catalog.spark_catalog.cache.expiration-interval-ms", "0");

        session = builder.getOrCreate();
        System.out.println(session.catalog().currentCatalog());
      } else {
        System.out.println("using Delta Lake");
        SparkSession.Builder builder = SparkSession.builder().appName("YCSB - Lakebench")
            .master(surl).config("hive.metastore.uris", metastore).enableHiveSupport()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/wh/")
            .config("spark.sql.warehouse.dir", "s3a://warehouse/wh/")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "user")
            .config("spark.hadoop.fs.s3a.secret.key", "password")
            .config("spark.eventLog.dir", "/spark-events")
            .config("spark.history.fs.logDirectory", "/spark-events")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.eventLog.enabled", "true")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.defaultCatalog", "spark_catalog")
            .config("spark.driver.memory", "16g")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.executor.memory", "8g")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "16g")
            .config("spark.files.useFetchCache", "false")
            .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
            .config("spark.sql.catalog.spark_catalog.cache.expiration-interval-ms", "0")
            .config("spark.databricks.io.cache.enabled", "false");

        session = builder.getOrCreate();
      }

      System.out.println(">>> Spark Master at: " + session.sparkContext().master());

    } catch (Exception e) {
      throw new DBException("Could not create SparkSession object.", e);
    }

    insertQueue = new ConcurrentLinkedQueue<>();
    updateQueue = new ConcurrentLinkedQueue<>();
    deleteQueue = new ConcurrentLinkedQueue<>();
    readQueue = new ConcurrentLinkedQueue<>();
    scanQueue = new ConcurrentLinkedQueue<>();

    insertQueueError = new ConcurrentLinkedQueue<>();
    updateQueueError = new ConcurrentLinkedQueue<>();
    deleteQueueError = new ConcurrentLinkedQueue<>();
    readQueueError = new ConcurrentLinkedQueue<>();
    scanQueueError = new ConcurrentLinkedQueue<>();
  }

  /**
   * Shutdown the client.
   */
  @Override
  public void cleanup() {
    Random rand = new Random();
    String fullName = "";
    try {
      boolean newFile = false;
      while (!newFile) {
        fullName = resultFile + rand.nextInt() + ".txt";
        File file = new File(fullName);
        if (file.createNewFile()) {
          System.out.println("File created: " + file.getName());
          newFile = true;
        } else {
          System.out.println("File already exists.");
        }
      }
    } catch (IOException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
      return;
    }

    try {
      FileWriter writer = new FileWriter(fullName);

      if (!insertQueue.isEmpty()) {
        writer.write("---inserts---\n");
        for (MutablePair<Long, Long> elem : insertQueue) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!updateQueue.isEmpty()) {
        writer.write("---updates---\n");
        for (MutablePair<Long, Long> elem : updateQueue) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!deleteQueue.isEmpty()) {
        writer.write("---deletes---\n");
        for (MutablePair<Long, Long> elem : deleteQueue) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!readQueue.isEmpty()) {
        writer.write("---reads---\n");
        for (MutablePair<Long, Long> elem : readQueue) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!scanQueue.isEmpty()) {
        writer.write("---scans---\n");
        for (MutablePair<Long, Long> elem : scanQueue) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!insertQueueError.isEmpty()) {
        writer.write("---inserts-errors---\n");
        for (MutablePair<Long, Long> elem : insertQueueError) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!updateQueueError.isEmpty()) {
        writer.write("---updates-errors---\n");
        for (MutablePair<Long, Long> elem : updateQueueError) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!deleteQueueError.isEmpty()) {
        writer.write("---delete-errors---\n");
        for (MutablePair<Long, Long> elem : deleteQueueError) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!readQueueError.isEmpty()) {
        writer.write("---read-errors---\n");
        for (MutablePair<Long, Long> elem : readQueueError) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      if (!scanQueueError.isEmpty()) {
        writer.write("---scan-errors---\n");
        for (MutablePair<Long, Long> elem : scanQueueError) {
          writer.write(elem.left + ", " + elem.right + "\n");
        }
        writer.write("-------------\n");
      }

      writer.write("--------->>>>" + redos + "<<<<---------");

      writer.close();
    } catch (IOException ex) {
      System.out.println("error while writing");
      ex.printStackTrace();
    }

    session.close();
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {

    int reCounter = 0;
    while (reCounter < 10) {
      long startTime = System.nanoTime();
      try {
        Dataset<Row> data = session.table(formatKey(table)).where(KEY_NAME + "='" + key + "'");
        long endTime = System.nanoTime();

        readQueue.add(new MutablePair<>(startTime, endTime - startTime));

        if (data.isEmpty()) {
          return Status.NOT_FOUND;
        }

        if (null == fields || fields.isEmpty()) {
          for (String field : data.columns()) {
            Dataset<Row> dataField = data.select(field);
            java.util.List<Row> list = dataField.collectAsList();

            result.put(field, new StringByteIterator(list.toString()));
          }
        } else {
          int counter = 0;
          for (String field : fields) {
            Dataset<Row> dataField = data.select(ATTRIBUTE_NAMES.get(counter));
            java.util.List<Row> list = dataField.collectAsList();

            result.put(field, new StringByteIterator(list.toString()));
            counter++;
          }
        }

        return Status.OK;
      } catch (Exception e) {
        long endTime = System.nanoTime();

        readQueueError.add(new MutablePair<>(startTime, endTime - startTime));
        e.printStackTrace();
        redos++;
        reCounter++;
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {

    int reCounter = 0;
    while (reCounter < 10) {
      long startTime = System.nanoTime();
      try {
        Dataset<Row> data = session.table(formatKey(table)).where(KEY_NAME + ">='" + startkey + "'")
            .limit(recordcount);

        long endTime = System.nanoTime();

        scanQueue.add(new MutablePair<>(startTime, endTime - startTime));

        if (data.isEmpty()) {
          return Status.NOT_FOUND;
        }

        if (null == fields || fields.isEmpty()) {
          for (String field : data.columns()) {
            Dataset<Row> dataField = data.select(field);
            java.util.List<Row> list = dataField.collectAsList();

            HashMap<String, ByteIterator> rowMap = new HashMap();
            rowMap.put(field, new StringByteIterator(list.toString()));
            result.add(rowMap);
          }
        } else {
          int counter = 0;
          for (String field : fields) {
            Dataset<Row> dataField = data.select(ATTRIBUTE_NAMES.get(counter));
            java.util.List<Row> list = dataField.collectAsList();

            HashMap<String, ByteIterator> rowMap = new HashMap();

            rowMap.put(field, new StringByteIterator(list.toString()));
            result.add(rowMap);
            counter++;

          }

        }

        return Status.OK;

      } catch (Exception e) {
        long endTime = System.nanoTime();

        scanQueueError.add(new MutablePair<>(startTime, endTime - startTime));
        redos++;
        reCounter++;
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {

    StringBuilder query = new StringBuilder("UPDATE " + formatKey(table) + " SET ");

    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    boolean first = true;
    int counter = 0;
    for (String attrKey : stringMap.keySet()) {
      if (first) {
        query.append(ATTRIBUTE_NAMES.get(counter)).append(" = '").append(checkInput(stringMap.get(attrKey)))
            .append("'");
        first = false;
      } else {
        query.append(", ").append(ATTRIBUTE_NAMES.get(counter)).append(" = '")
            .append(checkInput(stringMap.get(attrKey)))
            .append("'");
      }

      counter++;
    }

    query.append(" WHERE YCSB_KEY = '");
    query.append(key);
    query.append("'");

    int reCounter = 0;
    while (reCounter < 10) {
      long startTime = System.nanoTime();
      try {
        session.sql(query.toString());
        long endTime = System.nanoTime();

        updateQueue.add(new MutablePair<>(startTime, endTime - startTime));

        return Status.OK;

      } catch (Exception e) {
        long endTime = System.nanoTime();

        updateQueueError.add(new MutablePair<>(startTime, endTime - startTime));
        redos++;
        reCounter++;
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    StringBuilder query = new StringBuilder("INSERT INTO " + formatKey(table) + " VALUES ('" + key);
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (String attrKey : stringMap.keySet()) {

      query.append("', '");
      query.append(checkInput(stringMap.get(attrKey)));
    }

    query.append("'");
    for (int i = 1 + stringMap.size(); i < 11; i++) {
      query.append(", NULL");
    }

    query.append(")");

    int reCounter = 0;
    while (reCounter < 10) {
      long startTime = System.nanoTime();
      try {
        session.sql(query.toString());
        long endTime = System.nanoTime();

        insertQueue.add(new MutablePair<>(startTime, endTime - startTime));

        return Status.OK;
      } catch (Exception e) {
        long endTime = System.nanoTime();

        insertQueueError.add(new MutablePair<>(startTime, endTime - startTime));
        redos++;
        reCounter++;
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(final String table, final String key) {
    StringBuilder query = new StringBuilder();
    query.append("DELETE FROM ");
    query.append(formatKey(table));
    query.append(" WHERE YCSB_KEY = '");
    query.append(key).append("'");

    int reCounter = 0;
    while (reCounter < 10) {
      long startTime = System.nanoTime();
      try {
        session.sql(query.toString());
        long endTime = System.nanoTime();

        deleteQueue.add(new MutablePair<>(startTime, endTime - startTime));
        return Status.OK;
      } catch (Exception e) {
        long endTime = System.nanoTime();

        deleteQueueError.add(new MutablePair<>(startTime, endTime - startTime));
        redos++;
        reCounter++;
      }
    }
    return Status.ERROR;

  }

  private String formatKey(final String table) {
    return namespace + "." + table;
  }

  private String checkInput(final String data) {
    return data.replace("'", "r");

  }
}
