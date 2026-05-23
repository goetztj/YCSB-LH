package site.ycsb.db;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.MutablePair;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A YCSB Client implementation that interacts directly with LakeVilla
 * using native serialization to match the C++ engine diagnostic loops.
 */
public class LakeVillaTrinoAPIClient
    extends DB {

  public static final String LAKEVILLA_CONFIG = "lakevilla.config_path";
  public static final String LAKEVILLA_TABLE_PATH = "lakevilla.table_path";
  public static final String LAKEVILLA_SINGLE_TXN = "lakevilla.single_txn";
  public static final String RESULT_FILE = "lakevilla.resultFile";

  public static final List<String> ATTRIBUTE_NAMES = Arrays.asList(
      "FIELD0", "FIELD1", "FIELD2", "FIELD3", "FIELD4",
      "FIELD5", "FIELD6", "FIELD7", "FIELD8", "FIELD9"
  );

  private site.ycsb.db.LakeVillaTransactionManager transactionManager;
  private int tableId;
  private boolean singleTxn;
  private String resultFile;
  private Schema arrowSchema;

  // Latency metrics tracking queues matching the C++ layer vectors
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> insertQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> updateQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> deleteQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> readQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> scanQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> initQueue;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> commitQueue;

  private ConcurrentLinkedQueue<MutablePair<Long, Long>> insertQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> updateQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> deleteQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> readQueueError;
  private ConcurrentLinkedQueue<MutablePair<Long, Long>> scanQueueError;

  private int redos;

  @Override
  public void init()
      throws DBException {
    Properties props = getProperties();

    String configPath = props.getProperty(LAKEVILLA_CONFIG, "/LakeVilla/lvconfig.conf");
    String tablePath = props.getProperty(LAKEVILLA_TABLE_PATH, "warehouse/wh/usertable");
    singleTxn = Boolean.parseBoolean(props.getProperty(LAKEVILLA_SINGLE_TXN, "false"));
    resultFile = props.getProperty(RESULT_FILE, "./lakevilla_result");

    redos = 0;
    boolean[] levels = {true, true, true};

    // Initialize the shared schema definition to match ycsbc::LHTransactionsDB layout
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("YCSB_KEY", FieldType.notNullable(new ArrowType.Utf8()), null));
    for (int i = 0; i < 10; i++) {
      fields.add(new Field("field" + i, FieldType.nullable(new ArrowType.Utf8()), null));
    }
    this.arrowSchema = new Schema(fields);

    // Initialize metric tracking queues
    insertQueue = new ConcurrentLinkedQueue<>();
    updateQueue = new ConcurrentLinkedQueue<>();
    deleteQueue = new ConcurrentLinkedQueue<>();
    readQueue = new ConcurrentLinkedQueue<>();
    scanQueue = new ConcurrentLinkedQueue<>();
    initQueue = new ConcurrentLinkedQueue<>();
    commitQueue = new ConcurrentLinkedQueue<>();

    insertQueueError = new ConcurrentLinkedQueue<>();
    updateQueueError = new ConcurrentLinkedQueue<>();
    deleteQueueError = new ConcurrentLinkedQueue<>();
    readQueueError = new ConcurrentLinkedQueue<>();
    scanQueueError = new ConcurrentLinkedQueue<>();

    System.out.println("Initializing LakeVilla Native Transaction Manager...");
    try {
      transactionManager = new LakeVillaTransactionManager(levels, tablePath, configPath, 0);
      tableId = transactionManager.openNewTable(tablePath);

      if (singleTxn) {
        System.out.println("Beginning persistent single-transaction context...");
        long txnStart = System.nanoTime();
        transactionManager.begin();
        long txnEnd = System.nanoTime();
        initQueue.add(new MutablePair<>(txnStart, txnEnd - txnStart));
      }
    } catch (Exception e) {
      throw new DBException("Failed to initialize LakeVilla native transaction manager.", e);
    }
  }

  @Override
  public void cleanup() {
    System.out.println("Cleaning up LakeVilla Client...");

    if (singleTxn && transactionManager != null) {
      System.out.println("Committing persistent transaction block...");
      long commitStart = System.nanoTime();
      transactionManager.commit(false);
      long commitEnd = System.nanoTime();
      commitQueue.add(new MutablePair<>(commitStart, commitEnd - commitStart));
    }

    if (transactionManager != null) {
      transactionManager.close();
    }

    // Call diagnostic print out utility to console
    printStats();

    // Flush execution tracking metrics output out to performance log file
    Random rand = new Random();
    String fullName = "";
    try {
      boolean newFile = false;
      while (!newFile) {
        fullName = resultFile + rand.nextInt() + ".txt";
        File file = new File(fullName);
        if (file.createNewFile()) {
          newFile = true;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    try (FileWriter writer = new FileWriter(fullName)) {
      writeMetricsToLog(writer, "inserts", insertQueue);
      writeMetricsToLog(writer, "updates", updateQueue);
      writeMetricsToLog(writer, "deletes", deleteQueue);
      writeMetricsToLog(writer, "reads", readQueue);
      writeMetricsToLog(writer, "scans", scanQueue);
      writeMetricsToLog(writer, "init", initQueue);
      writeMetricsToLog(writer, "commit", commitQueue);
      writeMetricsToLog(writer, "inserts-errors", insertQueueError);
      writeMetricsToLog(writer, "updates-errors", updateQueueError);
      writeMetricsToLog(writer, "delete-errors", deleteQueueError);
      writeMetricsToLog(writer, "read-errors", readQueueError);
      writeMetricsToLog(writer, "scan-errors", scanQueueError);

      writer.write("--------->>>>" + redos + "<<<<---------");
    } catch (IOException ex) {
      System.out.println("Error encountered flushing benchmark file output logs.");
      ex.printStackTrace();
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    long txnStart = 0, opStart = 0, opEnd = 0, txnEnd = 0;
    try {
      if (!singleTxn) {
        txnStart = System.nanoTime();
        transactionManager.begin();
        opStart = System.nanoTime();
        initQueue.add(new MutablePair<>(txnStart, opStart - txnStart));
      } else {
        opStart = System.nanoTime();
      }

      byte[] dataRaw = transactionManager.readTable(tableId, 1);
      opEnd = System.nanoTime();

      if (!singleTxn) {
        transactionManager.commit(true);
        txnEnd = System.nanoTime();
        commitQueue.add(new MutablePair<>(opEnd, txnEnd - opEnd));
      }

      readQueue.add(new MutablePair<>(opStart, opEnd - opStart));

      if (dataRaw == null) {
        return Status.NOT_FOUND;
      }

      if (fields == null || fields.isEmpty()) {
        result.put("FIELD0", new StringByteIterator("poc_payload_data"));
      } else {
        for (String field : fields) {
          result.put(field, new StringByteIterator("poc_payload_data"));
        }
      }
      return Status.OK;
    } catch (Exception e) {
      long errorEnd = System.nanoTime();
      readQueueError.add(new MutablePair<>(opStart, errorEnd - opStart));
      redos++;
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    long txnStart = 0, opStart = 0, opEnd = 0, txnEnd = 0;
    try {
      if (!singleTxn) {
        txnStart = System.nanoTime();
        transactionManager.begin();
        opStart = System.nanoTime();
        initQueue.add(new MutablePair<>(txnStart, opStart - txnStart));
      } else {
        opStart = System.nanoTime();
      }

      byte[] dataRaw = transactionManager.readTable(tableId, 1);
      opEnd = System.nanoTime();

      if (!singleTxn) {
        transactionManager.commit(true);
        txnEnd = System.nanoTime();
        commitQueue.add(new MutablePair<>(opEnd, txnEnd - opEnd));
      }

      scanQueue.add(new MutablePair<>(opStart, opEnd - opStart));

      if (dataRaw == null) {
        return Status.NOT_FOUND;
      }

      HashMap<String, ByteIterator> rowMap = new HashMap<>();
      rowMap.put("FIELD0", new StringByteIterator("poc_scan_payload"));
      result.add(rowMap);

      return Status.OK;
    } catch (Exception e) {
      long errorEnd = System.nanoTime();
      scanQueueError.add(new MutablePair<>(opStart, errorEnd - opStart));
      redos++;
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    long txnStart = 0, opStart = 0, opEnd = 0, txnEnd = 0;
    try {
      if (!singleTxn) {
        txnStart = System.nanoTime();
        transactionManager.begin();
        opStart = System.nanoTime();
        initQueue.add(new MutablePair<>(txnStart, opStart - txnStart));
      } else {
        opStart = System.nanoTime();
      }

      // Replaces old mock with actual structural arrow stream composition logic
      byte[] arrowPayloadBytes = arrowTableBuilder(key, values);
      boolean success = transactionManager.writeToTable(tableId, arrowPayloadBytes);
      opEnd = System.nanoTime();

      if (!singleTxn) {
        transactionManager.commit(false);
        txnEnd = System.nanoTime();
        commitQueue.add(new MutablePair<>(opEnd, txnEnd - opEnd));
      }

      updateQueue.add(new MutablePair<>(opStart, opEnd - opStart));
      return success ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      long errorEnd = System.nanoTime();
      updateQueueError.add(new MutablePair<>(opStart, errorEnd - opStart));
      redos++;
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    long txnStart = 0, opStart = 0, opEnd = 0, txnEnd = 0;
    try {
      if (!singleTxn) {
        txnStart = System.nanoTime();
        transactionManager.begin();
        opStart = System.nanoTime();
        initQueue.add(new MutablePair<>(txnStart, opStart - txnStart));
      } else {
        opStart = System.nanoTime();
      }

      byte[] arrowPayloadBytes = arrowTableBuilder(key, values);
      boolean success = transactionManager.writeToTable(tableId, arrowPayloadBytes);
      opEnd = System.nanoTime();

      if (!singleTxn) {
        transactionManager.commit(false);
        txnEnd = System.nanoTime();
        commitQueue.add(new MutablePair<>(opEnd, txnEnd - opEnd));
      }

      insertQueue.add(new MutablePair<>(opStart, opEnd - opStart));
      return success ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      long errorEnd = System.nanoTime();
      insertQueueError.add(new MutablePair<>(opStart, errorEnd - opStart));
      redos++;
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    long txnStart = 0, opStart = 0, opEnd = 0, txnEnd = 0;
    try {
      if (!singleTxn) {
        txnStart = System.nanoTime();
        transactionManager.begin();
        opStart = System.nanoTime();
        initQueue.add(new MutablePair<>(txnStart, opStart - txnStart));
      } else {
        opStart = System.nanoTime();
      }

      byte[] clearPayloadBytes = arrowTableBuilder(key, Collections.emptyMap());
      boolean success = transactionManager.writeToTable(tableId, clearPayloadBytes);
      opEnd = System.nanoTime();

      if (!singleTxn) {
        transactionManager.commit(false);
        txnEnd = System.nanoTime();
        commitQueue.add(new MutablePair<>(opEnd, txnEnd - opEnd));
      }

      deleteQueue.add(new MutablePair<>(opStart, opEnd - opStart));
      return success ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      long errorEnd = System.nanoTime();
      deleteQueueError.add(new MutablePair<>(opStart, errorEnd - opStart));
      redos++;
      return Status.ERROR;
    }
  }

  /**
   * Java Implementation of arrow_table_builder.
   * Compiles elements to a true Arrow IPC Byte Stream expected by writeToTableNative.
   */
  private byte[] arrowTableBuilder(String key, Map<String, ByteIterator> values) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);

    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(this.arrowSchema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

      root.allocateNew();

      // Set key column
      VarCharVector keyVector = (VarCharVector) root.getVector("YCSB_KEY");
      keyVector.setSafe(0, key.getBytes(StandardCharsets.UTF_8));

      // Populating field0 to field9 structures mirroring lowercase names in C++ schema setup
      for (int i = 0; i < 10; i++) {
        String val = stringMap.get("FIELD" + i); // YCSB produces uppercase attributes
        VarCharVector fieldVector = (VarCharVector) root.getVector("field" + i);
        if (val != null) {
          fieldVector.setSafe(0, val.getBytes(StandardCharsets.UTF_8));
        } else {
          fieldVector.setNull(0);
        }
      }

      root.setRowCount(1);
      writer.start();
      writer.writeBatch();
      writer.end();

      return out.toByteArray();
    } catch (Exception e) {
      System.err.println("Failed building sequential Arrow payload table serialization array stream.");
      e.printStackTrace();
      return new byte[0];
    }
  }

  /**
   * Java equivalent of print_stats() computing and displaying queue averages in milliseconds.
   */
  private void printStats() {
    printQueueSummary("read", readQueue);
    printQueueSummary("insert", insertQueue);
    printQueueSummary("update", updateQueue);
    printQueueSummary("init", initQueue);
    printQueueSummary("commit", commitQueue);
  }

  private void printQueueSummary(String label, ConcurrentLinkedQueue<MutablePair<Long, Long>> queue) {
    if (queue.isEmpty()) {
      return;
    }

    System.out.println("----- " + label + " -----");
    double sumMs = 0;
    int count = 0;

    for (MutablePair<Long, Long> entry : queue) {
      double ms = entry.right / 1_000_000.0; // Conversion from nanoseconds to milliseconds
      System.out.println(ms);
      sumMs += ms;
      count++;
    }

    System.out.println("----------------");
    System.out.println(label + " (avg): " + (sumMs / count));
    System.out.println("----------------");
  }

  private void writeMetricsToLog(FileWriter writer, String label, ConcurrentLinkedQueue<MutablePair<Long, Long>> queue)
      throws IOException {
    if (!queue.isEmpty()) {
      writer.write("---" + label + "---\n");
      for (MutablePair<Long, Long> elem : queue) {
        writer.write(elem.left + ", " + elem.right + "\n");
      }
      writer.write("-------------\n");
    }
  }
}