package site.ycsb.db;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper for the c++ LakeVilla lib.
 */
public class LakeVillaTransactionManager
    implements AutoCloseable {
  private final AtomicInteger nextTableId = new AtomicInteger(0);
  private long localHandle;

  public LakeVillaTransactionManager(boolean[] levels, String path, String config, int transactionId)
  {
    localHandle = createNative(levels, path, config, transactionId);
  }

  public boolean begin() {
    return beginTransaction(localHandle);
  }

  public void commit(boolean readOnly) {
    commitNative(localHandle, readOnly);
  }

  public byte[] readTable(int tableId, int numThreads) {
    return readTableAsBytes(localHandle, tableId);
  }

  public int openNewTable(String path) {
    int currentId = nextTableId.incrementAndGet();
    openTable(localHandle, path); // Pass the pointer handle here
    return currentId;
  }

  public boolean writeToTable(int tableId, byte[] arrowTable) {
    return writeToTableNative(localHandle, tableId, arrowTable);
  }

  @Override
  public void close() {
    if (localHandle != 0) {
      destroyNative(localHandle);
      localHandle = 0;
    }
  }

  // Native methods
  private native long createNative(boolean[] levels, String path, String configPath, int transactionId);

  private native void destroyNative(long handle);

  //private native long readTable(long handle, int tableId, int numThreads);

  private native byte[] readTableAsBytes(long handle, int tableId);

  private native boolean beginTransaction(long handle);

  private native boolean writeToTableNative(long handle, int tableId, byte[] arrowTable);

  private native void commitNative(long handle, boolean readOnly);

  private native void openTable(long handle, String path);

  static {
    LakeVillaLoader.loadLibrary();
  }
}
