package site.ycsb.db;

/**
 * A simple class loading the LakeVilla C++ lib.
 */
public final class LakeVillaLoader {
  private LakeVillaLoader()
  {}

  public static void loadLibrary() {
    //String os = System.getProperty("os.name").toLowerCase();
    //String arch = System.getProperty("os.arch").toLowerCase();
    String libPath = "/LakeVilla/build2/liblakevilla.so";

    /*if (os.contains("mac")) {
      if (arch.contains("x86_64")) {
        libPath = "plugin/trino-lakevilla/libs/macos/x86_64/liblakevilla.dylib";
      } else if (arch.contains("aarch64") || arch.contains("arm64")) {
        libPath = "plugin/trino-lakevilla/libs/macos/arm64/liblakevilla.dylib";
      }
    } else if (os.contains("linux")) {
      if (arch.contains("x86_64")) {
        libPath = "plugin/trino-lakevilla/libs/linux/x86_64/liblakevilla.so";
      } else if (arch.contains("aarch64") || arch.contains("arm64")) {
        libPath = "plugin/trino-lakevilla/libs/linux/arm64/liblakevilla.so";
      }
    } else {
      throw new UnsupportedOperationException("Unsupported OS/arch: " + os + " / " + arch);
    }*/

    System.load(libPath);
    System.out.println("Loaded native library: " + libPath);
  }
}

