package xdb;

import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import org.jetbrains.annotations.NotNull;
import jetbrains.exodus.management.*;
import jetbrains.exodus.bindings.LongBinding;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.bindings.LongBinding.entryToLong;
import static jetbrains.exodus.bindings.LongBinding.longToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES;
import jetbrains.exodus.util.LightOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StressTest9 {
  private static Logger log = LogManager.getLogger(StressTest9.class);

  public static class Event {
    long key1;
    long key2;
    double val;

    public static ByteIterable get(long p1, long p2) {
      final LightOutputStream output = new LightOutputStream();
      output.writeUnsignedLong(p1);
      output.writeUnsignedLong(p2);
      return output.asArrayByteIterable();
    }

    public static Event getEvent(ByteIterable kbytes, ByteIterable vbytes) {
      final ByteIterator iterator = kbytes.iterator();
      long p1 = LongBinding.entryToUnsignedLong(iterator, 8);
      iterator.skip(8);
      long p2 = LongBinding.entryToUnsignedLong(iterator, 8);
      long v = entryToLong(vbytes);
      return new Event(p1, p2, Double.longBitsToDouble(v));
    }

    public Event(long key1, long key2, double val) {
      this.key1 = key1;
      this.key2 = key2;
      this.val = val;
    }

    public void set(long p1, long p2, double val) {
      key1 = p1;
      key2 = p2;
      this.val = val;
    }

    public String toString() {
      return key1 + ":" + key2 + "; val=" + val;
    }

    public ByteIterable getKey() {
      return get(key1, key2);
    }

    public ByteIterable getValue() {
      return longToEntry(Double.doubleToLongBits(val));
    }

  }

  public static void main( String[] args ) throws Exception {
    EnvironmentConfig config = new EnvironmentConfig();
    config.setGcUseExclusiveTransaction(false);
    config.setTreeMaxPageSize(960);
    config.setGcTransactionAcquireTimeout(10);
    config.setGcMinUtilization(60);
    config.setGcStartIn(300);
    config.setGcRunPeriod(1000);
    config.setGcFileMinAge(10000);
    config.setGcTransactionAcquireTimeout(100);
    config.setManagementEnabled(false);
    config.setEnvGatherStatistics(false);
    config.setEnvCloseForcedly(true);
    config.setLogCacheOpenFilesCount(10000);
    config.setTreeNodesCacheSize(1024*1024*1024);
    config.setTreeNodesCacheSize(1024*1024*1024);
    config.setLogFileSize(81920);
    final Environment env = Environments.newInstance("guids", config);
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("stressdb", WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
          //return env.openStore("stressdb", WITHOUT_DUPLICATES, txn);
        }
      });
    //env.suspendGC();
    log.info("test start");
    int count = 100000; final int batch = 1000;
    final Event[] evts = new Event[batch];
    for(int i = 0; i < count; i++) {
      for (int j = 0; j < batch; j++) {
        UUID g = UUID.randomUUID();
        evts[j] = new Event(g.getLeastSignificantBits(), g.getMostSignificantBits(), j);
      }
      try {
        env.suspendGC();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
              for (int j = 0; j < batch; j++) {
                store.add(txn, evts[j].getKey(), evts[j].getValue());
              }
            }
          });
      } finally {
        env.resumeGC();
      }
      if(i%100 == 0) {
        log.info("remaining count {}", count-i);
      }

      if(i%10000 == 0) {
        log.info("starting gc");
        env.gc();
        log.info("gc done");
      }
    }
    log.info("test ends");

    for(String name :  env.getStatistics().getItemNames()) {
      StatisticsItem item = env.getStatistics().getStatisticsItem(name);
      log.info("{}={}",name, item.getTotal());
    }

    env.close();
  }

}
