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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleGuidTest {
  private static Logger log = LoggerFactory.getLogger(SimpleGuidTest.class);

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
      long p1 = LongBinding.readCompressed(iterator);
      long p2 = LongBinding.readCompressed(iterator);
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

  private static boolean stop = false;

  public static void main( String[] args ) throws Exception {
    final EnvironmentConfig config = new EnvironmentConfig().setLogFileSize(32 * 1024);
    //EnvironmentConfig config = new EnvironmentConfig();
    /*config.setGcStartIn(600000);
    config.setGcRunPeriod(3000);
    config.setGcUseExclusiveTransaction(false);
    config.setGcTransactionAcquireTimeout(1000);
    config.setGcRunPeriod(300000);*/
    final Environment env = Environments.newInstance("guids", config);
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("stressdb", WITHOUT_DUPLICATES, txn);
        }
      });

    int count = 2000; final int batch = 1000;
    final Event[] evts = new Event[batch];
    for(int i = 0; i < count; i++) {
      for (int j = 0; j < batch; j++) {
        UUID g = UUID.randomUUID();
        evts[j] = new Event(g.getLeastSignificantBits(), g.getMostSignificantBits(), j);
      }
      env.executeInTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            for (int j = 0; j < batch; j++) {
              store.add(txn, evts[j].getKey(), evts[j].getValue());
            }
          }
        });
      if(i%100 == 0) {
        System.out.println("remaining count "+(count-i));
      }
    }

    for(String name :  env.getStatistics().getItemNames()) {
      StatisticsItem item = env.getStatistics().getStatisticsItem(name);
      log.info("{}={}",name, item.getTotal());
    }
    env.close();
  }

}
