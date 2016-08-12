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

public class StressTest7 {
  private static Logger log = LogManager.getLogger(StressTest7.class);

  public static class Event {
    long key1;
    long key2;
    double val;

    public static ByteIterable get(long p1, long p2) {
      final LightOutputStream output = new LightOutputStream();
      LongBinding.writeCompressed(output, p1);
      LongBinding.writeCompressed(output, p2);
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

  public static class WriteTask implements Runnable {
    Environment[] envs;
    Store[] stores;
    Random rnd;
    double sum, avg, min, max, total;
    int p;
    int count;

    public WriteTask(Environment[] envs, Store[] stores, int count) {
      this.envs = envs;
      this.stores = stores;
      this.rnd = new Random();
      sum = 0;
      avg = 0;
      min = 10000;
      max = 0;
      total = 0;
      rnd.setSeed(Thread.currentThread().getId());
      p = rnd.nextInt(envs.length);
      this.count = count;
    }

    private void write(Environment env, Store store, Event[] batch) {
      long t1 = System.nanoTime();
      env.executeInTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            for (int i = 0; i < batch.length; i++) {
              store.add(txn, batch[i].getKey(), batch[i].getValue());
            }
          }
        });
      long t2 = System.nanoTime();
      double d = (t2-t1)/1e9;
      sum += d;
      total += count;
      avg = sum/(total);
      d /= (count==0?1 : count);
      if(d > max)
        max = d;
      if(d < min)
        min = d;
      log.info("write partition {} count = {} time {} avg ={} min={} max={}", p, batch.length, d, avg, min, max);
    }

    public void run() {
      Event[] batch = new Event[100000];
      for(int i = 0; i < batch.length; i++)
        batch[i] = new Event(0, 0, 0);
      while(!stop) {
        if(count<=0)
          break;
        int b = batch.length;
        while(--b >= 0) {
          UUID g = UUID.randomUUID();
          batch[b].set(g.getLeastSignificantBits(), g.getMostSignificantBits(), b);
        }
        p = ++p%envs.length;
        write(envs[p], stores[p], batch);
        counter.addAndGet(batch.length);
        count -= batch.length;
      }
    }

  }

  public static class ReadTask implements Runnable {
    final Environment[] envs;
    final Store[] stores;
    Random rnd;
    public ReadTask(Environment[] envs, Store[] stores) {
      this.envs = envs;
      this.stores = stores;
      rnd = new Random();
    }

    private int read(Environment env, Store store) {
      final int[] count = new int[1];
      env.executeInReadonlyTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            try (Cursor cursor = store.openCursor(txn)) {
              while (cursor.getNext()) {
                ByteIterable key = cursor.getKey();
                ByteIterable value = cursor.getValue();
                count[0]++;
              }
            }
          }
        });
      return count[0];
    }

    public void run() {
      while(!stop) {
        try {Thread.currentThread().sleep(rnd.nextInt(15000));} catch(Exception e) {}
        int n = rnd.nextInt(envs.length);
        long t1 = System.nanoTime();
        int c = read(envs[n], stores[n]);
        long t2 = System.nanoTime();
        double d = (t2-t1)/1e9;
        log.info("scan partition {} rows {} tx = {}", n, c, d);

      }
    }

  }

  static AtomicInteger counter = new AtomicInteger(0);
  private static boolean stop = false;

  public static void main( String[] args ) throws Exception {
    EnvironmentConfig config = new EnvironmentConfig();
    config.setGcStartIn(300);
    config.setGcRunPeriod(1000);
    config.setGcUseExclusiveTransaction(false);
    config.setGcTransactionAcquireTimeout(100);
    config.setGcRunPeriod(300);
    config.setLogFileSize(512);
    config.setGcStartIn(300);
    config.setGcRunPeriod(30000);
     config.setGcUseExclusiveTransaction(false);
    config.setManagementEnabled(false);
    config.setEnvGatherStatistics(false);
    config.setTreeMaxPageSize(512);
    config.setGcFileMinAge(1000);
    config.setGcTransactionAcquireTimeout(100);
    config.setLogFileSize(1024);

    /*config.setLogDurableWrite(true);
      config.setLogFileSize(81920);
      config.setGcMinUtilization(80);
      config.setGcStartIn(10000);
      config.setTreeMaxPageSize(512);
      config.setTreeNodesCacheSize(8192);*/
    final Environment env = Environments.newInstance("guids", config);
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          //return envs[e[0]].openStore("stressdb", WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
          return env.openStore("stressdb", WITHOUT_DUPLICATES, txn);
        }
      });

    int count = 50000; final int batch = 1000;
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
      if(i%100==0)
        log.info("count {}", i);
    }

    for(String name :  env.getStatistics().getItemNames()) {
      StatisticsItem item = env.getStatistics().getStatisticsItem(name);
      log.info("{}={}",name, item.getTotal());
    }
    env.close();
  }

}
