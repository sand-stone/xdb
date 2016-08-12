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

public class StressTest8 {
  private static Logger log = LogManager.getLogger(StressTest8.class);

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

  public static class WriteTask implements Runnable {
    Random rnd;
    double sum, avg, min, max, total;
    int p;
    int count;

    public WriteTask(int p, int count) {
      this.rnd = new Random();
      sum = 0;
      avg = 0;
      min = 10000;
      max = 0;
      total = 0;
      rnd.setSeed(Thread.currentThread().getId());
      this.count = count;
      this.p = p;
    }

    final int batch = 100000;
    final Event[] evts = new Event[batch];
    int pn = 0;

    private void write(Environment env, Store store) {
      for (int i = 0; i < batch; i++) {
        UUID g = UUID.randomUUID();
        evts[i] = new Event(g.getLeastSignificantBits(), g.getMostSignificantBits(), i);
      }
      long t1 = System.nanoTime();
      try {
        env.suspendGC();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
              for (int i = 0; i < batch; i++) {
                store.add(txn, evts[i].getKey(), evts[i].getValue());
              }
            }
          });
      } finally {
        env.resumeGC();
      }
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
      log.info("write partition {} count = {} time {} avg ={} min={} max={}", p, batch, d, avg, min, max);
    }

    private Environment getEnv() {
      EnvironmentConfig config = new EnvironmentConfig();
      config.setGcUseExclusiveTransaction(false);
      config.setTreeMaxPageSize(512);
      /*config.setGcTransactionAcquireTimeout(10);
      config.setGcMinUtilization(10);
      config.setGcStartIn(300);
      config.setGcRunPeriod(2000);
      config.setTreeMaxPageSize(512);
      config.setGcFileMinAge(1000);
      config.setGcTransactionAcquireTimeout(1000);*/
      return Environments.newInstance("guids#"+p+"#"+pn++, config);
    }

    private Store getStore(Environment env) {
      return env.computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return env.openStore("stressdb", WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
          }
        });
    }

    public void run() {
      Environment env = getEnv();
      Store store = getStore(env);
      while(!stop) {
        if(count<=0)
          break;
        if(count%1000000 == 0) {
          if(env != null)
            env.close();
          log.info("thread {} switch to a new shard", p);
          env = getEnv();
          store = getStore(env);
        }
        write(env, store);
        counter.addAndGet(batch);
        count -= batch;
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
    int count = 1000000000;
    int cw = 10; int rw = 0;
    Thread[] workers = new Thread[cw+rw];
    for(int i = 0; i< cw; i++) {
      workers[i] = new Thread(new WriteTask(i, count/cw));
      workers[i].start();
    }

    for(int i = cw; i< cw + rw; i++) {
      workers[i] = new Thread(new ReadTask(null, null));
      workers[i].start();
    }

    while(true) {
      if(counter.get() >= count)
        break;
      try {Thread.currentThread().sleep(5000);} catch(Exception ex) {}
      log.info("evts processed {}", counter.get());
    }

    stop = true;

    for(int i=0; i< workers.length; i++) {
      workers[i].join();
    }

  }

}
