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

public class StressTest5 {
  private static Logger log = LogManager.getLogger(StressTest5.class);

  public static class Event {
    String key;
    long ts;
    double val;

    public static ByteIterable get(String key, long ts) {
      final LightOutputStream output = new LightOutputStream();
      output.writeString(key);
      LongBinding.writeCompressed(output, ts);
      return output.asArrayByteIterable();
    }

    public static Event getEvent(ByteIterable kbytes, ByteIterable vbytes) {
      final ByteIterator iterator = kbytes.iterator();
      String key = entryToString(kbytes);
      iterator.skip(key.length()+1);
      long ts = LongBinding.readCompressed(iterator);
      long v = entryToLong(vbytes);
      return new Event(key, ts, Double.longBitsToDouble(v));
    }

    public Event(String key, long ts, double val) {
      this.key = key;
      this.ts = ts;
      this.val = val;
    }

    public String toString() {
      return key + "ts= "+ts + "; val=" + val;
    }

    public ByteIterable getKey() {
      return get(key, ts);
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

    public WriteTask(Environment[] envs, Store[] stores) {
      this.envs = envs;
      this.stores = stores;
      this.rnd = new Random();
      sum = 0;
      avg = 0;
      min = 10000;
      max = 0;
      total = 0;
    }

    private void write(Environment env, Store store, Event[] batch, final int count) {
      long t1 = System.nanoTime();
      env.executeInTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            for (int i = 0; i < count; i++) {
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
      log.info("write tx = {} time {} avg ={} min={} max={}", count, d, avg, min, max);
    }

    public void run() {
      Event[] batch = new Event[100000];
      int n = 0;
      while(!stop) {
        if(evts.size()<=0)
          continue;
        int c = 0;
        while(c<batch.length&&evts.size()>0) {
          Event e = evts.poll();
          if(e==null)
            break;
          batch[c++] = e;
        }
        n = n++%envs.length;
        write(envs[n], stores[n], batch, c);
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
        try {Thread.currentThread().sleep(rnd.nextInt(500));} catch(Exception e) {}
        int n = rnd.nextInt(envs.length);
        long t1 = System.nanoTime();
        int c = read(envs[n], stores[n]);
        long t2 = System.nanoTime();
        double d = (t2-t1)/1e9;
        log.info("scan partition {} rows {} tx = {}", n, c, d);

      }
    }

  }

  public static class Producer implements Runnable {
    int count;

    public Producer(int count) {
      this.count = count;
    }

    public void run() {
      Random rnd = new Random();
      while(count-->0) {
        Event evt = new Event(metrics[rnd.nextInt(metrics.length)], System.nanoTime(), count);
        evts.offer(evt);
        if(count%10000000 == 0)
          log.info("evts needs to produced: {}", count);
        while(evts.size()>10000000) {
          try {Thread.currentThread().sleep(rnd.nextInt(1000));} catch(Exception e) {}
        }
      }
    }

  }

  private static LinkedBlockingQueue<Event> evts = new LinkedBlockingQueue<Event>();
  private static boolean stop = false;
  public static String[] metrics;

  static {
    metrics = new String[1000000];
    Random rnd = new Random();
    for(int i = 0; i < metrics.length; i++) {
      metrics[i] = "DC" + rnd.nextInt(200) + "#machine" + rnd.nextInt(100000) + "#metrics" + rnd.nextInt(1000);
    }
  }


  public static void main( String[] args ) throws Exception {
    int shards = 2048;
    final Environment[] envs = new Environment[shards];
    final Store[] stores = new Store[shards]; final int[] e = new int[1];
    EnvironmentConfig config = new EnvironmentConfig();
    /*config.setLogDurableWrite(true);
      config.setLogFileSize(81920);
      config.setGcMinUtilization(80);
      config.setGcStartIn(10000);
      config.setTreeMaxPageSize(512);
      config.setTreeNodesCacheSize(8192);*/
    for (int i = 0; i < shards; i++) {
      envs[i] = Environments.newInstance("data"+i, config);
      e[0] = i;
      stores[i] = envs[i].computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return envs[e[0]].openStore("stressdb", WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
            //return envs[e[0]].openStore("stressdb", WITHOUT_DUPLICATES, txn);
          }
        });
    }

    int cw = 20; int rw = 10;
    Thread[] workers = new Thread[cw+rw];
    for(int i = 0; i< cw; i++) {
      workers[i] = new Thread(new WriteTask(envs, stores));
      workers[i].start();
    }

    for(int i = cw; i< cw + rw; i++) {
      workers[i] = new Thread(new ReadTask(envs, stores));
      workers[i].start();
    }

    int count = 1000000000;
    int n = 2;
    Thread[] producers = new Thread[n];
    for(int i = 0; i < n; i++) {
      producers[i] = new Thread(new Producer(count/n));
      producers[i].start();
    }

    for(int i=0; i< n; i++) {
      producers[i].join();
    }

    while(true) {
      if (evts.size() <= 0)
        break;
      try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
      log.info("evts left to processed {}", evts.size());
    }

    stop = true;

    for(int i=0; i< workers.length; i++) {
      workers[i].join();
    }

    for (Environment env: envs) {
      for(String name :  env.getStatistics().getItemNames()) {
        StatisticsItem item = env.getStatistics().getStatisticsItem(name);
        log.info("{}={}",name, item.getTotal());
      }
      env.close();
    }
  }

}
