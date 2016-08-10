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
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class StressTest3 {
  private static Logger log = LogManager.getLogger(StressTest3.class);

  
  public static class WriteTask implements Runnable {
    Environment env;
    Store store;
    Random rnd;
    
    public WriteTask(Environment env, Store store) {
      this.env = env;
      this.store = store;
      rnd = new Random();
    }

    public ByteIterable[] generateKeys(int count) {
      DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
      FORMAT.applyPattern("000000000000");
      ByteIterable[] keys = new ByteIterable[count];
      for (int i = 0; i < count; i++) {
        keys[i] = stringToEntry(FORMAT.format(rnd.nextInt(1000000000)));
      }
      return keys;
    }
    
    public void run() {
      double sum = 0; double max = 0; double min = 10;
      double avg = 0; int count = 1000000;
      for (int tx = 0; tx < 10; tx++) {
        ByteIterable[] data = generateKeys(count);
        long t1 = System.nanoTime();
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
              for (int i = 0; i < count; i++) {
                store.add(txn, data[i], data[i]);
              }
            }
          });
        long t2 = System.nanoTime();
        double d = (t2-t1)/1e9;
        sum += d;
        avg = sum/tx;
        if(d>max) max = d;
        if(d<min) min = d;
        log.info("write d = {} avg ={} min={} max={}", d, avg, min, max);
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

    public void run() {
      double sum = 0; double max = 0; double min = 10;
      double avg = 0;
      final long[] count = new long[1];
      for (int tx=0; tx < 100000; tx++) {
        try {Thread.currentThread().sleep(rnd.nextInt(500));} catch(Exception e) {}
        long t1 = System.nanoTime();
        count[0] = 0; final int[] s = new int[1];
        for (int c = 0; c < stores.length; c++) {
          s[0] = c;
          envs[c].executeInReadonlyTransaction(new TransactionalExecutable() {
              @Override
              public void execute(@NotNull final Transaction txn) {
                try (Cursor cursor = stores[s[0]].openCursor(txn)) {
                  while (cursor.getNext()) {
                    ByteIterable key = cursor.getKey();
                    ByteIterable value = cursor.getValue();
                    count[0]++;
                  }
                }
              }
            });
        }
        long t2 = System.nanoTime();
        double d = (t2-t1)/1e9;
        sum += d;
        avg = sum/tx;
        if(d>max) max = d;
        if(d<min) min = d;
        log.info("read count = {} d = {} avg ={} min={} max={}", count[0], d, avg, min, max);
      }
    }
  }

  public static void main( String[] args ) throws Exception {
    int shards = 10;
    final Environment[] envs = new Environment[shards];
    final Store[] stores = new Store[shards]; final int[] e = new int[1];
    for (int i = 0; i < shards; i++) {
      envs[i] = Environments.newInstance("data"+i);
      e[0] = i;
      stores[i] = envs[i].computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            //return env.openStore("stressdb", WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
            return envs[e[0]].openStore("stressdb", WITHOUT_DUPLICATES, txn);
          }
        });
    }
    int cw = 2 * shards; int rw = 5;
    Thread[] workers = new Thread[cw+rw];
    for(int i = 0; i< shards; i++) {
      workers[i] = new Thread(new WriteTask(envs[i], stores[i]));
      workers[i].start();
      workers[i+1] = new Thread(new WriteTask(envs[i], stores[i]));
      workers[i+1].start();
    }
    
    for(int i = cw; i< cw + rw; i++) {
      workers[i] = new Thread(new ReadTask(envs, stores));
      workers[i].start();
    }

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
