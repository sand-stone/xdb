package xdb;

import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ArrayByteIterable;
import org.jetbrains.annotations.NotNull;
import jetbrains.exodus.management.*;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;

public class LocalStore {
  private static Logger log = LogManager.getLogger(LocalStore.class);

  private static byte[] toBytes(long n, byte[] b, int offset) {
    b[offset+7] = (byte) (n);
    n >>>= 8;
    b[offset+6] = (byte) (n);
    n >>>= 8;
    b[offset+5] = (byte) (n);
    n >>>= 8;
    b[offset+4] = (byte) (n);
    n >>>= 8;
    b[offset+3] = (byte) (n);
    n >>>= 8;
    b[offset+2] = (byte) (n);
    n >>>= 8;
    b[offset+1] = (byte) (n);
    n >>>= 8;
    b[offset] = (byte) (n);
    return b;
  }

  private static long toLong(byte[] b, int offset) {
    return ((((long) b[offset+7]) & 0xFF)
            + ((((long) b[offset+6]) & 0xFF) << 8)
            + ((((long) b[offset+5]) & 0xFF) << 16)
            + ((((long) b[offset+4]) & 0xFF) << 24)
            + ((((long) b[offset+3]) & 0xFF) << 32)
            + ((((long) b[offset+2]) & 0xFF) << 40)
            + ((((long) b[offset+1]) & 0xFF) << 48)
            + ((((long) b[offset]) & 0xFF) << 56));
  }

  private static byte[] writeGuid(UUID guid, byte[] buf) {
    toBytes(guid.getLeastSignificantBits(), buf, 0);
    toBytes(guid.getMostSignificantBits(), buf, 8);
    return buf;
  }

  public static void writeTest() {
    final Environment env = Environments.newInstance("data");
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("idstore", WITHOUT_DUPLICATES, txn);
        }
      });

    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          log.info("start writing");
          for (int i=0;i<2000000;i++) {
            byte[] kbuf = new byte[16];
            ArrayByteIterable key = new ArrayByteIterable(kbuf);
            byte[] vbuf = new byte[64];
            ArrayByteIterable value = new ArrayByteIterable(vbuf);
            UUID guid = UUID.randomUUID();
            writeGuid(guid, kbuf);
            store.put(txn, key, value);
          }
        }
      });
    log.info("done writing");
    for(String name :  env.getStatistics().getItemNames()) {
      StatisticsItem item = env.getStatistics().getStatisticsItem(name);
      log.info("{}={}",name, item.getTotal());
    }
    env.close();
  }

  public static void readTest() {
    final Environment env = Environments.newInstance("data");
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("idstore", WITHOUT_DUPLICATES, txn);
        }
      });

    final int[] count = new int[1];
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          log.info("start reading");
          try (Cursor cursor = store.openCursor(txn)) {
            while (cursor.getNext()) {
              ByteIterable key = cursor.getKey();
              ByteIterable value = cursor.getValue();
              count[0]++;
            }
          }
        }
      });
    log.info("done reading {} records", count[0]);
    for(String name :  env.getStatistics().getItemNames()) {
      StatisticsItem item = env.getStatistics().getStatisticsItem(name);
      log.info("{}={}",name, item.getTotal());
    }
    env.close();
  }

  public static class WriteTask implements Runnable {
    Environment env;
    Store store;

    public WriteTask(Environment env, Store store) {
      this.env = env;
      this.store = store;
    }

    public void run() {
      long t1 = System.nanoTime();
      for (int tx = 0; tx < 1000; tx++) {
        if(tx%10==0) {
          long t2 = System.nanoTime();
          log.info("write 100K transactions {}={}",this,(t2-t1)/1e9);
          t1 = t2;
          //env.gc();
        }
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
              try {
                for (int i=0;i<10000;i++) {
                  byte[] kbuf = new byte[16];
                  ArrayByteIterable key = new ArrayByteIterable(kbuf);
                  byte[] vbuf = new byte[64];
                  ArrayByteIterable value = new ArrayByteIterable(vbuf);
                  UUID guid = UUID.randomUUID();
                  writeGuid(guid, kbuf);
                  store.put(txn, key, value);
                }
              } catch (Exception e) {
                log.info(e);
              }
            }
          });
      }
    }
  }

  public static void writeTest2() {
    try {
      final Environment env = Environments.newInstance("data");
      final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return env.openStore("idstore", WITHOUT_DUPLICATES, txn);
          }
        });
      log.info("start write workers");
      Thread[] workers = new Thread[5];
      for(int i=0; i< workers.length; i++) {
        workers[i] = new Thread(new WriteTask(env, store));
        workers[i].start();
      }

      for(int i=0; i< workers.length; i++) {
        workers[i].join();
      }
      log.info("end writing");
      for(String name :  env.getStatistics().getItemNames()) {
        StatisticsItem item = env.getStatistics().getStatisticsItem(name);
        log.info("{}={}",name, item.getTotal());
      }
      env.close();
    } catch (Exception e) {
      log.info(e);
    }
  }

  public static class ReadTask implements Runnable {
    Environment env;
    Store store;

    public ReadTask(Environment env, Store store) {
      this.env = env;
      this.store = store;
    }

    public void run(){
      final int[] count = new int[1];
      int prev = -1;
      while (true) {
        try {
          Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {}
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
        log.info("read {} records", count[0]);
        if (prev == count[0])
          break;
        else
          prev = count[0];
        count[0]=0;
      }
    }
  }

  public static void readTest2() {
     try {
      final Environment env = Environments.newInstance("data");
      final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return env.openStore("idstore", WITHOUT_DUPLICATES, txn);
          }
        });
      log.info("start read workers");
      Thread[] workers = new Thread[10];
      for(int i=0; i< workers.length; i++) {
        workers[i] = new Thread(new ReadTask(env, store));
        workers[i].start();
      }

      for(int i=0; i< workers.length; i++) {
        workers[i].join();
      }
      log.info("end reading");
      for(String name :  env.getStatistics().getItemNames()) {
        StatisticsItem item = env.getStatistics().getStatisticsItem(name);
        log.info("{}={}",name, item.getTotal());
      }
      env.close();
    } catch (Exception e) {
      log.info(e);
    }
  }

  public static void mixedTest() {
     try {
      final Environment env = Environments.newInstance("data");
      final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return env.openStore("idstore", WITHOUT_DUPLICATES, txn);
          }
        });
      log.info("start workers");
      Thread[] workers = new Thread[8];
      for(int i=0; i< workers.length; i++) {
        if (i%2==0)
          workers[i] = new Thread(new ReadTask(env, store));
        else
          workers[i] = new Thread(new WriteTask(env, store));
        workers[i].start();
      }

      for(int i=0; i< workers.length; i++) {
        workers[i].join();
      }
      log.info("end ");
      for(String name :  env.getStatistics().getItemNames()) {
        StatisticsItem item = env.getStatistics().getStatisticsItem(name);
        log.info("{}={}",name, item.getTotal());
      }
      env.close();
    } catch (Exception e) {
      log.info(e);
    }
  }

  public static class Shard implements Runnable {
    String dir;

    public Shard(String dir) {
      this.dir = dir;
    }

    public void run() {
      try {
        final Environment env = Environments.newInstance(dir);
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
              return env.openStore("idstore#"+dir, WITHOUT_DUPLICATES, txn);
            }
          });
        log.info("start workers {}", dir);
        Thread[] workers = new Thread[8];
        for(int i=0; i< workers.length; i++) {
          if (i%2==0)
            workers[i] = new Thread(new ReadTask(env, store));
          else
            workers[i] = new Thread(new WriteTask(env, store));
          workers[i].start();
        }

        for(int i=0; i< workers.length; i++) {
          workers[i].join();
        }
        log.info("end ");
        for(String name :  env.getStatistics().getItemNames()) {
          StatisticsItem item = env.getStatistics().getStatisticsItem(name);
          log.info("{}={}",name, item.getTotal());
        }
        env.close();
      } catch (Exception e) {
        log.info(e);
      }
    }
  }

  public static void mixedTest2() {
     try {
       long t1 = System.nanoTime();
       Thread[] workers = new Thread[4];
       for(int i=0; i< workers.length; i++) {
         workers[i] = new Thread(new Shard("data/data"+i));
         workers[i].start();
       }

       for(int i=0; i< workers.length; i++) {
         workers[i].join();
       }
       long t2 = System.nanoTime();
       log.info("***total time:{}\n",(t2-t1)/1e9);
     } catch (Exception e) {
       log.info(e);
     }
  }

  public static void main( String[] args ) {
    mixedTest2();
    //readTest2();
  }
}
