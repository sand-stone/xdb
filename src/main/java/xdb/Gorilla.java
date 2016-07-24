package xdb;

import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ArrayByteIterable;
import org.jetbrains.annotations.NotNull;
import jetbrains.exodus.management.*;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;

public class Gorilla {
  private static Logger log = LogManager.getLogger(Gorilla.class);

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

  public static class Event {
    String key;
    double val;
    long ts;

    public static ByteIterable get(String key) {
      byte[] k = key.getBytes();
      byte[] buf = new byte[k.length+8];
      System.arraycopy(k, 0, buf, 0, k.length);
      return new ArrayByteIterable(buf);
    }

    public static ByteIterable get(String key, long ts) {
      byte[] k = key.getBytes();
      byte[] buf = new byte[k.length+8];
      System.arraycopy(k, 0, buf, 0, k.length);
      toBytes(ts, buf, k.length);
      return new ArrayByteIterable(buf);
    }

    public static String get(ByteIterable key) {
      byte[] bytes = key.getBytesUnsafe();
      try {
        return new String(bytes, 0, bytes.length-10, "US-ASCII");
      } catch(java.io.UnsupportedEncodingException e) {}
      return "";
    }

    public Event(String key, double val, long ts) {
      this.key = key;
      this.val = val;
      this.ts = ts;
    }

    public String toString() {
      return key + ";val=" + val + ";ts=" + ts;
    }

    public ByteIterable getKey() {
      return get(key, ts);
    }

    public ByteIterable getValue() {
      byte[] buf = new byte[8];
      return new ArrayByteIterable(toBytes(Double.doubleToLongBits(val), buf, 0));
    }

  }

  public static class DataGeneratorTask implements Runnable {
    private static String[] streams = new String[]{"%s|%s|CPU", "%s|%s|MEM"};
    private String[] keys;
    private LinkedBlockingQueue<Event> evtsq;
    private int cap;
    private Random rnd;
    private Shard shard;
    private boolean stop;

    public DataGeneratorTask(Shard shard) {
      this.shard = shard;
      this.evtsq = new LinkedBlockingQueue<Event>();
      this.cap = 1000000;
      this.rnd = new Random(this.hashCode());
      stop = false;
    }

    public Event poll() {
      return evtsq.poll();
    }

    public boolean empty() {
      return evtsq.size() <= 0;
    }

    public int qsize() {
      return evtsq.size();
    }

    public void stop() {
      stop = true;
    }

    public void run() {
      log.info("data generation starts");
      while (!stop) {
        if(evtsq.size()>cap) {
          try {
            Thread.currentThread().sleep(100);
          } catch(InterruptedException e) {}
          continue;
        }
        for(String t : streams) {
          StringBuilder sb = new StringBuilder();
          Formatter formatter = new Formatter(sb, Locale.US);
          long ts = Instant.now().toEpochMilli()+rnd.nextInt(5000);
          double val = rnd.nextDouble();
          formatter.format(t, "shard"+shard.id+"#"+rnd.nextInt(5), "machine"+rnd.nextInt(1000));
          Event evt = new Event(sb.toString(), val, ts);
          try {
            evtsq.put(evt);
          } catch(InterruptedException e) {}
        }
      }
      log.info("data generation stops");
    }

  }

  public static class WriteTask implements Runnable {
    Environment env;
    Store store;
    DataGeneratorTask dt;

    public WriteTask(Environment env, Store store, DataGeneratorTask dt) {
      this.env = env;
      this.store = store;
      this.dt = dt;
    }

    public void run() {
      final int[] count = new int[1];
      log.info("write task starts");
      while(!dt.empty()) {
        count[0] = 0;
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
              try {
                int batch=10000;
                long t1 = System.nanoTime();
                while (!dt.empty()) {
                  if (count[0]>=batch)
                    break;
                  Event evt = dt.poll();
                  if(evt == null) break;
                  //log.info("pull {}", evt);
                  store.put(txn, evt.getKey(), evt.getValue());
                  count[0]++;
                }
                long t2 = System.nanoTime();
                log.info("commit {} transactions in {} mill-seconds", count[0], (t2-t1)/1e6);
              } catch (Exception e) {
                log.info(e);
              }
            }
          });
      }
      log.info("write task stops");
    }

  }


  public static class Shard implements Runnable {
    Environment env;
    Store store;
    int id;

    public Shard(String dir, int id) {
      this.id = id;
      env = Environments.newInstance(dir);
      store = env.computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return env.openStore("gorillastore#"+dir, WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
          }
        });
      log.info("create shard {}", dir);
    }

    public void run() {
      try {
        DataGeneratorTask dt = new DataGeneratorTask(this);
        Thread[] workers = new Thread[3];
        workers[0] = new Thread(dt);
        workers[0].start();
        Thread.currentThread().sleep(1000);
        for(int i=1; i< workers.length; i++) {
          workers[i] = new Thread(new WriteTask(env, store, dt));
          workers[i].start();
        }
        Thread.currentThread().sleep(5000);
        dt.stop();
        for(int i=0; i< workers.length; i++) {
          workers[i].join();
        }
      } catch(Exception e) {
        log.info(e);
      }
    }

    Environment getEnv() {
      return env;
    }

    Store getStore() {
      return store;
    }

  }

  public static class AnalyticsTask implements Runnable {
    Shard[] shards;
    private boolean stop;

    public AnalyticsTask(Shard[] shards) {
      this.shards = shards;
      stop =false;
    }

    public void stop() {
      stop = true;
    }

    public void run() {
      log.info("start analyst");
      while (!stop) {
        try {
          for(Shard shard : shards) {
            Environment env = shard.getEnv();
            Store store = shard.getStore();
            env.executeInReadonlyTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                  try (Cursor cursor = store.openCursor(txn)) {
                    if(cursor.getSearchKeyRange(Event.get("shard1#1|machine00000|CPU")) != null) {
                      log.info("found count {} {}", cursor.count(), Event.get(cursor.getKey()));
                      while (cursor.getNext()) {
                        ByteIterable key = cursor.getKey();
                        //log.info("key {}", Event.get(key));
                        ByteIterable value = cursor.getValue();
                      }
                    }
                  }
                }
              });
          }
        } catch (Exception e) {
          log.info(e);
        }
      }
      log.info("ends analyst");
    }

  }

  public static void scenario1() {
    int c = 2;
    Shard[] shards = new Shard[c];
    Thread[] threadcamp1 = new Thread[c];
    for(int i = 0; i < shards.length; i++) {
      shards[i] = new Shard("data/"+i,i);
      threadcamp1[i] = new Thread(shards[i]);
      threadcamp1[i].start();
    }

    AnalyticsTask[] analysts = new AnalyticsTask[1];
    Thread[] threadcamp2 = new Thread[analysts.length];
    for(int i=0; i< threadcamp2.length; i++) {
      analysts[i] = new AnalyticsTask(shards);
      threadcamp2[i] = new Thread(analysts[i]);
      threadcamp2[i].start();
    }
    try {
      Thread.currentThread().sleep(8000);
    } catch(InterruptedException e) {}

    for(AnalyticsTask analyst : analysts) {
      analyst.stop();
    }
    for(int i=0; i< threadcamp1.length; i++) {
      try {
        threadcamp1[i].join();
      } catch(InterruptedException e) {}
    }
    for(int i=0; i< threadcamp2.length; i++) {
      try {
        threadcamp2[i].join();
      } catch(InterruptedException e) {}
    }
  }

  public static void main( String[] args ) {
    scenario1();
  }
}
