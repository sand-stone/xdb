package xdb;

import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import org.jetbrains.annotations.NotNull;
import jetbrains.exodus.management.*;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.bindings.LongBinding.entryToLong;
import static jetbrains.exodus.bindings.LongBinding.longToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;

public class SimpleDB {
  private static Logger log = LogManager.getLogger(SimpleDB.class);

  public static class Event {
    String key;
    long ts;
    double val;

    public static ByteIterable get(String key) {
      return stringToEntry(key);
    }

    public static ByteIterable get(String key, long ts) {
      ByteIterable[] segs = new ByteIterable[2];
      segs[0] = stringToEntry(key);
      segs[1] = longToEntry(ts);
      return new CompoundByteIterable(segs);
    }

    public static String getKey(ByteIterable key) {
      byte[] bytes = key.getBytesUnsafe();
      return entryToString(key);
    }

    public static long getTS(ByteIterable key) {
      byte[] bytes = key.getBytesUnsafe();
      byte[] d = new byte[8];
      System.arraycopy(bytes, key.getLength()-8, d, 0, 8);
      return entryToLong(new ArrayByteIterable(d));
    }

    public Event(String key, long ts, double val) {
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
      return longToEntry(Double.doubleToLongBits(val));
    }

  }

  public static void doTest() {
    final Environment env = Environments.newInstance("data");
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("simpledb", WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
        }
      });
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          log.info("start writing");
          Event evt = new Event("ABC", 10000, 100000);
          store.put(txn, evt.getKey(), evt.getValue());
          evt = new Event("ABCD", 200, 100000);
          store.put(txn, evt.getKey(), evt.getValue());
          evt = new Event("AAAA", 300, 100000);
          store.put(txn, evt.getKey(), evt.getValue());
          evt = new Event("ABC", 888, 100000);
          store.put(txn, evt.getKey(), evt.getValue());          
          evt = new Event("ABC", 20000, 100000);
          store.put(txn, evt.getKey(), evt.getValue());

          //log.info(evt);
        }
      });
    env.executeInReadonlyTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          try (Cursor cursor = store.openCursor(txn)) {
            ByteIterable k = Event.get("AB",9999);
            if(cursor.getSearchKeyRange(k) != null) {
              int count = 0;
              do {
                ByteIterable key = cursor.getKey();
                ByteIterable value = cursor.getValue();
                log.info("key {} = {}.{}", cursor.getKey(), Event.getKey(cursor.getKey()), Event.getTS(cursor.getKey()));
                count++;
              } while (cursor.getNext());
              log.info("count {}", count);
            }
          }
        }
      });
  }

  public static void main( String[] args ) {
    doTest();
  }

}
