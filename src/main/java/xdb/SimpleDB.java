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
import jetbrains.exodus.util.LightOutputStream;
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

    public static ByteIterable get(String key, long ts) {
      final LightOutputStream output = new LightOutputStream();
      output.writeString(key);
      LongBinding.writeCompressed(output, ts);
      return output.asArrayByteIterable();
    }

    public static Event getKey(ByteIterable bytes) {
      final ByteIterator iterator = bytes.iterator();
      String key = entryToString(bytes);
      iterator.skip(key.length()+1);
      long ts = LongBinding.readCompressed(iterator);
      return new Event(key, ts, -1);
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
                Event evt = Event.getKey(key);
                log.info("key {}.{}", evt.key, evt.ts);
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
