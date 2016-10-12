package xdb;

import static org.lmdbjava.CursorIterator.IteratorType.BACKWARD;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import org.lmdbjava.CursorIterator.KeyVal;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPFIXED;
import static org.lmdbjava.Env.open;
import org.lmdbjava.Env;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.lmdbjava.Cursor;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;

import java.nio.ByteBuffer;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleLMDB {
  private static Logger log = LoggerFactory.getLogger(SimpleLMDB.class);
  
  private static File checkDir(String dir) {
    File d = new File(dir);
    if(!d.exists()) {
      d.mkdirs();
    } else {
      if(d.isFile())
        throw new RuntimeException("file exists:" + dir);
    } 
    return d;
  }
  
  public static void main( String[] args ) throws Exception {
    String guids = "./guids";
    //File path = checkDir(guids);
    File path = new File(guids);
    Env<ByteBuffer> env = open(path, 256, MDB_NOSUBDIR);
    Dbi<ByteBuffer> db = env.openDbi("acme", MDB_CREATE);
    int count = 2000; int batch = 1000;
    for(int i = 0; i < count; i++) {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = db.openCursor(txn);
        for (int j = 0; j < batch; j++) {
          UUID uuid = UUID.randomUUID();
          long hi = uuid.getMostSignificantBits();
          long lo = uuid.getLeastSignificantBits();
          ByteBuffer key = ByteBuffer.allocateDirect(16).putLong(hi).putLong(lo);
          ByteBuffer val = ByteBuffer.allocateDirect(8).putLong(j);
          key.flip(); val.flip();
          c.put(key, val);
        }
        txn.commit();
      }
    }
    env.close();
  }

}
