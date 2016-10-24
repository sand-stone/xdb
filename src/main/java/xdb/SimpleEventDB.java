package xdb;

import com.wiredtiger.db.*;
import java.nio.ByteBuffer;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleEventDB {
  private static Logger log = LoggerFactory.getLogger(SimpleEventDB.class);

  final static String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";

  private static boolean checkDir(String dir) {
    boolean ret = true;
    File d = new File(dir);
    if(d.exists()) {
      if(d.isFile())
        ret = false;
    } else {
      d.mkdirs();
    }
    return ret;
  }

  public static class RawEvent {
    public byte[] deviceID; //16 bytes
    public byte[] timestamp; //8 byte
    public byte[] eventType; //2 bytes
    public byte[] data; //1024 bytes
  }

  public static class JoinEvent {
    public byte[] deviceID; //16 bytes
    public byte[] queryID; //4 bytes
    public byte[] timebucket; //2 bytes
    public byte[] values; // columns
  }

  public static void main( String[] args ) throws Exception {
    String db = "./eventdb";
    checkDir(db);
    Connection conn =  wiredtiger.open(db, dbconfig);
    System.out.println("conn:"+conn);
    String evtTable = "table:rawevents";

    Session session = conn.open_session(null);
    session.create(evtTable, "type=lsm,key_format=u,value_format=u");
    Cursor c = session.open_cursor(evtTable, null, null);
    for (long i = 0; i < 10; i++) {
      byte[] key = ByteBuffer.allocate(8).putLong(i).array();
      byte[] val = ByteBuffer.allocate(8).putLong(1234567+i).array();
      c.putKeyByteArray(key);
      c.putValueByteArray(val);
      c.update();
    }
    c.close();
    session.close(null);

    session = conn.open_session(null);
    c = session.open_cursor(evtTable, null, null);
    for (long i = 0; i < 10; i++) {
      byte[] key = ByteBuffer.allocate(8).putLong(i).array();
      c.putKeyByteArray(key);
      if(c.search() == 0) {
        byte[] v = c.getValueByteArray();
        byte[] val = ByteBuffer.allocate(16).put(v).putLong(i).array();
        c.putKeyByteArray(c.getKeyByteArray());
        c.putValueByteArray(val);
        c.update();
      }
      c.reset();
    }
    c.close();
    session.close(null);

    session = conn.open_session(null);
    c = session.open_cursor(evtTable, null, null);
    while(c.next() == 0) {
      byte[] key = c.getKeyByteArray();
      byte[] val = c.getValueByteArray();
      log.info("key {} val {} ", key, val);
    }
    c.close();
    session.close(null);

    conn.close(null);

  }

  public static void test( String[] args ) throws Exception {
    String db = "./eventdb";
    checkDir(db);
    Connection conn =  wiredtiger.open("db", dbconfig);
    System.out.println("conn:"+conn);
    Session session = conn.open_session(null);
    session.create("table:rawevents", "type=lsm,key_format=u,value_format=u");
    session.create("table:joinevents", "type=lsm,key_format=u,value_format=u");

    int count = 2000; int batch = 1000;
    Cursor c = session.open_cursor("table:rawevents", null, null);
    for(int i = 0; i < count; i++) {
      session.begin_transaction("isolation=snapshot");
      for (int j = 0; j < batch; j++) {
        UUID uuid = UUID.randomUUID();
        long hi = uuid.getMostSignificantBits();
        long lo = uuid.getLeastSignificantBits();
        byte[] key = ByteBuffer.allocate(16).putLong(hi).putLong(lo).array();
        byte[] val = ByteBuffer.allocate(8).putLong(j).array();
        c.putKeyByteArray(key);
        c.putValueByteArray(val);
        c.insert();
      }
      session.commit_transaction(null);
      if(i%100 == 0) {
        System.out.println("remaining count "+(count-i));
      }
    }
    c.close();
    conn.close(null);
  }

}
