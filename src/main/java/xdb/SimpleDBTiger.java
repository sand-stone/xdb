package xdb;

import com.wiredtiger.db.*;
import java.nio.ByteBuffer;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.File;

public class SimpleDBTiger {
  private static Logger log = LoggerFactory.getLogger(SimpleDBTiger.class);

  public static class Event {
    long serverid;
    String table;
    String database;
    long timestamp;
    long position;
    byte[] mapEvent;
    byte[] cudEvent;
  }

  public static boolean checkDir(String dir) {
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

  public static void write() {
    Connection conn =  wiredtiger.open(db, "create");
    Session session = conn.open_session(null);
    session.create("table:acme", "type=lsm,key_format=qSSqq,value_format=uu");

    Cursor c = session.open_cursor("table:acme", null, null);
    session.begin_transaction("isolation=snapshot");
    c.putKeyLong(12345)
      .putKeyString("acmetable")
      .putKeyString("acmeDB")
      .putKeyLong(7654321)
      .putKeyLong(12345678);
    c.putValueByteArray(ByteBuffer.allocate(8).putLong(12345).array())
      .putValueByteArray(ByteBuffer.allocate(8).putLong(54321).array());
    c.insert();
    session.commit_transaction(null);
    conn.close(null);
  }

  public static void read() {
    Connection conn =  wiredtiger.open(db, "create");
    Session session = conn.open_session(null);
    session.create("table:acme", "type=lsm,key_format=qSSqq,value_format=uu");
    Cursor c = session.open_cursor("table:acme", null, null);
    c.next();
    log.info("c={}",c);
    session.begin_transaction("isolation=snapshot");
    log.info("k1={}", c.getKeyLong());
    log.info("k2={}", c.getKeyString());
    log.info("k3={}", c.getKeyString());
    log.info("k4={}", c.getKeyLong());
    log.info("k5={}", c.getKeyLong());
    log.info("v1={}", ByteBuffer.wrap(c.getValueByteArray()).getLong());
    log.info("v2={}", ByteBuffer.wrap(c.getValueByteArray()).getLong());
    session.commit_transaction(null);
    conn.close(null);
  }

  private static String db = "./simpledb";

  public static void main( String[] args ) throws Exception {
    checkDir(db);
    write();
    read();
  }

}
