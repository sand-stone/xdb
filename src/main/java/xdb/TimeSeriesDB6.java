package xdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSeriesDB6 {
  private static Logger log = LogManager.getLogger(TimeSeriesDB6.class);

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

  public static class Ingestor implements Runnable {
    String table;
    int id;
    Random rnd;

    public Ingestor(String table, int id) {
      this.table = table;
      this.id = id;
      this.rnd = new Random();
    }

    private byte[] getKey() {
      UUID uuid = UUID.randomUUID();
      long hi = uuid.getMostSignificantBits();
      long lo = uuid.getLeastSignificantBits();
      return ByteBuffer.allocate(14).putShort((short)rnd.nextInt(60)).putLong(hi).putInt((int)lo).array();
    }

    public void run() {
      Random rnd = new Random();
      Session session = conn.open_session(null);
      session.create(table, storage);
      log.info("ingestor {} starts", id);
      int batch = 1024;
      int total = 0;
      byte[] val = new byte[1024];
      Cursor c = session.open_cursor(table, null, null);
      boolean done = false;
      long t1 = 0, t2 = 0, c2 = 100;
      while(!stop) {
        try {
          done = false;
          t1 = System.nanoTime();
          session.begin_transaction(tnx);
          for(int i = 0; i < batch; i++) {
            c.putKeyByteArray(getKey());
            rnd.nextBytes(val);
            c.putValueByteArray(val);
            c.insert();
          }
          done = true;
        } catch(WiredTigerRollbackException e) {
          session.rollback_transaction(tnx);
          log.info("ingestor roll back");
        } finally {
          if(done) {
            session.commit_transaction(null);
            t2 = System.nanoTime();
            if(c2--<= 0) {
              log.info("writer {} write 1MB in {} \n", id, (t2-t1)/1e9);
              c2 = 100;
            }
            counter.addAndGet(batch);
          }
        }
      }
      c.close();
      session.close(null);
      log.info("ingestor {} stopped", id);
    }

  }

  public static class Analyst implements Runnable {
    String table;
    int id;

    public Analyst(String table, int id) {
      this.table = table;
      this.id = id;
    }

    public void run() {
      int ret;
      try {Thread.currentThread().sleep(10000);} catch(Exception ex) {}
      Session session = conn.open_session(null);
      session.create(table, storage);
      while(true) {
        Cursor c = null;
        long count = 0, t1 = 0, t2 = 0;
        byte[] skey = new byte[2];
        try {
          c = session.open_cursor(table, null, null);
          t1 = System.nanoTime();
          for (int i = 0; i < 60; i++) {
            //byte[] skey = ByteBuffer.allocate(2).putShort((short)i).array();
            skey[0] = (byte)(i&0xF0);
            skey[1] = (byte)(i&0x0F);
            c.putKeyByteArray(skey);
            SearchStatus status = c.search_near();
            switch(status) {
            case NOTFOUND:
              log.info("no data points for time period {}", i);
              break;
            case SMALLER:
              //log.info("scan smaller time period {}", i);
              do {
                byte[] key = c.getKeyByteArray();
                int t = key[0]<<8|key[0];
                //int t = key[0];
                //log.info("S i = {} t = {}", i, t);
                if (t != i) break;
                byte[] val = c.getValueByteArray();
                count++;
              } while(c.prev() == 0);
              break;
            case FOUND:
            case LARGER:
              //log.info("scan bigger time period {}", i);
              do {
                byte[] key = c.getKeyByteArray();
                int t = key[0]<<8|key[0];
                //int t = key[0];
                //int t = key[1]<<8 | key[0];
                //log.info("L i = {} t = {}", i, t);
                if (t != i) break;
                byte[] val = c.getValueByteArray();
                count++;
              } while(c.next() == 0);
              break;
            }
          }
          t2 = System.nanoTime();
        } catch(WiredTigerRollbackException e) {
          log.info("analyst roll back");
        } catch(WiredTigerException e) {
          log.info("analyst {}", e);
        } finally {
          if(c != null)
            c.close();
        }
        log.info("reader {} read {} rows in {} \n", id, count, (t2-t1)/1e9);
      }
      //session.close(null);
    }

  }

  private static final String db = "./testdb";
  private static final String hottable = "table:hot";
  private static final String coldtable = "table:cold";
  private static final String storage = "type=lsm,key_format=u,value_format=u";
  private static final String tnx = "isolation=snapshot";

  static AtomicInteger counter = new AtomicInteger(0);

  private static Connection init(String db) {
    checkDir(db);
    Connection conn = wiredtiger.open(db, "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)");
    Session session = conn.open_session(null);
    session.create(hottable, storage);
    session.create(coldtable, storage);
    session.checkpoint(null);
    session.close(null);
    return conn;
  }

  private static Connection conn;
  private static boolean stop;

  public static void main( String[] args ) throws Exception {

    conn = init(db);

    log.info("start writing into cold table");
    stop = false;
    int nw = 15;
    for (int i= 0; i < nw; i++) {
      new Thread(new Ingestor(coldtable, i)).start();
    }

    int n = 1000;

    while(n-->0) {
      try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
      if(n%30 == 0)
        log.info("evts processed {} n = {}", counter.get(), n);
    }

    stop = true;
    log.info("start readers");
    int nr = 3;
    for (int i= 0; i < nr; i++) {
      new Thread(new Analyst(coldtable, i)).start();
    }

    try {Thread.currentThread().sleep(10000);} catch(Exception ex) {}
    log.info("start writing into hot table");
    stop = false;
    for (int i= 0; i < nw; i++) {
      new Thread(new Ingestor(hottable, i)).start();
    }

    while(true) {
      try {Thread.currentThread().sleep(30000);} catch(Exception ex) {}
      log.info("evts processed {}", counter.get());
    }
    //conn.close(null);
  }

}
