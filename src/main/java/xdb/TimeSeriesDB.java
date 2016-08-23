package xdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSeriesDB {
  private static Logger log = LoggerFactory.getLogger(TimeSeriesDB.class);

  public static class Event {
    public String host;
    public String metric;
    public long ts;
    public byte[] val;

    public Event(String host, String metric, long ts, byte[] val) {
      this.host = host;
      this.metric = metric;
      this.ts = ts;
      this.val = val;
    }

    public Event(String host, String metric, long ts) {
      this(host, metric, ts, null);
    }

    public String toString() {
      return host +"#"+ metric + "#"+ts;
    }
  }

  public static class EventFactory {
    private Random rnd;
    private String[] metrics;
    private String[] hosts;

    public EventFactory(int numhosts, int nummetrics) {
      hosts = new String[numhosts];
      metrics = new String[nummetrics];
      for(int i = 0; i < numhosts; i++) {
        hosts[i] = "hosts"+i;
      }
      for(int i = 0; i < nummetrics; i++) {
        metrics[i] = "metrics"+i;
      }
      rnd = new Random(numhosts+nummetrics);
    }

    public Event getNextEvent() {
      return new Event(hosts[rnd.nextInt(hosts.length)], metrics[rnd.nextInt(metrics.length)], Instant.now().toEpochMilli(), new byte[64]);
    }
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

  public static class Ingestor implements Runnable {
    Session session;
    int batch;

    public Ingestor(Connection conn) {
      session = conn.open_session(null);
      session.create(table, storage);
      batch = 10;
    }

    public void run() {
      //EventFactory producer = new EventFactory(100000000, 1000);
      EventFactory producer = new EventFactory(10, 100);
      Cursor c = session.open_cursor(table, null, null);
      int count = 5;
      while(count-->0) {
        session.begin_transaction(tnx);
        for(int i = 0; i < batch; i++) {
          Event evt = producer.getNextEvent();
          c.putKeyString(evt.host);
          c.putKeyString(evt.metric);
          c.putKeyLong(evt.ts);
          c.putValueInt(ttl);
          c.putValueByteArray(evt.val);
          c.insert();
        }
        counter.addAndGet(batch);
        session.commit_transaction(null);
      }
    }

  }

  public static class TTLMonitor implements Runnable {
    Session session;

    public TTLMonitor(Connection conn) {
      session = conn.open_session(null);
      session.create(table, null);
    }

    public void run() {
      int interval = 1000;
      while(!stop) {
        try {Thread.currentThread().sleep(interval);} catch(Exception ex) {}
        Cursor c = session.open_cursor(table, null, null);
        int ret = -1;
        while((ret = c.next()) == 0) {
          Cursor uc = session.open_cursor(null, c, null);
          int ttl = c.getValueInt();
          byte[] val = c.getValueByteArray();
          String host = c.getKeyString();
          String metric = c.getKeyString();
          long ts = c.getKeyLong();
          if(ttl-interval<=0) {
            uc.putKeyString(host);
            uc.putKeyString(metric);
            uc.putKeyLong(ts);
            log.info("remove host {} metric {} ts {} {}", host, metric, ts, ttl);
            int r = uc.remove();
            if(r!=0) {
              throw new RuntimeException("fails to remove");
            }
            uc.close();
            continue;
          }
          log.info("update host {} metric {} ts {} {}==>{}", host, metric, ts, ttl, ttl-interval);
          uc.putKeyString(host);
          uc.putKeyString(metric);
          uc.putKeyLong(ts);
          uc.putValueInt(ttl-interval);
          uc.putValueByteArray(val);
          int r = uc.update();
          if(r!=0) {
            throw new RuntimeException("fails to update");
          }
          uc.close();
        }
        c.close();
      }
    }

  }

  public static class Analyst implements Runnable {
    Session session;

    public Analyst(Connection conn) {
      session = conn.open_session(null);
      session.create(table, null);
    }


    public void run() {
      while(!stop) {
        Cursor c = session.open_cursor(table, null, null);
        int ret = c.next();
        if(ret<0) {
          log.info("nothing to read");
          try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
          c.close();
          continue;
        }
        Event evt = new Event(c.getKeyString(), c.getKeyString(), c.getKeyLong());
        log.info("evt={}", evt);
        c.close();
      }
    }

  }

  private static boolean stop = false;
  private static final String db = "./tsdb";
  private static final String table = "table:metrics";
  private static final String cols = "columns=(host,metric,ts,ttl,val)";
  private static final String storage = "type=lsm,key_format=SSq,value_format=iu";
  private static final String tnx = "isolation=snapshot";
  private static final int ttl = 3000;

  static AtomicInteger counter = new AtomicInteger(0);

  public static void main( String[] args ) throws Exception {
    checkDir(db);
    Connection conn =  wiredtiger.open(db, "create");
    new Thread(new Ingestor(conn)).start();
    new Thread(new Analyst(conn)).start();
    new Thread(new TTLMonitor(conn)).start();
    int count = 100;
    while(true) {
      int c1= counter.get();
      try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
      int c2 = counter.get();
      if(c2 >= count)
        break;
      log.info("evts processed {} {}/{}", c2-c1, c2, count);
    }
    //stop = true;
    //log.info("counter={}", counter.get());
    try {Thread.currentThread().sleep(3000);} catch(Exception ex) {}
    while(true) ;
    //conn.close(null);
  }

}
