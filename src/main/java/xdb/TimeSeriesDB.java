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
    public String site;
    public String metric;
    public long ts;
    public byte[] val;

    public Event(String site, String metric, long ts, byte[] val) {
      this.site = site;
      this.metric = metric;
      this.ts = ts;
      this.val = val;
    }

    public Event(String site, String metric, long ts) {
      this(site, metric, ts, null);
    }

    public String toString() {
      return site +"#"+ metric + "#"+ts;
    }
  }

  public static class EventFactory {
    private Random rnd;
    private String[] metrics;
    private String[] sites;

    public EventFactory(int numsites, int nummetrics) {
      sites = new String[numsites];
      metrics = new String[nummetrics];
      for(int i = 0; i < numsites; i++) {
        sites[i] = "sites"+i;
      }
      for(int i = 0; i < nummetrics; i++) {
        metrics[i] = "metrics"+i;
      }
      rnd = new Random(numsites+nummetrics);
    }

    public Event getNextEvent() {
      return new Event(sites[rnd.nextInt(sites.length)], metrics[rnd.nextInt(metrics.length)], Instant.now().toEpochMilli(), new byte[64]);
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
      EventFactory producer = new EventFactory(1000, 100);
      Cursor c = session.open_cursor(table, null, null);
      while(!stop) {
        session.begin_transaction(tnx);
        for(int i = 0; i < batch; i++) {
          Event evt = producer.getNextEvent();
          c.putKeyString(evt.site);
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
      while(!stop) {
        Cursor c = session.open_cursor(table, null, null);
        int ret = c.next();
        if(ret<0) { c.close(); continue; }
        int ttl = c.getValueInt();
        byte[] val = c.getValueByteArray();
        String site = c.getKeyString();
        String metric = c.getKeyString();
        long ts = c.getKeyLong();
        c.putKeyString(site);
        c.putKeyString(metric);
        c.putKeyLong(ts);
        c.putValueInt(ttl);
        c.putValueByteArray(val);
        c.update();
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
      Cursor c = session.open_cursor(table, null, null);
      while(!stop) {
        int ret = c.next();
        if(ret<0) continue;
        Event evt = new Event(c.getKeyString(), c.getKeyString(), c.getKeyLong());
        log.info("evt={}", evt);
      }
    }

  }

  private static boolean stop = false;
  private static final String db = "./tsdb";
  private static final String table = "table:metrics";
  private static final String cols = "columns=(site,metric,ts,ttl,val)";
  private static final String storage = "type=lsm,key_format=SSq,value_format=iu";
  private static final String tnx = "isolation=snapshot";
  private static final int ttl = 3000;

  static AtomicInteger counter = new AtomicInteger(0);

  public static void main( String[] args ) throws Exception {
    checkDir(db);
    Connection conn =  wiredtiger.open(db, "create");
    new Thread(new Ingestor(conn)).start();
    //new Thread(new Analyst(conn)).start();
    new Thread(new TTLMonitor(conn)).start();
    int count = 1000000;
    while(true) {
      int c1= counter.get();
      try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
      int c2 = counter.get();
      if(c2 >= count)
        break;
      log.info("evts processed {} {}/{}", c2-c1, c2, count);
    }
    stop = true;
    try {Thread.currentThread().sleep(3000);} catch(Exception ex) {}
    conn.close(null);
  }

}
