package xdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSeriesDB2 {
  private static Logger log = LoggerFactory.getLogger(TimeSeriesDB2.class);

  public static class Tuple2<T1 extends Comparable, T2 extends Comparable> implements Comparable<Tuple2> {

    public final T1 v1;
    public final T2 v2;

    public T1 v1() {
      return v1;
    }

    public T2 v2() {
      return v2;
    }

    public int compareTo(Tuple2 t) {
      int c = v1.compareTo(t.v1);
      return c !=0 ? c : v2.compareTo(t.v2);
    }

    public Tuple2(T1 v1, T2 v2) {
      this.v1 = v1;
      this.v2 = v2;
    }

    public int hashCode() {
      return v1.hashCode() | v2.hashCode();
    }

    public boolean equals(Object o) {
      if(o instanceof Tuple2) {
        Tuple2 t = (Tuple2)o;
        return v1.equals(t.v1) && v2.equals(t.v2);
      }
      return false;
    }

    public String toString() {
      return v1.toString()+"#"+v2.toString();
    }
  }

  public interface Tuple {
    static <T1 extends Comparable, T2 extends Comparable> Tuple2<T1, T2> tuple(T1 v1, T2 v2) {
      return new Tuple2<>(v1, v2);
    }
  }

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

    public Event(long ts, String host, String metric) {
      this(host, metric, ts, null);
    }

    public String getHost() {
      return host;
    }

    public String getMetric() {
      return metric;
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
        hosts[i] = "host"+i;
      }
      for(int i = 0; i < nummetrics; i++) {
        metrics[i] = "metric"+i;
      }
      rnd = new Random(numhosts+nummetrics);
    }

    public Event getNextEvent() {
      return new Event(hosts[rnd.nextInt(hosts.length)], metrics[rnd.nextInt(metrics.length)], Instant.now().toEpochMilli(), new byte[64]);
    }

    public static Event getStartEvent() {
      return new Event("zzzz", "zzzz", 0, new byte[64]);
    }

    public static Event getEndEvent() {
      return new Event("zzzz", "zzzz", Instant.now().toEpochMilli(), new byte[64]);
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
    int batch;

    public Ingestor() {
      batch = 10;
    }

    public void run() {
      //EventFactory producer = new EventFactory(100000000, 1000);
      log.info("ingestor starts");
      Session session = conn.open_session(null);
      session.create(table, storage);
      EventFactory producer = new EventFactory(10, 10);
      Cursor c = session.open_cursor(table, null, null);
      int count = 10;
      while(count-->0) {
        session.begin_transaction(tnx);
        for(int i = 0; i < batch; i++) {
          Event evt = producer.getNextEvent();
          c.putKeyLong(evt.ts);
          c.putKeyString(evt.host);
          c.putKeyString(evt.metric);
          c.putValueByteArray(evt.val);
          c.insert();
        }
        counter.addAndGet(batch);
        session.commit_transaction(null);
      }
      log.info("ingestor ends {}", counter.get());
      session.close(null);
    }

  }

  public static class TTLMonitor implements Runnable {
    Session session;

    public TTLMonitor() {
    }

    public void run() {
      int interval = 1;
      Session session = conn.open_session(null);
      session.create(table, storage);
      while(!cancel) {
        try {Thread.currentThread().sleep(interval*1000);} catch(Exception ex) {}
        log.info("TTL scanning starts {}", session);
        Cursor start = null;
        Cursor stop = null;
        try {
          Event evt1 = EventFactory.getStartEvent();
          Cursor mc = session.open_cursor(table, null, null);
          mc.putKeyLong(evt1.ts);
          mc.putKeyString(evt1.host);
          mc.putKeyString(evt1.metric);
          mc.putValueByteArray(evt1.val);
          mc.insert();
          Event evt2 = EventFactory.getEndEvent();
          mc.putKeyLong(evt2.ts);
          mc.putKeyString(evt2.host);
          mc.putKeyString(evt2.metric);
          mc.putValueByteArray(evt2.val);
          mc.insert();
          mc.close();
          start = session.open_cursor(table, null, null);
          start.putKeyLong(evt1.ts);
          start.putKeyString(evt1.host);
          start.putKeyString(evt1.metric);
          start.putValueByteArray(evt1.val);
          int r1 = start.search();
          stop = session.open_cursor(table, null, null);
          stop.putKeyLong(evt2.ts);
          stop.putKeyString(evt2.host);
          stop.putKeyString(evt2.metric);
          stop.putValueByteArray(evt2.val);
          int r2 = stop.search();
          if(r1==0 && r2==0) {
            int ret = session.truncate(null, start, stop, null);
            log.info("truncate ret {} for the past {} seconds ", ret, (stop.getKeyLong() - start.getKeyLong())/1000);
            break;
          } else {
            log.info("not found");
          }
        } catch(WiredTigerRollbackException e) {
          log.info("ttl monitor roll back");
        } finally {
          if(start != null)
            start.close();
          if(stop != null)
            stop.close();
        }
      }
      session.close(null);
    }

  }

  static long past(int seconds) {
    return Instant.now().toEpochMilli() - seconds*1000;
  }

  public static class Analyst implements Runnable {
    Session session;

    public Analyst(Session session) {
      this.session = session;
    }

    private void report(List<Event> evts) {
      log.info("select count(*) from events group by host, metric = {}",
               evts
               .stream()
               .collect((
                         Collectors.groupingBy(
                                               evt -> Tuple.tuple(evt.host, evt.metric),
                                               Collectors.counting())))
               .entrySet().stream()
               .sorted(Comparator.comparing(Map.Entry::getKey))
               .collect(Collectors.toList())
               );
    }

    public void run() {
      int ret;
      while(!cancel) {
        try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
        log.info("Analyst starts");
        Cursor c = session.open_cursor(table, null, null);
        long ts = past(10);
        c.putKeyLong(ts);
        SearchStatus status = c.search_near();
        switch(status) {
        case NOTFOUND:
          log.info("no data points");
          break;
        case SMALLER:
          log.info("beyond time horizon {}", (ts-c.getKeyLong())/1000.0);
          break;
        case FOUND:
        case LARGER:
          log.info("ts {}", ts);
          List<Event> evts = new ArrayList<Event>();
          do {
            Event evt = new Event(c.getKeyLong(), c.getKeyString(), c.getKeyString());
            evts.add(evt);
          } while(c.next() == 0);
          report(evts);
          break;
        }
        c.close();
        log.info("Analyst ends");
      }
    }

  }

  private static boolean cancel = false;
  private static final String db = "./tsdb";
  private static final String table = "table:metrics";
  private static final String cols = "columns=(ts,host,metric,val)";
  private static final String storage = "type=lsm,key_format=qSS,value_format=u,"+cols;
  private static final String tnx = "isolation=snapshot";
  private static final int ttl = 3000;
  private static final String ttlindex = "index:metrics:ttl";
  private static final String ttlcol = "columns=(ttl)";
  private static final String tsindex = "index:metrics:ts";
  private static final String tscol = "columns=(ts)";

  static AtomicInteger counter = new AtomicInteger(0);

  private static void init() {
    checkDir(db);
    conn =  wiredtiger.open(db, "create");
  }

  private static Connection conn;

  public static int rowcount() {
    Session session = conn.open_session(null);
    session.create(table, storage);
    Cursor c = session.open_cursor(table, null, null);
    int rc = 0;
    while(c.next() == 0) {
      rc++;
    }
    c.close();
    session.close(null);
    return rc;
  }

  public static void main( String[] args ) throws Exception {
    init();
    new Ingestor().run();
    log.info("rc {}", rowcount());
    //new Analyst(session).run();
    new TTLMonitor().run();
    cancel = true;
    log.info("rc {}", rowcount());
    conn.close(null);
  }

}
