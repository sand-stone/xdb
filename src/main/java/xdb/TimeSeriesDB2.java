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

    public Ingestor(Session session) {
      this.session = session;
      batch = 10;
    }

    public void run() {
      //EventFactory producer = new EventFactory(100000000, 1000);
      log.info("ingestor starts");
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
    }

  }

  public static class TTLMonitor implements Runnable {
    Session session;

    public TTLMonitor(Session session) {
      this.session = session;
    }

    public void run() {
      int interval = 1;
      while(!stop) {
        try {Thread.currentThread().sleep(interval*1000);} catch(Exception ex) {}
        log.info("TTL scanning starts {}", session);
        Cursor c = session.open_cursor(table, null, null);
        log.info("TTL scanning starts");
        int nd = 0;
        session.begin_transaction(tnx);
        long past = past(interval);
        c.putKeyLong(past);
        SearchStatus status = c.search_near();
        switch(status) {
        case LARGER:
        case NOTFOUND:
          log.info("no data points to remove");
          break;
        case FOUND:
        case SMALLER:
          do {
            nd++;
            Cursor uc = session.open_cursor(null, c, null);
            uc.putKeyLong(c.getKeyLong());
            uc.putKeyString(c.getKeyString());
            uc.putKeyString(c.getKeyString());
            uc.remove();
          } while(c.prev() == 0);
          break;
        }
        session.commit_transaction(null);
        c.close();
        log.info("TTL scanning ends nd {}", nd);
      }
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
      while(!stop) {
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

  private static boolean stop = false;
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

  private static Session init() {
    checkDir(db);
    conn =  wiredtiger.open(db, "create");
    Session session = conn.open_session(null);
    session.create(table, storage);
    return session;
  }

  private static Connection conn;

  public static void main( String[] args ) throws Exception {
    Session session = init();
    new Ingestor(session).run();
    //new Analyst(session).run();
    new TTLMonitor(session).run();
    conn.close(null);
  }

}
