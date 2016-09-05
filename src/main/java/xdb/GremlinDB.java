package xdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GremlinDB {
  private static Logger log = LogManager.getLogger(GremlinDB.class);
  private static Gson gson = new Gson();

  interface Element {
    long uid();
  }

  public static class Vertex implements Element {
    public long uid = -1;
    public Map<String, Object> props;

    public Vertex() {
      this.uid = -1;
      this.props = new HashMap<String, Object>();
    }

    public Vertex(long uid, Map<String, Object> props) {
      this.uid = uid;
      this.props = props;
    }

    public Edge link(Vertex end, Map<String, Object> props) {
      return new Edge(this, end, props);
    }

    public long uid() { return uid; }

    public String toString() {
      return "vertex:" + uid + "props:" + gson.toJson(props);
    }
  }

  public static class Edge implements Element {
    public long uid;
    public Map<String, Object> props;
    public Vertex start;
    public Vertex end;

    public Edge(Vertex start, Vertex end, Map<String, Object> props) {
      this(-1, start, end, props);
    }

    public Edge(long uid, Vertex start, Vertex end, Map<String, Object> props) {
      this.uid = uid;
      this.start = start;
      this.end = end;
      this.props = props;
    }

    public long uid() { return uid; }

    public String toString() {
      return "edge:" + uid + "props:" + gson.toJson(props) + " start:" + start + " end:" + end;
    }

  }

  private String db;
  private final  String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";

  private final Integer VERTEX_KIND = 0;
  private final Integer EDGE_KIND = 1;
  private Connection conn;
  private Session session;
  private Cursor uids;
  private Cursor tuples;
  private Cursor reversed;

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

  public GremlinDB(String db) {
    this.db = db;
    init();
  }

  private void init() {
    checkDir(db);
    conn = wiredtiger.open(db, dbconfig);
    session = conn.open_session(null);
    session.create("table:uids", "key_format=r,value_format=u");
    uids = session.open_cursor("table:uids", null, "append");
    session.create("table:tuples", "key_format=qS,value_format=S,columns=(uid,key,value)");
    tuples = session.open_cursor("table:tuples", null, null);
    session.create("index:tuples:index", "columns=(key,value)");
    reversed = session.open_cursor("index:tuples:index(uid)", null, null);
  }

  private long next_uid() {
    uids.putValueByteArray(new byte[]{(byte)0});
    uids.insert();
    uids.next();
    return uids.getKeyRecord();
  }

  private void delete(long uid) {
    uids.putKeyRecord(uid);
    if(uids.search()==0) {
      uids.putKeyRecord(uids.getKeyRecord());
      uids.remove();
    }
    uids.reset();

    tuples.putKeyLong(uid);
    tuples.putKeyString("");
    SearchStatus st = tuples.search_near();
    switch(st) {
    case NOTFOUND:
      tuples.reset();
      break;
    default:
      while(true) {
        long tid = tuples.getKeyLong();
        String key = tuples.getKeyString();
        if(tid == uid) {
          tuples.remove();
          if(tuples.next()!=0) {
            tuples.reset();
            break;
          }
        } {
          tuples.reset();
          break;
        }
      }
      break;
    }
  }

  private void update(long uid, Map<String, Object> props) {
    delete(uid);
    props.forEach((k,v) -> {
        tuples.putKeyLong(uid);
        tuples.putKeyString(k);
        tuples.putValueString(gson.toJson(v));
        tuples.insert();
      }
      );
  }

  private class ElementSpliterator implements  Spliterator.OfLong {
    private String key;
    private Object value;

    public ElementSpliterator(String key, Object value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int characteristics() {
      return DISTINCT | NONNULL | IMMUTABLE;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    public boolean tryAdvance(LongConsumer action) {
      String akey = reversed.getKeyString();
      String aval = reversed.getKeyString();
      String v = value.toString();
      if(key.equals(akey) && v.equals(aval)) {
        action.accept(reversed.getValueLong());
      }
      if(reversed.next()==0)
        return true;
      reversed.reset();
      return false;
    }

    public Spliterator.OfLong trySplit() {
      return null;
    }
  }

  private LongStream index(String key, Object value) {
    String val = gson.toJson(value);
    reversed.putKeyString(key);
    reversed.putKeyString(val);
    if(reversed.search() == 0) {
      return StreamSupport.longStream(new ElementSpliterator(key, value), false);
    }
    reversed.reset();
    return LongStream.empty();
  }

  private Object key(long uid, String key) {
    Object ret = "{}";
    tuples.putKeyLong(uid);
    tuples.putKeyString(key);
    if(tuples.search() == 0) {
      ret = tuples.getValueString();
    }
    tuples.reset();
    return ret;
  }

  public Element get(long uid) {
    Element ret = null;
    tuples.putKeyLong(uid);
    SearchStatus st = tuples.search_near();
    if (st != SearchStatus.NOTFOUND) {
      if(st == SearchStatus.SMALLER) {
        if(tuples.next() != 0)
          return ret;
      }
      Map<String, Object> props = new HashMap<String, Object>();
      do {
        long tid = tuples.getKeyLong();
        String key = tuples.getKeyString();
        if(tid == uid) {
          props.put(key, tuples.getValueString());
        } else
          break;
      } while(tuples.next() == 0);
      int kind = Integer.parseInt((String)props.get("__kind__"));
      if(kind == VERTEX_KIND) {
        ret = new Vertex(uid, props);
      } else {
        long start = Integer.parseInt((String)props.get("__start__"));
        long end = Integer.parseInt((String)props.get("__end__"));
        ret = new Edge(uid, (Vertex)get(start), (Vertex)get(end), props);
      }
    }
    tuples.reset();
    return ret;
  }

  public Element save(Element element) {
    if(element instanceof Vertex) {
      Vertex v = (Vertex)element;
      long uid = v.uid==-1? next_uid() : v.uid;
      v.uid = uid;
      Map<String, Object> props = new HashMap<String, Object>(v.props);
      props.put("__kind__", VERTEX_KIND);
      update(v.uid, props);
    } else {
      Edge e = (Edge)element;
      long uid = e.uid==-1? next_uid() : e.uid;
      e.uid = uid;
      Map<String, Object> props = new HashMap<String, Object>(e.props);
      props.put("__kind__", EDGE_KIND);
      props.put("__start__", e.start.uid == -1? save(e.start).uid() : e.start.uid);
      props.put("__end__", e.end.uid == -1? save(e.end).uid() : e.end.uid);
      update(e.uid, props);
    }
    return element;
  }

  public void close() {
    session.close(null);
    conn.close(null);
  }

  public Stream<Vertex> vertexes() {
    return index("__kind__", VERTEX_KIND).mapToObj(uid -> (Vertex)get(uid));
  }

  public Stream<Edge> edges() {
    return index("__kind__", EDGE_KIND).mapToObj(uid -> (Edge)get(uid));
  }

  public Stream<Edge> incomings(Vertex v) {
    return index("__end__", v.uid()).mapToObj(uid -> (Edge)get(uid));
  }

  public Stream<Edge> outgoings(Vertex v) {
    return index("__start__", v.uid()).mapToObj(uid -> (Edge)get(uid));
  }

  public Vertex start(Edge e) {
    return (Vertex)get((Long)key(e.uid(), "__start__"));
  }

  public Vertex end(Edge e) {
    return (Vertex)get((Long)key(e.uid(), "__end"));
  }

  private static void test1() {
    GremlinDB gdb = new GremlinDB("acme");
    Vertex[] vs = new Vertex[10];
    for(int i = 0; i < vs.length; i++) {
      vs[i] = new Vertex();
      gdb.save(vs[i]);
    }
    gdb.vertexes().forEach(System.out::println);
    gdb.edges();
    gdb.close();
  }

  private static void test2() {
    GremlinDB gdb = new GremlinDB("acme");
    int vc = 10;
    Vertex[] vs = new Vertex[vc];
    for(int i = 0; i < vs.length; i++) {
      vs[i] = new Vertex();
      gdb.save(vs[i]);
    }
    int ec = vc/2;
    Edge[] es = new Edge[ec];
    for(int i = 0; i < ec; i++) {
      Map<String, Object> props = new HashMap();
      props.put("prop1", "somevalue");
      props.put("prop2",i*1000);
      es[i] = new Edge(vs[i], vs[ec+i], props);
      gdb.save(es[i]);
    }
    gdb.vertexes().forEach(System.out::println);
    gdb.edges().forEach(System.out::println);
    gdb.close();
  }

  public static void main(String[] args) {
    test2();
  }

}
