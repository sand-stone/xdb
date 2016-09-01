package xdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GremlinDB {
  private static Logger log = LogManager.getLogger(GremlinDB.class);
  private static Gson gson = new Gson();

  interface Element {}
  public static class Vertex implements Element {
    public long uid;
    public Map<String, Object> props;

    public Edge link(Vertex end, Map<String, Object> props) {
      return new Edge(this, end, props);
    }

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
      this.start = start;
      this.end = end;
      this.props = props;
    }

    public String toString() {
      return "edge:" + uid + "props:" + gson.toJson(props) + " start:" + start + " end:" + end;
    }

  }

  private String db;
  private final  String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";

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
    session.create("table:tuples", "key_format=QS,value_format=S,columns=(uid,key,value)");
    tuples = session.open_cursor("table:tuples", null, null);
    session.create("index:tuples:index", "columns=(key,value)");
    reversed = session.open_cursor("index:tuples:index(uid)", null, null);
  }

  private long next_uid() {
    uids.putValueByteArray(new byte[]{(byte)0});
    uids.insert();
    return uids.getKeyRecord();
  }

  private void delete(long uid) {
    uids.putKeyRecord(uid);
    uids.search();
    uids.remove();
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
        long tid = tuples.getKeyRecord();
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

  private List<Long> index(String key, Object value) {
    String val = gson.toJson(value);
    reversed.putKeyString(key);
    reversed.putKeyString(val);
    ArrayList<Long> ret  = new ArrayList<Long>();
    if(reversed.search() == 0) {
      do {
        String akey = reversed.getKeyString();
        String aval = reversed.getKeyString();
        String v = value.toString();
        if(key.equals(akey) && v.equals(aval)) {
          ret.add(reversed.getKeyLong());
        }
      } while(reversed.next()==0);
    }
    reversed.reset();
    return ret;    
  }

  private Object key(long uid, String key) {
    Object ret = "{}";
    tuples.putKeyLong(uid);
    tuples.putKeyString(key);
    if(tuples.search() == 0) {
      ret = gson.fromJson(tuples.getValueString(), new HashMap<String,Object>().getClass());
    }
    tuples.reset();
    return ret;
  }

  public Element get(long uid) {
    return null;
  }

  public void save(Element element) {

  }

  public void close() {
    session.close(null);
    conn.close(null);
  }

  
}
