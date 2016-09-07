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

public class SimpleGuidTiger {
  private static Logger log = LoggerFactory.getLogger(SimpleGuidTiger.class);
  
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
  
  public static void main( String[] args ) throws Exception {
    String guids = "./guids";
    checkDir(guids);
    Connection conn =  wiredtiger.open("./guids", dbconfig);
    System.out.println("conn:"+conn);
    Session session = conn.open_session(null);
    session.create("table:acme", "type=lsm,key_format=u,value_format=u");
    //session.create("table:acme", "key_format=u,value_format=u");

    int count = 2000; int batch = 1000;
    Cursor c = session.open_cursor("table:acme", null, null);
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
