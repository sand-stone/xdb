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

public class SimpleGuidTiger {
  private static Logger log = LoggerFactory.getLogger(SimpleGuidTiger.class);

  public static void main( String[] args ) throws Exception {
    Connection conn =  wiredtiger.open("./guids", "create");
    System.out.println("conn:"+conn);
    Session session = conn.open_session(null);
    session.create("table:acme", "type=lsm,key_format=u,value_format=u");
    //session.create("table:acme", "key_format=u,value_format=u");

    int count = 1000000000; int batch = 10;
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
      if(i%100000 == 0) {
        System.out.println("remaining count "+(count-i));
      }
    }
    c.close();
    conn.close(null);
  }

}
