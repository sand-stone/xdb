package xdb;

import java.io.*;
import io.atomix.*;
import io.atomix.catalyst.transport.*;
import io.atomix.copycat.server.storage.*;
import io.atomix.catalyst.transport.netty.*;
import io.atomix.group.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;

public class DistributedStore {
  private static Logger log = LogManager.getLogger(DistributedStore.class);
  private final static int PORT = 7000;
  AtomixReplica cluster;


  public DistributedStore(String host) {
    Address address = new Address(host, PORT);
    cluster = AtomixReplica.builder(address)
      .withTransport(new NettyTransport())
      .withStorage(Storage.builder()
                   .withDirectory(new File("logs"))
                   .withStorageLevel(StorageLevel.MAPPED)
                   .withMaxSegmentSize(1024 * 1024)
                   .build())
      .build();
    cluster.bootstrap().join();
  }

  AtomixReplica getCluster() {
    return cluster;
  }

  public static AtomixClient newClient() {
    AtomixClient client = AtomixClient.builder()
      .withTransport(new NettyTransport())
      .build();
    List<Address> cluster = Arrays.asList(
                                          new Address("localhost", 7000),
                                          new Address("localhost", 7001),
                                          new Address("localhost", 7002));
    client.connect(cluster).thenRun(() -> {
        log.info("Client connected!");
      });
    return client;
  }

  static int replicaPort = 7001;
  public static AtomixReplica newReplica() {
    log.info("create new replica");
    AtomixReplica replica = AtomixReplica.builder(new Address("localhost", replicaPort++))
      .withTransport(new NettyTransport())
      .build();
    replica.join(new Address("localhost", PORT)).join();
    return replica;
  }

  public static void main(String[] args) {
    log.info("bootstrap");
    DistributedStore store = new DistributedStore("localhost");
    log.info("starting");
    //AtomixReplica replica2 = newReplica();
    //log.info("joined {}", replica2);
    //AtomixReplica replica3 = newReplica();
    //log.info("joined {}", replica3);

    AtomixClient client = newClient();
    AtomixClient client2 = newClient();

    try {
      Thread.currentThread().sleep(2000);
    } catch(InterruptedException e) {}

    DistributedGroup group = client.getGroup("my-group").join();
    log.info("joined a group {}", group);
    try {
      log.info("emnumerate membership");
      group = client.getGroup("my-group").get();
      log.info("emnumerate membership {}", group);
      for (GroupMember member : group.members()) {
        log.info("member {}", member);
      }
    } catch(ExecutionException e) {
    } catch(InterruptedException e) {}
  }

}
