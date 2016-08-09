package xdb;

import jetbrains.exodus.entitystore.*;

public class EntityDB {

  public static void dotest() {
    PersistentEntityStore entityStore = PersistentEntityStores.newInstance("data-entity");
    for (int tx = 0; tx < 10; tx++) {
      System.out.println("tx:" + tx);
      StoreTransaction txn = entityStore.beginTransaction();
      for (int c = 0; c < 10000; c++) {
          final Entity user = txn.newEntity("User#"+tx+"#"+c);
          final String type = user.getType();
          final EntityId id = user.getId();
          user.setProperty("login", "acme");
          user.setProperty("fullName", "John Smith");
          user.setProperty("email", "acme@foo.com");
          user.setProperty("salt", 12345);
          user.setProperty("password", "password");
          final Entity userProfile = txn.newEntity("UserProfile");
          userProfile.setLink("user", user);
          user.setLink("userProfile", userProfile);
          userProfile.setProperty("age", 15);
      }
      if (!txn.flush())
        txn.abort();
    }      
    StoreTransaction txn = entityStore.beginTransaction();
    final EntityIterable allUsers = txn.getAll("User");
    int c = 0;
    for (Entity u: allUsers) {
      c++;
      //System.out.println(u.getProperty("fullName"));
    }
    System.out.println("c:"+ c);
  }

  public static void dotest2() {
    PersistentEntityStore entityStore = PersistentEntityStores.newInstance("data-entity");
    for (int tx = 0; tx < 10; tx++) {
      System.out.println("tx:" + tx);
      for (int c = 0; c < 10000; c++) {
        StoreTransaction txn = null;
        do {
          txn = entityStore.beginTransaction();
          final Entity user = txn.newEntity("User#"+tx+"#"+c);
          final String type = user.getType();
          final EntityId id = user.getId();
          user.setProperty("login", "acme");
          user.setProperty("fullName", "John Smith");
          user.setProperty("email", "acme@foo.com");
          user.setProperty("salt", 12345);
          user.setProperty("password", "password");
          final Entity userProfile = txn.newEntity("UserProfile");
          userProfile.setLink("user", user);
          user.setLink("userProfile", userProfile);
          userProfile.setProperty("age", 15);
          if (txn != entityStore.getCurrentTransaction()) {
            txn = null;
            break;
          }
        } while (!txn.flush());
        if (txn != null) {
          txn.abort();
        }
      }
    }
    StoreTransaction txn = entityStore.beginTransaction();
    final EntityIterable allUsers = txn.getAll("User");
    for (Entity u: allUsers) {
      System.out.println(u.getProperty("fullName"));
    }
  }

  public static void main(String[] args) {
    dotest();
  }

}
