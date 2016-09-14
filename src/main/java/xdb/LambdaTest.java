package xdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class LambdaTest {
  private static Logger log = LoggerFactory.getLogger(LambdaTest.class);

  public interface SerializableFunction<I, O> extends Function<I, O>, Serializable {
    SerializableFunction<?, Void> RETURN_NOTHING = i -> null;
  }

  public interface SerializableUpdater<U> extends Consumer<U>, Serializable {
  }

  public final static class Serializer {
    private static Logger log = LoggerFactory.getLogger(Serializer.class);

    private Serializer() {
    }

    public static ByteBuffer serialize(Object msg) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(msg);
        oos.close();
        return ByteBuffer.wrap(bos.toByteArray());
      }
    }

    public static Object deserialize(ByteBuffer bb) {
      byte[] bytes = new byte[bb.remaining()];
      bb.get(bytes);
      try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
           ObjectInputStream ois = new ObjectInputStream(bis)) {
        return ois.readObject();
      } catch (ClassNotFoundException|IOException ex) {
        log.error("Failed to deserialize: {}", bb, ex);
        throw new RuntimeException("Failed to deserialize ByteBuffer");
      }
    }

  }

  public static class WireSerializedLambda {
    private String capturingClass;
    private String functionalInterfaceClass;
    private String functionalInterfaceMethodName;
    private String functionalInterfaceMethodSignature;
    private String implClass;
    private String implMethodName;
    private String implMethodSignature;
    private int implMethodKind;
    private String instantiatedMethodType;
    private List<Object> capturedArgs = new ArrayList<>();

    public static boolean isSerializableLambda(Class clazz) {
      return Serializable.class.isAssignableFrom(clazz) && clazz.getName().contains("$Lambda$");
    }

    public static <Lambda> WireSerializedLambda get(Lambda lambda) {
      try {
        Method writeReplace = lambda.getClass().getDeclaredMethod("writeReplace");
        writeReplace.setAccessible(true);
        SerializedLambda sl = (SerializedLambda) writeReplace.invoke(lambda);
        WireSerializedLambda wsl = new WireSerializedLambda();
        wsl.capturingClass = sl.getCapturingClass().replace('/', '.');
        wsl.functionalInterfaceClass = sl.getFunctionalInterfaceClass();
        wsl.functionalInterfaceMethodName = sl.getFunctionalInterfaceMethodName();
        wsl.functionalInterfaceMethodSignature = sl.getFunctionalInterfaceMethodSignature();
        wsl.implMethodKind = sl.getImplMethodKind();
        wsl.implClass = sl.getImplClass();
        wsl.implMethodName = sl.getImplMethodName();
        wsl.implMethodSignature = sl.getImplMethodSignature();
        wsl.instantiatedMethodType = sl.getInstantiatedMethodType();
        for (int i = 0; i < sl.getCapturedArgCount(); i++) {
          wsl.capturedArgs.add(sl.getCapturedArg(i));
        }
        return wsl;
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    public static <Lambda> ByteBuffer write(Lambda lambda) {
      try {
        WireSerializedLambda wsl = get(lambda);
        List<Object> objs = new ArrayList<Object>();
        objs.add(wsl.capturingClass);
        objs.add(wsl.functionalInterfaceClass);
        objs.add(wsl.functionalInterfaceMethodName);
        objs.add(wsl.functionalInterfaceMethodSignature);
        objs.add(wsl.implMethodKind);
        objs.add(wsl.implClass);
        objs.add(wsl.implMethodName);
        objs.add(wsl.implMethodSignature);
        objs.add(wsl.instantiatedMethodType); 
        objs.addAll(wsl.capturedArgs);
        return Serializer.serialize(objs);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    public static WireSerializedLambda read(ByteBuffer wire) throws IllegalStateException {
      List<Object> objs = (List<Object>)Serializer.deserialize(wire);
      WireSerializedLambda wsl = new WireSerializedLambda();
      wsl.capturingClass = (String)objs.remove(0);
      wsl.functionalInterfaceClass = (String)objs.remove(0);
      wsl.functionalInterfaceMethodName = (String)objs.remove(0);
      wsl.functionalInterfaceMethodSignature = (String)objs.remove(0);
      wsl.implMethodKind = (Integer)objs.remove(0);
      wsl.implClass = (String)objs.remove(0);
      wsl.implMethodName = (String)objs.remove(0);
      wsl.implMethodSignature = (String)objs.remove(0);
      wsl.instantiatedMethodType = (String)objs.remove(0);
      while(objs.size()>0) {
        wsl.capturedArgs.add(objs.remove(0));
      }
      return wsl;
    }

    public Object readResolve() {
      try {
        SerializedLambda sl = new SerializedLambda(Class.forName(capturingClass), functionalInterfaceClass,
                                                   functionalInterfaceMethodName, functionalInterfaceMethodSignature,
                                                   implMethodKind, implClass, implMethodName, implMethodSignature,
                                                   instantiatedMethodType, capturedArgs.toArray());
        Method readReplace = SerializedLambda.class.getDeclaredMethod("readResolve");
        readReplace.setAccessible(true);
        return readReplace.invoke(sl);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static void test1() {
    Function<String, String> fun = (Function<String, String> & Serializable) String::toUpperCase;
    log.info("fun: {}", WireSerializedLambda.isSerializableLambda(fun.getClass()));
    SerializableUpdater<AtomicInteger> upd = AtomicInteger::incrementAndGet;
    int a = 5;
    SerializableFunction<Integer, Integer> fun2 = i -> i + a;
    WireSerializedLambda wsl = WireSerializedLambda.get(fun2);
    ByteBuffer bytes = WireSerializedLambda.write(fun2);
    bytes.rewind();
    WireSerializedLambda x = WireSerializedLambda.read(bytes);
    log.info("x {}", ((Function<Integer, Integer>)x.readResolve()).apply(10000));
  }

  public static void main( String[] args ) throws Exception {
    test1();
  }

}
