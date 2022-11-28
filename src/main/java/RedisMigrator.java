import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;


public class RedisMigrator {


  Gson gson;
  ExecutorService executorService;


  public RedisMigrator(ExecutorService executor) {
    this.gson = new Gson();
    this.executorService = executor;

  }

  void migrationExecutor(String surl, Integer sport, String spassword, Integer sDb, String durl, Integer dport, String dpassword, Integer dDb, Integer scanSize, String scanParam) {

    long sd = new Date().toInstant().getEpochSecond();

    ConnectionPoolConfig connectionPoolConfig = getConnectionPoolConfig();

    // Source Redis Pool
    JedisPooled Sjedis = new JedisPooled(connectionPoolConfig, surl, sport, 15000, spassword, sDb);

    // Destination Redis Pool
    JedisPooled Djedis = new JedisPooled(connectionPoolConfig, durl, dport, 15000, dpassword, dDb);


    try {


      ScanParams scanParams = new ScanParams();
      scanParams.count(scanSize);
      Long keyCnt = 0L;

      // Use this flag in case you want to test whether this is working.
      Boolean breakAfterFirst = Boolean.FALSE;

      if (!scanParam.equals("*")) {
        scanParams.match(scanParam);
      }


      ScanResult<String> scanResult = Sjedis.scan(ScanParams.SCAN_POINTER_START, scanParams);


      List<Callable<Void>> jobs = new ArrayList<>();

      String cursor = scanResult.getCursor();
      Boolean isCompleted = scanResult.isCompleteIteration();

      keyCnt += scanResult.getResult().size();
      System.out.println(" Keys Processed:\t " + keyCnt);

      jobs.add(() -> {
        pushKeys(scanResult.getResult(), Sjedis, Djedis);
        return null;
      });


      while (!isCompleted && !breakAfterFirst) {

        ScanResult<String> scanResult2 = Sjedis.scan(cursor, scanParams);

        cursor = scanResult2.getCursor();
        isCompleted = scanResult2.isCompleteIteration();
        System.out.println("CursorPosition::" + cursor + " \t isScanCompleted::" + isCompleted);

        jobs.add(() -> {
          pushKeys(scanResult2.getResult(), Sjedis, Djedis);
          return null;
        });

        keyCnt += scanResult2.getResult().size();
        System.out.println(" Keys Processed:\t " + keyCnt);
      }

      executorService.invokeAll(jobs);
      System.out.println("Completed Successfully....");

    } catch (Exception e) {
      System.out.println(e);
    } finally {
      System.out.println("Closing All Open Connections");
      long xx = new Date().toInstant().getEpochSecond() - sd;
      System.out.println("Total Time Taken: " + xx + " Sec");

      executorService.shutdown();
      Sjedis.close();
      Djedis.close();
    }

  }

  private ConnectionPoolConfig getConnectionPoolConfig() {
    ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();
    connectionPoolConfig.setMaxTotal(200);
    connectionPoolConfig.setMaxIdle(50);
    connectionPoolConfig.setMinIdle(5);
    return connectionPoolConfig;
  }

  private void pushKeys(List<String> result, JedisPooled Sjedis, JedisPooled Djedis) {


    System.out.println(Thread.currentThread().getName());
    result.forEach(r -> {
      try {
        Long ttl = Sjedis.ttl(r);
        String type = Sjedis.type(r);
        Object resp;
        switch (type) {
          case "hash":
            Map<String, String> skey = Sjedis.hgetAll(r);
            resp = Djedis.hmset(r, skey);
            if (Math.toIntExact(ttl) > 0) {
              Djedis.expire(r, Math.toIntExact(ttl));
            }
            System.out.println("Key: " + r + "\tStatus: " + resp);
            break;
          case "string":
            String sString = Sjedis.get(r);
            if (Math.toIntExact(ttl) < 0) {
              resp = Djedis.set(r, sString);
            } else {
              resp = Djedis.setex(r, Math.toIntExact(ttl), sString);
            }
            System.out.println("Key: " + r + "\tStatus: " + resp);

            break;
          case "ReJSON-RL":
            Object jsonObj = Sjedis.jsonGet(r);

            JsonElement gJsonObj = gson.toJsonTree(jsonObj);

            resp = Djedis.jsonSet(r, Path.ROOT_PATH, gJsonObj);

            System.out.println("Key: " + r + "\tStatus: " + resp);
            break;
          default:
            System.out.println("Type not Support :: " + Sjedis.type(r) + "Key is :: " + r);
        }
      } catch (Exception e) {
        System.out.println("Key:" + r + "\t Error:" + e);
      }
    });
  }
}