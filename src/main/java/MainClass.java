import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainClass {

  public static void main(String[] args) {
    if (args.length != 11) {
      System.out.println("Format \t ::  java.jar <source_host> <source_port> <source_password> <source_db> <dest_host> <dest_port> <dest_password> <dest_db> <scan_size> <scan_pattern> <thread_pool_size>");
      return;
    }

    String Surl = args[0];
    Integer Sport = Integer.valueOf(args[1]);
    String Spassword = args[2];
    Integer SDb = Integer.valueOf(args[3]);

    String Durl = args[4];
    Integer Dport = Integer.valueOf(args[5]);
    String Dpassword = args[6];
    Integer DDb = Integer.valueOf(args[7]);
    Integer ScanSize = Integer.valueOf(args[8]);
    String ScanParam = String.valueOf(args[9]);
    Integer threadPoolSize = Integer.valueOf(args[10]);
    

    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    RedisMigrator redisMigrator = new RedisMigrator(executor);

    redisMigrator.migrationExecutor(Surl,
        Sport,
        Spassword,
        SDb,
        Durl,
        Dport,
        Dpassword,
        DDb,
        ScanSize,
        ScanParam);


  }
}

