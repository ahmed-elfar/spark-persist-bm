package spark;


import datagen.TestSelectSingleColumn;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import spark.conf.DefaultPropertiesReader;
import tpch.TPCHParquetLocal;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static spark.Config2.*;
//import static spark.Config.*;

public class SparkSubmit {

    public static void main(String[] args) throws Exception {

        if(args.length == 2){
            Config2.getConfig().putAll(new DefaultPropertiesReader(args[1]).getProperties());
        }

        if(args.length != 0) {
            switch (Integer.valueOf(args[0])) {
                case 1: {
                    submitScript("TPCH Original", TPCHOriginal.class.getName(), 1);
                    break;
                }
                case 2: {
                    submitScript("TPCH Persist base Tables Memory", TPCHPersistBaseTable.class.getName(), 1, "memory");
                    break;
                }
                case 3: {
                    submitScript("TPCH Persist base Tables Memory", TPCHPersistBaseTable.class.getName(), 1, "disk");
                    break;
                }
                case 4: {
                    submitScript("Shuffled Sequential Queries", ShuffledSequentialQueries.class.getName(), 1, args[2]);
                    break;
                } case 5: {
                    submitScript("Concurrent Queries", ConcurrentQueries.class.getName(), 1, args[2]);
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown run ID : " + args[0]);
            }
        }

        //submitScript("TPCH Parquet Local", TPCHParquetLocal.class.getName(), 1);

        //submitScript("TPCH Original", TPCHOriginal.class.getName(), 1);

        //submitScript("TPCH Persist base Tables Disk", TPCHPersistBaseTable.class.getName(), 1, "disk");
        //submitScript("TPCH Persist base Tables Memory", TPCHPersistBaseTable.class.getName(), 1, "memory");
        //submitScript("TPCH Persist base Tables Off Heap", TPCHPersistBaseTable.class.getName(), 1, "offHeap");

        //submitScript("TPCH Persist base Tables Catalog Memory", CachingTablesUsingCatalog.class.getName(), 1, "memory");

        //submitScript("TPCH Persist Result Memory", TPCHPersistingResults.class.getName(), 1, "memory");
        //submitScript("TPCH Persist Result Disk", TPCHPersistingResults.class.getName(), 1, "disk");

        //submitScript("Test select single column", TestSelectSingleColumn.class.getName(), 1, "false");
        //submitScript("Test select single column", TestSelectSingleColumn.class.getName(), 1, "true");

        //submitScript("Test select all columns", TestSelectAllColumn.class.getName(), 1, "false");
        //submitScript("Test select all columns", TestSelectAllColumn.class.getName(), 1, "true");

        //submitScript("TPCH Persist base Tables ordered by key", TPCHPersistBaseTableOrderedBykey.class.getName(), 1);

        //submitScript("TPCH Hive Support", HiveTPCH.class.getName(), 1);
    }

    public static double[] submitScript(String appName, String mainClass, int runs, String... appArgs) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        System.out.println("RUNNING " + appName);
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        SparkLauncher spark = new SparkLauncher()
                .setVerbose(true)
                .setAppName(appName)
                .setMaster(SPARK_MASTER)
                .setSparkHome(SPARK_HOME)
                .setAppResource(APP_RESOURCE) // "/my/app.jar"
                .setMainClass(mainClass) // "my.spark.app.Main";
                .addAppArgs(appArgs);

        Config2.getConfig().entrySet().forEach(entry -> spark.setConf(entry.getKey(), entry.getValue()));

        CountDownLatch latch = new CountDownLatch(2);
        final long endSubmit[] = {0};

        SparkAppHandle sparkAppHandle = spark.startApplication(new SparkAppHandle.Listener() {

            public void stateChanged(SparkAppHandle handle) {
                if (handle.getState() == SparkAppHandle.State.RUNNING) {
                    endSubmit[0] = System.currentTimeMillis();
                    latch.countDown();
                } else if (handle.getState() == SparkAppHandle.State.FINISHED) {
                    latch.countDown();
                } else if (handle.getState() == SparkAppHandle.State.FAILED
                        || handle.getState() == SparkAppHandle.State.KILLED
                        || handle.getState() == SparkAppHandle.State.LOST) {

                    for (long count = latch.getCount(); count > 0; count--)
                        latch.countDown();
                }
                System.out.println("Current App State: " + handle.getState());
            }

            @Override
            public void infoChanged(SparkAppHandle handle) {
            }
        });

        latch.await();

        double submitTime = (endSubmit[0] - startTime) / 1000.0;

        long endTime = System.currentTimeMillis();
        sparkAppHandle.kill();
        double executionTime = (endTime - endSubmit[0]) / 1000.0;
        double totalTime = (endTime - startTime) / 1000.0;
        System.out.println("TIME FOR " + appName);
        System.out.println("\nSubmitTime \t= " + submitTime + " sec");
        System.out.println("ExcTime \t= " + executionTime + " sec");
        System.out.println("Total Time \t= " + totalTime + " sec\n");

        return new double[]{submitTime, executionTime, totalTime};
    }

}


