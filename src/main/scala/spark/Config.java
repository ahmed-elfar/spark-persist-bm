package spark;

import sys.Constants;

import java.util.HashMap;
import java.util.Map;

public class Config {

//    final static String SPARK_HOME = Constants.HOME_PATH + "/spark-2.4.3";
    final static String SPARK_HOME = Constants.HOME_PATH() + "/spark-3.0.0";
    final static String SPARK_MASTER = "spark://localhost:7077";
    final static String WAREHOUSE_DIR = Constants.HOME_PATH() + "/spark-2.4.3/spark-warehouse";
    final static String WORKING_DIR = Constants.HOME_PATH() + "/spark-2.4.3/tmp";
//    final static String APP_RESOURCE = "target/scala-2.11/sparkpersistbenchmark_2.11-0.1.jar";
    //final static String APP_RESOURCE = "target/scala-2.12/sparkpersistbenchmark_2.12-0.1.jar";
    final static String APP_RESOURCE = "./SparkPersistBenchMark-assembly-0.1.jar";

    static Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();

        config.put("spark.cores.max", "10");
        config.put("spark.driver.memory", "3g");
        config.put("spark.executor.memory", "4g");
        config.put("spark.executor.cores", "2");
        config.put("spark.sql.shuffle.partitions", "20");
        config.put("spark.sql.warehouse.dir", WAREHOUSE_DIR);
        config.put("spark.local.dir", WORKING_DIR);
        config.put("spark.default.parallelism", "20");
        config.put("spark.memory.fraction", "0.8");

        config.put("spark.executor.extraJavaOptions",
                "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=25 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'");


//        config.put("spark.dynamicAllocation.enabled", "true");
//        config.put("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60");
//        config.put("spark.dynamicAllocation.initialExecutors", "3");
//        config.put("spark.dynamicAllocation.shuffleTracking.enabled", "true");
//        config.put("spark.dynamicAllocation.maxExecutors", "3");
//
//        config.put("spark.driver.allowMultipleContexts", "true");



        //config.put("spark.memory.offHeap.enabled", "true");
        //config.put("spark.memory.offHeap.size", "1106127360");
        //config.put("spark.memory.offHeap.size", "16106127360");
        //config.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //config.put("spark.sql.autoBroadcastJoinThreshold", "-1");

        //3.0.0
//        config.put("spark.sql.adaptive.enabled", "true");
//        config.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
//
//        config.put("spark.sql.adaptive.localShuffleReader.enabled", "true");
//        config.put("spark.sql.adaptive.skewJoin.enabled", "true");
//
//        config.put("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64");
//        config.put("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "12");
//        config.put("spark.sql.adaptive.coalescePartitions.minPartitionNum", "12");
//        config.put("spark.sql.adaptive.skewJoin.enabled", "true");

//        config.put("spark.sql.cbo.enabled", "true");
//        config.put("spark.sql.cbo.joinReorder.enabled", "true");
//        config.put("spark.sql.cbo.planStats.enabled", "true");
//        config.put("spark.sql.cbo.starSchemaDetection", "true");
//        config.put("spark.sql.cbo.joinReorder.dp.star.filter", "true");

//        config.put("spark.sql.hive.metastore.version", "2.3.7");


        return config;
    }
}
