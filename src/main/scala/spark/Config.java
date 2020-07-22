package spark;

import java.util.HashMap;
import java.util.Map;

public class Config {

//    final static String SPARK_HOME = "/home/asherif/programs/spark-2.4.3";
    final static String SPARK_HOME = "/home/asherif/programs/spark-3.0.0";
    final static String SPARK_MASTER = "spark://localhost:7077";
    final static String WAREHOUSE_DIR = "/home/asherif/programs/spark-2.4.3/spark-warehouse";
    final static String WORKING_DIR = "/home/asherif/programs/spark-2.4.3/tmp";
//    final static String APP_RESOURCE = "target/scala-2.11/sparkpersistbenchmark_2.11-0.1.jar";
    final static String APP_RESOURCE = "target/scala-2.12/sparkpersistbenchmark_2.12-0.1.jar";

    static Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();

        config.put("spark.core.max", "12");
        config.put("spark.driver.memory", "2g");
        config.put("spark.executor.memory", "16g");
        config.put("spark.executor.cores", "12");
        config.put("spark.sql.shuffle.partitions", "2000");
        config.put("spark.sql.warehouse.dir", WAREHOUSE_DIR);
        config.put("spark.local.dir", WORKING_DIR);
        //config.put("spark.default.parallelism", "24");
        config.put("spark.memory.fraction", "0.7");
        config.put("spark.memory.offHeap.enabled", "true");
        config.put("spark.memory.offHeap.size", "1106127360");
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
//        config.put("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864");
//        config.put("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "24");
//        config.put("spark.sql.adaptive.coalescePartitions.minPartitionNum", "24");
//        config.put("spark.sql.adaptive.skewJoin.enabled", "true");

//        config.put("spark.sql.cbo.enabled", "true");
//        config.put("spark.sql.cbo.joinReorder.enabled", "true");
//        config.put("spark.sql.cbo.planStats.enabled", "true");
//        config.put("spark.sql.cbo.starSchemaDetection", "true");
//        config.put("spark.sql.cbo.joinReorder.dp.star.filter", "true");

        return config;
    }
}
