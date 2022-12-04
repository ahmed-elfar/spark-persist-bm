package spark;

import sys.Constants;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static spark.Config.*;
import static spark.Config.getConfig;

public class SubmitPython {

    public static void main(String[] args) throws IOException, InterruptedException {
        submitScript("PyArrow", TPCHPersistBaseTableOrderedBykey.class.getName(), 1);
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
                .setAppResource(Constants.HOME_PATH() + "/SparkArrow/driver/Logic.py") // "/my/app.jar"
                //.setMainClass(mainClass) // "my.spark.app.Main";
                .addAppArgs(appArgs);

        getConfig().entrySet().forEach(entry -> spark.setConf(entry.getKey(), entry.getValue()));

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
