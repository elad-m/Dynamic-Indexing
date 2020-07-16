package dynamic_index.global_tools;

import java.io.PrintWriter;

/**
 * Takes care of printing utilities.
 */
public final class PrintingTool {

    //=========================  Printing  =====================================//

    public static void printElapsedTime(long startTime, String methodName) {
        long endTime = System.currentTimeMillis();
        long elapsedTimeMilliSeconds = endTime - startTime;
        long elapsedTimeSeconds = elapsedTimeMilliSeconds / 1000;
        long elapsedTimeMinutes = elapsedTimeSeconds / 60;
        System.out.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);
    }

    public static void printElapsedTimeToLog(PrintWriter tlog, long startTime, String methodName) {
        long endTime = System.currentTimeMillis();
        long elapsedTimeMilliSeconds = endTime - startTime;
        long elapsedTimeSeconds = elapsedTimeMilliSeconds / 1000;
        long elapsedTimeMinutes = elapsedTimeSeconds / 60;
        System.out.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);

        tlog.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);
    }
}
