package dynamic_index.global_tools;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Takes care of printing utilities.
 */
public class PrintingTool {


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

    /**
     * Prints to tlog and System.out the elapsed time from start time, and divide by averageFactor (should be 1
     * for no average)
     *
     * @param tlog          - antoher print writer
     * @param startTime     - should be taken with System.currentTimeMillis() before calling this method.
     * @param methodName    - the method or action that its elapsed time is being measured.
     */
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


    /**
     * Prints to tlog a collection
     *
     * @param tlog       - print writer to print to
     * @param collection - collection of integers.
     */
    public static void printList(PrintWriter tlog, Collection<?> collection) {
        tlog.print("[");
        for (Object number : collection) {
            tlog.print(number.toString() + ",");
        }
        tlog.println("]");
    }

    /**
     * Prints to out
     * @param list - collection of integers.
     */
    public static void printList(List<?> list) {
        System.out.print("[");
        for (Object obj : list) {
            System.out.print(obj.toString() + ",");
        }
        System.out.println("]");
    }
}
