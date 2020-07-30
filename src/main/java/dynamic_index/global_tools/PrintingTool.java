package dynamic_index.global_tools;

import java.io.PrintWriter;
import java.util.*;

/**
 * Takes care of printing utilities.
 */
public class PrintingTool {
    public static final String WHITE_SPACE = " ";


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
        for (Object number : collection) {
            tlog.print(number.toString() + WHITE_SPACE);
        }
        tlog.println();
    }

    /**
     * Prints to out
     * @param list - collection of integers.
     */
    public static void printList(List<?> list) {
        for (Object obj : list) {
            System.out.print(obj.toString() + WHITE_SPACE);
        }
        System.out.println();
    }

    public static void printMap(Map<?,?> map){
        for(Map.Entry<?,?> entry: map.entrySet()){
            System.out.print(entry.getKey() + WHITE_SPACE + entry.getValue() + WHITE_SPACE);
        }
        System.out.println();
    }

    public static void printMap(PrintWriter tlog, Map<?,?> map){
        for(Map.Entry<?,?> entry: map.entrySet()){
            tlog.print(entry.getKey() + WHITE_SPACE + entry.getValue() + WHITE_SPACE);
        }
        tlog.println();
    }

}
