package dynamic_index.index_experiments;

import dynamic_index.global_tools.PrintingTool;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Gathers results of construction time, average query time and size on disk.
 */
public class ResultsWriter {

    private final List<Double> elapsedQueryTimeList = new ArrayList<>();
    private final List<Integer> elapsedConstructionTimeList = new ArrayList<>();
    private final List<Long> totalIndexDiskSize = new ArrayList<>();
    private final List<Integer> numberOfIndexes = new ArrayList<>();

    public void addToElapsedQueryTimeList(long startTime, int averageFactor){
        long endTime = System.currentTimeMillis();
        double elapsedTimeMilliSeconds = (endTime - startTime)/(double)averageFactor;
        elapsedQueryTimeList.add(elapsedTimeMilliSeconds);
    }

    public void addToElapsedConstructionTimeList(long startTime){
        long endTime = System.currentTimeMillis();
        int elapsedTimeMilliSeconds = (int)(endTime - startTime);
        elapsedConstructionTimeList.add(elapsedTimeMilliSeconds);
    }

    public void addToIndexDiskSize(long allIndexSize) {
        totalIndexDiskSize.add(allIndexSize);
    }

    public void addToNumberOfIndexes(int numberOfIndexes){
        this.numberOfIndexes.add(numberOfIndexes);
    }

    public void printResults(String message, PrintWriter tlog){
        tlog.println(message);
        tlog.print("Average Query: ");
        PrintingTool.printList(tlog, elapsedQueryTimeList);
        tlog.print("Construction time: ");
        PrintingTool.printList(tlog, elapsedConstructionTimeList);
        tlog.print("Total index disk size:");
        PrintingTool.printList(tlog, totalIndexDiskSize);
        tlog.print("Number of indexes:");
        PrintingTool.printList(tlog, numberOfIndexes);
    }
}
