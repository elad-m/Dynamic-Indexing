package dynamic_index.index_experiments;

import dynamic_index.global_tools.PrintingTool;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ResultsWriter {

    private final List<Double> elapsedQueryTimeList = new ArrayList<>();
    private final List<Integer> elapsedConstructionTimeList = new ArrayList<>();

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

    public void printResults(String message, PrintWriter tlog){
        tlog.println(message);
        tlog.print("Average Query: ");
        PrintingTool.printList(tlog, elapsedQueryTimeList);
        tlog.print("Construction time: ");
        PrintingTool.printList(tlog, elapsedConstructionTimeList);
    }
}
