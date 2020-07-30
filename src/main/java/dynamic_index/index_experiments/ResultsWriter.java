package dynamic_index.index_experiments;

import dynamic_index.global_tools.PrintingTool;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ResultsWriter {

    public final List<Double> currentElapsedList = new ArrayList<>();

    public void addToElapsedList(long startTime, int averageFactor){
        long endTime = System.currentTimeMillis();
        double elapsedTimeMilliSeconds = (endTime - startTime)/(double)averageFactor;
        currentElapsedList.add(elapsedTimeMilliSeconds);
    }

    public void printResults(String message, PrintWriter tlog){
        tlog.println(message);
        PrintingTool.printList(tlog, currentElapsedList);
    }
}
