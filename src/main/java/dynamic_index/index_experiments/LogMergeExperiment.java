package dynamic_index.index_experiments;

import dynamic_index.LogMergeIndexWriter;
import dynamic_index.IndexReader;
import dynamic_index.global_tools.PrintingTool;
import dynamic_index.global_tools.MiscTools;

import java.io.File;
import java.util.Enumeration;


public class LogMergeExperiment extends Experiment{


    public LogMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.LOG_MERGE_INDEXES_DIR_NAME,
                inputScale,
                true);
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Log Merge ");

        LogMergeIndexWriter logMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory, true);
        queryAfterBuildIndex(logMergeIndexWriter, indexReader);

        deleteReviews(logMergeIndexWriter);
        queryAfterDelete(logMergeIndexWriter, indexReader);

    }

    private void deleteReviews(LogMergeIndexWriter logMergeIndexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        logMergeIndexWriter.removeReviews(allIndexesDirectory, scalingCases.getDelReviews());
    }

    private void queryAfterDelete(LogMergeIndexWriter logMergeIndexWriter, IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testGetReviewsWithToken(logMergeIndexWriter, indexReader, scalingCases.getWordQueries());
    }

    private LogMergeIndexWriter buildIndex(){
        long startTime = System.currentTimeMillis();
        LogMergeIndexWriter logMergeIndexWriter = new LogMergeIndexWriter(allIndexesDirectory);
        logMergeIndexWriter.construct(scalingCases.getInputFilename());
        PrintingTool.printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction: Log-Merged");
        return logMergeIndexWriter;
    }

    private void queryAfterBuildIndex(LogMergeIndexWriter logMergeIndexWriter, IndexReader indexReader) {
        tlog.println("===== words in index queries... =====");
        testGetReviewsWithToken(logMergeIndexWriter, indexReader, scalingCases.getWordQueries());
    }

    private void testGetReviewsWithToken(LogMergeIndexWriter logMergeIndexWriter, IndexReader indexReader, String[] wordQueries) {
        for (String word : wordQueries) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word, logMergeIndexWriter);
            tlog.print(word + ": " + System.lineSeparator());
            printEnumeration(res);
        }
    }
}
