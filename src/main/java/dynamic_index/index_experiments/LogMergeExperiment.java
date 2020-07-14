package dynamic_index.index_experiments;

import dynamic_index.ContinuousIndexWriter;
import dynamic_index.IndexReader;
import dynamic_index.global_util.PrintingUtil;
import dynamic_index.global_util.MiscUtils;

import java.io.File;
import java.util.Enumeration;


public class LogMergeExperiment extends Experiment{


    public LogMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + MiscUtils.LOG_MERGE_INDEXES_DIR_NAME,
                inputScale,
                true);
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Log Merge ");

        ContinuousIndexWriter continuousIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory, true);
        queryAfterBuildIndex(continuousIndexWriter, indexReader);

        deleteReviews(continuousIndexWriter);
        queryAfterDelete(continuousIndexWriter, indexReader);

    }

    private void deleteReviews(ContinuousIndexWriter continuousIndexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        continuousIndexWriter.removeReviews(allIndexesDirectory, scalingCases.getDelReviews());
    }

    private void queryAfterDelete(ContinuousIndexWriter continuousIndexWriter, IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testGetReviewsWithToken(continuousIndexWriter, indexReader, scalingCases.getWordQueries());
    }

    private ContinuousIndexWriter buildIndex(){
        long startTime = System.currentTimeMillis();
        ContinuousIndexWriter continuousIndexWriter = new ContinuousIndexWriter();
        continuousIndexWriter.construct(scalingCases.getInputFilename(), allIndexesDirectory);
        PrintingUtil.printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction: Log-Merged");
        return continuousIndexWriter;
    }

    private void queryAfterBuildIndex(ContinuousIndexWriter continuousIndexWriter, IndexReader indexReader) {
        tlog.println("===== words in index queries... =====");
        testGetReviewsWithToken(continuousIndexWriter, indexReader, scalingCases.getWordQueries());
        tlog.println("===== words not in index queries... =====");
        testGetReviewsWithToken(continuousIndexWriter, indexReader, scalingCases.getNegWordQueries());
    }

    private void testGetReviewsWithToken(ContinuousIndexWriter continuousIndexWriter, IndexReader indexReader, String[] wordQueries) {
        for (String word : wordQueries) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word, continuousIndexWriter);
            tlog.print(word + ": " + System.lineSeparator());
            printEnumeration(res);
        }
    }
}
