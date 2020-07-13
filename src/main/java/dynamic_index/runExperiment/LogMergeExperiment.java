package dynamic_index.runExperiment;

import dynamic_index.ContinuousIndexWriter;
import dynamic_index.IndexReader;
import dynamic_index.Statics;

import java.io.File;
import java.util.Enumeration;

import static dynamic_index.Statics.printElapsedTimeToLog;

public class LogMergeExperiment extends Experiment{


    public LogMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + Statics.LOG_MERGE_INDEXES_DIR_NAME,
                inputScale,
                true);
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Log Merge ");

        ContinuousIndexWriter continuousIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(indexDirectory, true);
        queryAfterBuildIndex(continuousIndexWriter, indexReader);

        deleteReviews(continuousIndexWriter);
        queryAfterDelete(continuousIndexWriter, indexReader);

    }

    private void deleteReviews(ContinuousIndexWriter continuousIndexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        continuousIndexWriter.removeReviews(indexDirectory, scalingCases.getDelReviews());
    }

    private void queryAfterDelete(ContinuousIndexWriter continuousIndexWriter, IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testGetReviewsWithToken(continuousIndexWriter, indexReader, scalingCases.getWordQueries());
    }

    private ContinuousIndexWriter buildIndex(){
        long startTime = System.currentTimeMillis();
        ContinuousIndexWriter continuousIndexWriter = new ContinuousIndexWriter();
        continuousIndexWriter.construct(scalingCases.getInputFilename(), indexDirectory);
        printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction: Log-Merged");
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
