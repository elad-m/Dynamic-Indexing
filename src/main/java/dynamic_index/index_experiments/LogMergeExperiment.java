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

        indexReader = deleteReviews(logMergeIndexWriter);
        queryAfterDelete(logMergeIndexWriter, indexReader);

    }

    private IndexReader deleteReviews(LogMergeIndexWriter logMergeIndexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        logMergeIndexWriter.removeReviews(allIndexesDirectory, scalingCases.getDelReviews());
        return new IndexReader(allIndexesDirectory, true);
    }

    private void queryAfterDelete(LogMergeIndexWriter logMergeIndexWriter, IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testWordQueries(logMergeIndexWriter, indexReader, scalingCases.getWordQueries());
        testMetaData(indexReader);
        testReviewMetaData(indexReader);
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
        testWordQueries(logMergeIndexWriter, indexReader, scalingCases.getWordQueries());
        testMetaData(indexReader);
        testReviewMetaData(indexReader);
    }

    private void testWordQueries(LogMergeIndexWriter logMergeIndexWriter,
                                 IndexReader indexReader,
                                 String[] wordTestCases) {
        for (String word : wordTestCases) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word, logMergeIndexWriter);
            tlog.println(word + ":");
            printEnumeration(res);
            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word, logMergeIndexWriter));
            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word, logMergeIndexWriter));
        }
    }

    private void testMetaData(IndexReader indexReader){
        tlog.println("#Reviews: " + indexReader.getNumberOfReviews());
        tlog.println("#Tokens: " + indexReader.getTotalNumberOfTokens());
    }

    private void testReviewMetaData(IndexReader indexReader) {
        for (int rid : scalingCases.getMetaRev()) {
            tlog.println("rid: " + rid + " " +
                    indexReader.getProductId(rid) + " " +
                    indexReader.getReviewScore(rid) + " " +
                    indexReader.getReviewHelpfulnessNumerator(rid) + " " +
                    indexReader.getReviewHelpfulnessDenominator(rid) + " " +
                    indexReader.getReviewLength(rid));
        }
    }


}
