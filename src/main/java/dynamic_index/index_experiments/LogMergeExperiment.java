package dynamic_index.index_experiments;

import dynamic_index.IndexRemover;
import dynamic_index.LogMergeIndexWriter;
import dynamic_index.IndexReader;
import dynamic_index.global_tools.PrintingTool;
import dynamic_index.global_tools.MiscTools;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicLong;

import static dynamic_index.global_tools.MiscTools.TERM_MAP_FILE_DEBUG;
import static dynamic_index.global_tools.MiscTools.getRandomWords;


public class LogMergeExperiment extends Experiment{


    private final int TEMP_INDEX_SIZE;
    public LogMergeExperiment(String localDir, int inputScale, int tempIndexSize) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.LOG_MERGE_INDEXES_DIR_NAME,
                inputScale,
                true);
        TEMP_INDEX_SIZE = tempIndexSize;
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Log Merge ");

        LogMergeIndexWriter logMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory, true);
        queryAfterBuildIndex(logMergeIndexWriter, indexReader);

//        indexReader = deleteReviews(logMergeIndexWriter);
//        queryAfterDelete(logMergeIndexWriter, indexReader);
//        runRandomQueries(logMergeIndexWriter, indexReader, 50);
        long indexSize = getAllIndexSize((new File (allIndexesDirectory)).toPath());
        tlog.println("total index size: " + indexSize);

        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeAllIndexFiles(allIndexesDirectory);

        tlog.close();
    }

    public static long getAllIndexSize(Path path) {

        final AtomicLong size = new AtomicLong(0);

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {

                    System.out.println("skipped: " + file + " (" + exc + ")");
                    // Skip folders that can't be traversed
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {

                    if (exc != null)
                        System.out.println("had trouble traversing: " + dir + " (" + exc + ")");
                    // Ignore errors traversing a folder
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new AssertionError("walkFileTree will not throw IOException if the FileVisitor does not");
        }

        return size.get();
    }



    private LogMergeIndexWriter buildIndex(){
        tlog.println("temporary index size: " + TEMP_INDEX_SIZE);
        long startTime = System.currentTimeMillis();
        LogMergeIndexWriter logMergeIndexWriter = new LogMergeIndexWriter(allIndexesDirectory,
                TEMP_INDEX_SIZE);
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


    private void runRandomQueries(LogMergeIndexWriter logMergeIndexWriter, IndexReader indexReader, int numOfWords) {
        tlog.println(numOfWords + " random queries...");
        String[] randomWords = getRandomWords(allIndexesDirectory, numOfWords);
        long startReviewsWithToken = System.currentTimeMillis();
        testGetReviewsWithToken(logMergeIndexWriter, indexReader, randomWords);
        PrintingTool.printElapsedTimeToLog(tlog,
                startReviewsWithToken,
                numOfWords + " random getReviewsWithToken");

        tlog.close();
    }


    private void testGetReviewsWithToken(LogMergeIndexWriter logMergeIndexWriter, IndexReader indexReader, String[] words){
        for (String word : words) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word, logMergeIndexWriter);
            tlog.println(word + ":");
            printEnumeration(res);
        }
    }

}
