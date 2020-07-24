package dynamic_index.index_experiments;

import dynamic_index.*;
import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.PrintingTool;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.List;

import static dynamic_index.global_tools.MiscTools.ENTIRE_INSERTIONS_MESSAGE;
import static dynamic_index.global_tools.MiscTools.SINGLE_INSERTION_MESSAGE;


abstract public class Experiment {

    protected final int NUMBER_OF_REVIEWS_TO_QUERY_META = 2;
    protected final int NUMBER_OF_REVIEWS_TO_QUERY_DELETE = 10;

    protected final int NUMBER_OF_WORDS_TO_QUERY = 50;
    protected final int NO_AVERAGE_ARGUMENT = 1;

    private final String AUXILIARY_INDEX_DIR_PATTERN = "aux";

    public PrintWriter tlog = null;
    public final int inputScale;
    final String allIndexesDirectory;
    @SuppressWarnings("unused")
    final String indexParentDirectory;
    final ScalingCases scalingCases;

    public Experiment(String localDir, String indexDirectoryName, int inputScale, boolean logMergeType){
        this.indexParentDirectory = localDir;
        this.allIndexesDirectory = indexDirectoryName;
        this.inputScale = inputScale;
        this.scalingCases = new ScalingCases(this.inputScale, logMergeType);
    }

    public abstract void runExperiment();

    protected void queryAfterBuildIndex(IndexReader indexReader, IndexWriter indexWriter) {
        tlog.println("===== After Build/Merge... =====");
        testWordQueriesOnAverage(indexReader, indexWriter, scalingCases.getWordQueries());
        int currentNumberOfReviews = testMetaData(indexReader);
//        testReviewMetaData(indexReader, currentNumberOfReviews);
    }

    protected IndexReader doInsertions(IndexWriter indexWriter) {
        IndexReader indexReader;
        int currentNumberOfReviews;

        int numberOfInsertions = scalingCases.getNumberOfInsertionFiles();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfInsertions; i++) {
            tlog.println("=====  Index insertion number " + i + "=====");
            currentNumberOfReviews = insertToIndex(indexWriter, i);
            indexReader = deleteReviews(indexWriter, currentNumberOfReviews);
            testWordQueriesOnAverage(indexReader, indexWriter, MiscTools.getRandomWords(allIndexesDirectory, NUMBER_OF_WORDS_TO_QUERY));

        }
        PrintingTool.printElapsedTimeToLog(tlog, startTime, ENTIRE_INSERTIONS_MESSAGE, NO_AVERAGE_ARGUMENT);
        return recreateIndexReader(indexWriter);
    }

    protected int insertToIndex(IndexWriter indexWriter, int insertionNumber) {
//        tlog.println("=====  Index insertion number " + insertionNumber + "=====");
        long startTime = System.currentTimeMillis();
        int currentNumberOfReviews = indexWriter.insert(scalingCases.getInsertFileName(insertionNumber),
                getAuxiliaryIndexDirPattern(insertionNumber));
        PrintingTool.printElapsedTimeToLog(tlog, startTime, SINGLE_INSERTION_MESSAGE + insertionNumber, NO_AVERAGE_ARGUMENT);
        return currentNumberOfReviews;
    }


    protected IndexReader deleteReviews(IndexWriter indexWriter, int currentNumberOfReviews) {
        System.out.println("=====" + "Index deletion " + "=====");
        indexWriter.removeReviews(allIndexesDirectory,
                scalingCases.getRandomRids(NUMBER_OF_REVIEWS_TO_QUERY_DELETE, 1, currentNumberOfReviews));
        return recreateIndexReader(indexWriter);
    }

    private IndexReader recreateIndexReader(IndexWriter indexWriter){
        if(indexWriter instanceof LogMergeIndexWriter)
            return new IndexReader(allIndexesDirectory, true);
        else
            return new IndexReader(allIndexesDirectory);
    }



    private int testMetaData(IndexReader indexReader) {
        int numberOfReviews = indexReader.getNumberOfReviews();
        tlog.println("#Reviews: " + numberOfReviews);
        tlog.println("#Tokens: " + indexReader.getTotalNumberOfTokens());
        tlog.println();
        return numberOfReviews;
    }

    private void testReviewMetaData(IndexReader indexReader, int currentNumberOfReviews) {
        List<Integer> rids = scalingCases.getRandomRids(NUMBER_OF_REVIEWS_TO_QUERY_META, 1, currentNumberOfReviews);
        tlog.print("querying rids: ");
        PrintingTool.printCollection(tlog, rids);
        for (int rid : rids) {
            tlog.println("rid: " + rid + " " +
                    indexReader.getProductId(rid) + " " +
                    indexReader.getReviewScore(rid) + " " +
                    indexReader.getReviewHelpfulnessNumerator(rid) + " " +
                    indexReader.getReviewHelpfulnessDenominator(rid) + " " +
                    indexReader.getReviewLength(rid));
        }
    }

    protected void testWordQueriesOnAverage(IndexReader indexReader,
                                            IndexWriter indexWriter,
                                            String[] wordTestCases) {
        long startTime = System.currentTimeMillis();
        for (String word : wordTestCases) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word, indexWriter);
//            tlog.println(word + ":");
//            printEnumeration(res);
//             todo: delete the following for time measuring
//            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word));
//            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word));
        }
        String message = "querying " + wordTestCases.length + " random words on average";
        PrintingTool.printElapsedTimeToLog(tlog, startTime, message, wordTestCases.length);
    }

    protected String getAuxiliaryIndexDirPattern(int num) {
        return allIndexesDirectory + File.separator + AUXILIARY_INDEX_DIR_PATTERN + num;
    }

    private void removeIndex(String indexDirectoryName) {
        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeAllIndexFiles(indexDirectoryName);
    }

    protected void printEnumeration(Enumeration<?> enumToPrint) {
        while (enumToPrint.hasMoreElements()) {
            tlog.print(enumToPrint.nextElement().toString() + " ");
            tlog.flush();
        }
        tlog.println();
    }

    protected void printDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
    }


    protected void createTestLog(String experimentType) {
        try {
            File indexDirectoryFile = new File(this.allIndexesDirectory);
            File testLogFile = new File(indexDirectoryFile.getParent() + File.separator + "0TESTLOG.txt");
            tlog = new PrintWriter(new BufferedWriter(new FileWriter(testLogFile, true)), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tlog.println("=======================================");
        tlog.println(experimentType);
        tlog.println("Log E" + scalingCases.getTestType());
        tlog.println("=======================================");
        logDateAndTime();
    }

    private void logDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        tlog.println(dtf.format(now));
    }

}
