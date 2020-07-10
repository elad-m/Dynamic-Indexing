import dynamic_index.*;
import dynamic_index.runExperiment.Experiment;
import dynamic_index.runExperiment.LogMergeExperiment;
import dynamic_index.runExperiment.SimpleMergeExperiment;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static dynamic_index.Statics.printElapsedTimeToLog;

@SuppressWarnings({"SpellCheckingInspection", "SameParameterValue"})
public class Main {

    static final String INSERTION_DIR_NAME = "E1TestResources";

    public static void main(String[] args) {
        final String localDir = System.getProperty("user.dir");

        System.out.println("======================================= Starting Experiment =======================================");
//        SimpleMergeExperiment simpleMergeExperiment = new SimpleMergeExperiment(localDir);
//        simpleMergeExperiment.runExperiment();
        LogMergeExperiment logMergeExperiment = new LogMergeExperiment(localDir);
        logMergeExperiment.runExperiment();

    }



//    private static void testLogMerge(String indexDirectory) {
//        printDateAndTime();
//        ScalingCases scalingCases = new ScalingCases(INPUT_SCALE);
//        createTestLog(indexDirectory, scalingCases.getTestType());
//
//        ContinuousIndexWriter continuousIndexWriter = new ContinuousIndexWriter();
//
//        long startTime = System.currentTimeMillis();
//        continuousIndexWriter.construct(scalingCases.getInputFilename(), indexDirectory);
//        printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction: Log-Merged");
//
//        IndexReader indexReader = new IndexReader(indexDirectory, true);
//        queryAfterBuildIndex(continuousIndexWriter, indexReader, scalingCases);
//
//        System.out.println("=====\n" + "Index deletion " + "\n=====");
//        continuousIndexWriter.removeReviews(indexDirectory, scalingCases.getDelReviews());
//
//    }
//
//    private static void queryAfterBuildIndex(ContinuousIndexWriter continuousIndexWriter, IndexReader indexReader, ScalingCases scalingCases) {
//        tlog.println("===== #logMerged# words in index queries... =====");
//        testGetReviewsWithToken(continuousIndexWriter, indexReader, scalingCases.getWordQueries());
//        tlog.println("===== #logMerged# words not in index queries... =====");
//        testGetReviewsWithToken(continuousIndexWriter, indexReader, scalingCases.getNegWordQueries());
//    }
//
//    private static void testGetReviewsWithToken(ContinuousIndexWriter continuousIndexWriter, IndexReader indexReader, String[] wordQueries) {
//        for (String word : wordQueries) {
//            Enumeration<Integer> res = indexReader.getReviewsWithToken(word, continuousIndexWriter);
//            tlog.print(word + ": " + System.lineSeparator());
//            printEnumeration(res);
//        }
//    }

//    private static void testRegularMerge(String indexDirectory, String insertionDirectory) {
//        printDateAndTime();
//        ScalingCases scalingCases = new ScalingCases(INPUT_SCALE, insertionDirectory);
//        createTestLog(indexDirectory, scalingCases.getTestType());
//
//        IndexWriter indexWriter = new IndexWriter(indexDirectory, INPUT_SCALE);
//        buildIndex(indexDirectory, indexWriter, scalingCases.getInputFilename());
//        IndexReader indexReader = new IndexReader(indexDirectory);
//        queryAfterBuildIndex(indexReader, scalingCases);
//
//        indexReader = insertToIndex(indexDirectory, indexWriter, scalingCases, 6);
//        queryAfterInsert(indexReader, scalingCases);
//
//        deleteReviews(indexDirectory, indexWriter, scalingCases);
//        queryAfterDelete(indexReader, scalingCases);
//
//        indexReader = mergeIndex(indexDirectory, indexWriter, indexReader);
//        queryAfterDelete(indexReader, scalingCases);
//
//        tlog.close();
//    }
//
//    private static IndexReader mergeIndex(String indexDirectory, IndexWriter indexWriter, IndexReader indexReader) {
//        long startTime = System.currentTimeMillis();
//        System.out.println("=====\n" + "Merging All Indexes " + "\n=====");
//        indexWriter.merge(indexReader);
//        printElapsedTimeToLog(tlog, startTime, "\n\tIndex Merging");
//        return new IndexReader(indexDirectory);
//    }
//
//
//    private static IndexReader insertToIndex(String indexDirectory, IndexWriter indexWriter,
//                                             ScalingCases scalingCases, int numberOfInsertions) {
//        assert numberOfInsertions <= scalingCases.getNumberOfInsertionFiles();
//        for (int i = 0; i < numberOfInsertions; i++) {
//            System.out.println("=====\n" + "Index insertion number " + i + "\n=====");
//            insertToIndex(indexWriter,
//                    getAuxiliaryIndexDirPattern(indexDirectory, i),
//                    scalingCases.getInsertFileName(i));
//        }
//        return new IndexReader(indexDirectory);
//    }
//
//
//    private static void queryRandomWords(String indexDirectoryName, IndexReader indexReader, int numOfWords) {
//        tlog.println(numOfWords + " random queries...");
//        String[] randomWords = getRandomWords(indexDirectoryName, numOfWords);
//        long startReviewsWithToken = System.currentTimeMillis();
//        testGetReviewsWithToken(indexReader, randomWords);
//        printElapsedTimeToLog(tlog, startReviewsWithToken, numOfWords + " random getReviewsWithToken");
//
//    }
//
//    private static String[] getRandomWords(String indexDirectoryName, int numOfWords) {
//        File indexDirectory = new File(indexDirectoryName);
//        Map<Integer, String> wordIdToString = Statics.loadMapFromFile(indexDirectory, Statics.WORDS_MAPPING);
//        String[] randomWords = new String[numOfWords];
//        for (int i = 0; i < numOfWords; i++) {
//            int randomNum = ThreadLocalRandom.current().nextInt(0, wordIdToString.size());
//            randomWords[i] = (wordIdToString.get(randomNum));
//        }
//        return randomWords;
//    }
//
//    private static void queryAfterBuildIndex(IndexReader indexReader, ScalingCases scalingCases) {
//        tlog.println("===== words in index queries... =====");
//        testGetReviewsWithToken(indexReader, scalingCases.getWordQueries());
//        tlog.println("===== words not in index queries... =====");
//        testGetReviewsWithToken(indexReader, scalingCases.getNegWordQueries());
//    }
//
//    private static void queryAfterInsert(IndexReader indexReader, ScalingCases scalingCases){
//        tlog.println("===== words inserted queries... =====");
//        testGetReviewsWithToken(indexReader, scalingCases.getInsertQueries());
//    }
//
//    private static void queryAfterDelete(IndexReader indexReader, ScalingCases scalingCases) {
//        tlog.println("===== words after deleted reviews... =====");
//        testGetReviewsWithToken(indexReader, scalingCases.getDelQueries());
//    }
//
//    private static void testGetReviewsWithToken(IndexReader indexReader,
//                                                String[] wordTestCases) {
//        for (String word : wordTestCases) {
//            Enumeration<Integer> res = indexReader.getReviewsWithToken(word);
//            tlog.print(word + ": " + System.lineSeparator());
//            printEnumeration(res);
//        }
//    }
//
//    private static void buildIndex(String indexDirectoryName, IndexWriter indexWriter,  String rawDataFilename) {
//        long startTime = System.currentTimeMillis();
//        indexWriter.constructIndex(rawDataFilename, indexDirectoryName);
//        printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
//    }
//
//    private static void insertToIndex(IndexWriter indexWriter, String auxIndexDirectoryName, String rawDataFilename) {
//        long startTime = System.currentTimeMillis();
//        indexWriter.insert(rawDataFilename, auxIndexDirectoryName);
//        printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
//    }
//
//    private static void deleteReviews(String indexDirectory, IndexWriter indexWriter, ScalingCases scalingCases) {
//        System.out.println("=====\n" + "Index deletion " + "\n=====");
//        indexWriter.removeReviews(indexDirectory, scalingCases.getDelReviews());
//    }

//    private static String getAuxiliaryIndexDirPattern(String indexDirectory, int num) {
//        return indexDirectory + File.separator + AUXILIARY_INDEX_DIR_PATTERN + num;
//    }
//
//    private static void removeIndex(String indexDirectoryName) {
//        IndexRemover indexRemover = new IndexRemover();
//        indexRemover.removeAllIndexFiles(indexDirectoryName);
//    }
//
//    private static void printEnumeration(Enumeration<?> enumToPrint) {
//        while (enumToPrint.hasMoreElements()) {
//            tlog.print(enumToPrint.nextElement().toString() + " ");
//            tlog.flush();
//        }
//        tlog.println();
//    }
//
//    private static void printDateAndTime() {
//        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
//        LocalDateTime now = LocalDateTime.now();
//        System.out.println(dtf.format(now));
//    }
//
//
//    private static void createTestLog(String indexDirectoryName, int testCase) {
//        try {
//            File indexDirectory = new File(indexDirectoryName);
//            File testLogFile = new File(indexDirectory.getParent() + File.separator + "0TESTLOG.txt");
//            tlog = new PrintWriter(new BufferedWriter(new FileWriter(testLogFile, true)), true);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        tlog.println("===========================================================");
//        tlog.println("Log E" + testCase);
//        tlog.println("===========================================================");
//        logDateAndTime();
//    }
//
//    private static void logDateAndTime() {
//        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
//        LocalDateTime now = LocalDateTime.now();
//        tlog.println(dtf.format(now));
//    }

}

