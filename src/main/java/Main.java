import dynamic_index.*;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("SpellCheckingInspection")
public class Main {

    static final String INDEX_DIR_NAME = "index";
    static PrintWriter tlog = null;
    public static final int INPUT_SCALE = 5;

    public static void main(String[] args) {
        final String dir = System.getProperty("user.dir");
        final String indexDirectory = dir + File.separatorChar + INDEX_DIR_NAME;

        testAll(indexDirectory);
        tlog.close();
        deleteIndex(indexDirectory);
    }


    private static void testAll(String indexDirectory) {
        printDateAndTime();
        ScalingCases scalingCases = new ScalingCases(INPUT_SCALE);
        createTestLog(indexDirectory, scalingCases.getTestType());

        buildIndex(indexDirectory, scalingCases.getInputFilename());

        IndexReader indexReader = new IndexReader(indexDirectory);
        queryRandomWords(indexDirectory, indexReader, 20);
        queryWordIndex(indexReader, scalingCases);
    }

    private static void printEnumeration(Enumeration<?> enumToPrint) {
        while (enumToPrint.hasMoreElements()) {
            tlog.print(enumToPrint.nextElement().toString() + " ");
            tlog.flush();
        }
        tlog.println();
    }

    private static void printDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
    }


    private static void createTestLog(String indexDirectoryName, int testCase) {
        try {
            File indexDirectory = new File(indexDirectoryName);
            File testLogFile = new File(indexDirectory.getParent() + File.separator + "0TESTLOG.txt");
            tlog = new PrintWriter(new BufferedWriter(new FileWriter(testLogFile, true)), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tlog.println("===========================================================");
        tlog.println("Log E" + testCase);
        tlog.println("===========================================================");
        logDateAndTime();
    }


    private static void logDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        tlog.println(dtf.format(now));
    }

    private static void queryRandomWords(String indexDirectoryName, IndexReader indexReader, int numOfWords) {
        tlog.println(numOfWords + " random queries...");
        String[] randomWords = getRandomWords(indexDirectoryName, numOfWords);
        long startReviewsWithToken = System.currentTimeMillis();
        testGetReviewsWithToken(indexReader, randomWords);
        printElapsedTime(tlog, startReviewsWithToken, "100 random getReviewsWithToken");

    }

    private static String[] getRandomWords(String indexDirectoryName, int numOfWords) {
        File indexDirectory = new File(indexDirectoryName);
        Map<Integer, String> wordIdToString = Statics.loadMapFromFile(indexDirectory, Statics.WORDS_MAPPING);
        String[] randomWords = new String[numOfWords];
        for (int i = 0; i < numOfWords; i++) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, wordIdToString.size());
            randomWords[i] = (wordIdToString.get(randomNum));
        }
        return randomWords;
    }

    private static void queryWordIndex(IndexReader indexReader, ScalingCases testCases) {
        tlog.println("words queries...");
        doAllWordsQueries(indexReader,
                testCases.getWordQueries(),
                testCases.getNegWordQueries());
//        String[] t = {"lusso"};
//        testGetReviewsWithToken(indexReader, t);
    }

    private static void doAllWordsQueries(IndexReader indexReader, String[] inSet, String[] notInSet) {
        testGetReviewsWithToken(indexReader, inSet);
        testGetReviewsWithToken(indexReader, notInSet);
    }

    private static void testGetReviewsWithToken(IndexReader indexReader,
                                                String[] wordTestCases) {
        tlog.println("Checking getReviewsWithToken...");
        for (String word : wordTestCases) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word);
            tlog.print(word + ": " + System.lineSeparator());
            printEnumeration(res);
        }
    }

    private static void deleteIndex(String indexDirectoryName) {
        IndexWriter indexWriter = new IndexWriter(INPUT_SCALE);
        indexWriter.removeIndex(indexDirectoryName);
    }

    private static void buildIndex(String indexDirectoryName, String rawDataFilename) {
        long startTime = System.currentTimeMillis();
        IndexWriter indexWriter = new IndexWriter(INPUT_SCALE);
        indexWriter.write(rawDataFilename, indexDirectoryName);
        printElapsedTime(tlog, startTime, "\n\tEntire index construction");
    }

    private static void printElapsedTime(PrintWriter tlog, long startTime, String methodName) {
        long endTime = System.currentTimeMillis();
        long elapsedTimeMilliSeconds = endTime - startTime;
        long elapsedTimeSeconds = elapsedTimeMilliSeconds / 1000;
        long elapsedTimeMinutes = elapsedTimeSeconds / 60;
        System.out.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);

        tlog.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);
    }

}

