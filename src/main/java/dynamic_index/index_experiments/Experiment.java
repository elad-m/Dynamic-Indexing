package dynamic_index.index_experiments;

import dynamic_index.*;
import dynamic_index.global_tools.PrintingTool;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static dynamic_index.global_tools.MiscTools.ENTIRE_INSERTIONS_MESSAGE;
import static dynamic_index.global_tools.MiscTools.SINGLE_INSERTION_MESSAGE;


abstract public class Experiment {

    protected final int NUMBER_OF_REVIEWS_TO_QUERY_META = 2;
    protected final int NUMBER_OF_REVIEWS_TO_QUERY_DELETE = 10;

    protected final int NUMBER_OF_WORDS_TO_QUERY = 20;

    protected PrintWriter tlog = null;
    protected final int inputScale;
    protected final String allIndexesDirectory;
    protected final ScalingCases scalingCases;
    protected final ResultsWriter resultsWriter;
    protected final WordsRandomizer wordsRandomizer;
    protected final ResultsVerifier resultsVerifier;
    @SuppressWarnings("unused")
    final String indexParentDirectory;

    public Experiment(String localDir, String indexDirectoryName, int inputScale, boolean logMergeType){
        this.indexParentDirectory = localDir;
        this.allIndexesDirectory = indexDirectoryName;
        this.inputScale = inputScale;
        this.scalingCases = new ScalingCases(this.inputScale, logMergeType);
        this.resultsWriter = new ResultsWriter();
        this.wordsRandomizer = new WordsRandomizer(allIndexesDirectory, inputScale);
        this.resultsVerifier = new ResultsVerifier(localDir, allIndexesDirectory, inputScale);
    }

    public abstract void runExperiment();

    public abstract void initiateExperiment();

    protected void queryAfterBuildIndex(IndexReader indexReader, IndexWriter indexWriter) {
        tlog.println("===== After Build/Merge... =====");
        testWordQueriesOnAverage(indexReader,
                indexWriter,
                scalingCases.getWordQueries(),
                false);
//                wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY));
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
            insertToIndex(indexWriter, i);
            indexReader = deleteReviews(indexWriter);
//            indexReader = recreateIndexReader(indexWriter);
            testWordQueriesOnAverage(indexReader,
                    indexWriter,
                    wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY),
                    false);
//            scalingCases.getWordQueries());

        }
        PrintingTool.printElapsedTimeToLog(tlog, startTime, ENTIRE_INSERTIONS_MESSAGE);
        return recreateIndexReader(indexWriter);
    }

    protected int insertToIndex(IndexWriter indexWriter, int insertionNumber) {
        long startTime = System.currentTimeMillis();
        int currentNumberOfReviews = indexWriter.insert(scalingCases.getInsertFileName(insertionNumber),
                getAuxiliaryIndexDirPattern(insertionNumber));
        PrintingTool.printElapsedTimeToLog(tlog, startTime, SINGLE_INSERTION_MESSAGE + insertionNumber);
        return currentNumberOfReviews;
    }


    protected IndexReader deleteReviews(IndexWriter indexWriter) {
        System.out.println("=====" + "Index deletion " + "=====");
        indexWriter.removeReviews(allIndexesDirectory,
                scalingCases.getRandomRids(NUMBER_OF_REVIEWS_TO_QUERY_DELETE,
                        1,
                        indexWriter.getNumberOfReviewsIndexed()))
        ;
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
        PrintingTool.printList(tlog, rids);
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
                                            List<String> wordTestCases,
                                            boolean shouldVerify) {
//        long startTime = System.currentTimeMillis();
        System.out.print("words queried: ");
        PrintingTool.printList(wordTestCases);
        boolean print = true;
        for (String word : wordTestCases) {
            Map<Integer, Integer> resultedPostingsList = indexReader.getReviewsWithToken(word, indexWriter);
            if(shouldVerify)
                resultsVerifier.verifySingleQuery(word,
                        wordsRandomizer.getWordNumber(word),
                        resultedPostingsList,
                        indexWriter.getNumberOfReviewsIndexed());
            if(print && !resultedPostingsList.isEmpty()){ // print one non empty result
                printResultsOfQuery(word, resultedPostingsList, indexReader, indexWriter);
                print = false;
            }
        }
//        resultsWriter.addToElapsedList(startTime, wordTestCases.size());
//        String message = "querying " + wordTestCases.size() + " random words";
//        PrintingTool.printElapsedTimeToLog(tlog, startTime, message);

        tlog.println();
    }

    protected void printResultsOfQuery(String word, Map<Integer, Integer> res, IndexReader indexReader, IndexWriter indexWriter) {
        tlog.println(word + ":");
        PrintingTool.printMap(tlog, res);
//        if(indexWriter instanceof LogMergeIndexWriter){
//            LogMergeIndexWriter logMergeIndexWriter = (LogMergeIndexWriter)indexWriter;
//            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word, logMergeIndexWriter));
//            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word, logMergeIndexWriter));
//        } else {
//            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word));
//            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word));
//        }
    }

    protected String getAuxiliaryIndexDirPattern(int num) {
        String AUXILIARY_INDEX_DIR_PATTERN = "aux";
        return allIndexesDirectory + File.separator + AUXILIARY_INDEX_DIR_PATTERN + num;
    }

    protected void removeIndex() {
        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeAllIndexFiles(allIndexesDirectory);
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
        tlog.println("Scale E" + scalingCases.getTestType());
        tlog.println("=======================================");
        logDateAndTime();
    }

    private void logDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        tlog.println(dtf.format(now));
    }

}
