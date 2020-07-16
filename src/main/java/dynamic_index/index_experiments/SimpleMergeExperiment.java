package dynamic_index.index_experiments;

import dynamic_index.IndexReader;
import dynamic_index.IndexWriter;
import dynamic_index.global_tools.PrintingTool;
import dynamic_index.global_tools.MiscTools;

import java.io.File;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


public class SimpleMergeExperiment extends Experiment {


    public SimpleMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.INDEXES_DIR_NAME,
                inputScale,
                false);
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Simple Merge ");

        IndexWriter indexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory);
        queryAfterBuildIndex(indexReader);

        indexReader = insertToIndex(indexWriter);
        queryAfterInsert(indexReader);

        indexReader = deleteReviews(indexWriter);
        queryAfterDelete(indexReader);

        indexReader = mergeIndex(allIndexesDirectory, indexWriter, indexReader);
        queryAfterMerge(indexReader);

        tlog.close();
    }

    private IndexReader mergeIndex(String indexDirectory, IndexWriter indexWriter, IndexReader indexReader) {
        long startTime = System.currentTimeMillis();
        System.out.println("=====\n" + "Merging All Indexes " + "\n=====");
        indexWriter.merge(indexReader);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, "\n\tIndex Merging");
        return new IndexReader(indexDirectory);
    }

    private void queryAfterMerge(IndexReader indexReader) {
        queryAfterBuildIndex(indexReader);
    }


    private IndexReader deleteReviews(IndexWriter indexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        indexWriter.removeReviews(allIndexesDirectory, scalingCases.getDelReviews());
        return new IndexReader(allIndexesDirectory);
    }

    private void queryAfterDelete(IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testWordQueries(indexReader, scalingCases.getWordQueries());
        testMetaData(indexReader);
        testReviewMetaData(indexReader);
    }

    private IndexReader insertToIndex(IndexWriter indexWriter) {
        int numberOfInsertions = scalingCases.getNumberOfInsertionFiles();
//        assert numberOfInsertions <= scalingCases.getNumberOfInsertionFiles();
        for (int i = 0; i < numberOfInsertions; i++) {
            System.out.println("=====\n" + "Index insertion number " + i + "\n=====");
            insertToIndex(indexWriter,
                    getAuxiliaryIndexDirPattern(i),
                    scalingCases.getInsertFileName(i));
        }
        return new IndexReader(allIndexesDirectory);
    }

    private void insertToIndex(IndexWriter indexWriter, String auxIndexDirectoryName, String rawDataFilename) {
        long startTime = System.currentTimeMillis();
        indexWriter.insert(rawDataFilename, auxIndexDirectoryName);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
    }

    private void queryAfterInsert(IndexReader indexReader){
        tlog.println("===== words inserted queries... =====");
        testWordQueries(indexReader, scalingCases.getWordQueries());
        testMetaData(indexReader);
        testReviewMetaData(indexReader);
    }

    private IndexWriter buildIndex() {
        long startTime = System.currentTimeMillis();
        IndexWriter indexWriter = new IndexWriter(allIndexesDirectory, inputScale);
        indexWriter.sortAndConstructIndex(scalingCases.getInputFilename());
        PrintingTool.printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
        return indexWriter;
    }

    private void queryAfterBuildIndex(IndexReader indexReader) {
        tlog.println("===== After Build/Merge... =====");
        testWordQueries(indexReader, scalingCases.getWordQueries());
        testMetaData(indexReader);
        testReviewMetaData(indexReader);
    }

    private void testWordQueries(IndexReader indexReader,
                                 String[] wordTestCases) {
        for (String word : wordTestCases) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word);
            tlog.println(word + ":");
            printEnumeration(res);
            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word));
            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word));
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


    private void queryRandomWords(String indexDirectoryName, IndexReader indexReader, int numOfWords) {
        tlog.println(numOfWords + " random queries...");
        String[] randomWords = getRandomWords(indexDirectoryName, numOfWords);
        long startReviewsWithToken = System.currentTimeMillis();
        testWordQueries(indexReader, randomWords);
        PrintingTool.printElapsedTimeToLog(tlog, startReviewsWithToken, numOfWords + " random getReviewsWithToken");

    }

    private String[] getRandomWords(String indexDirectoryName, int numOfWords) {
        File indexDirectory = new File(indexDirectoryName);
        Map<Integer, String> wordIdToString = MiscTools.loadMapFromFile(indexDirectory, MiscTools.WORDS_MAPPING);
        String[] randomWords = new String[numOfWords];
        for (int i = 0; i < numOfWords; i++) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, wordIdToString.size());
            randomWords[i] = (wordIdToString.get(randomNum));
        }
        return randomWords;
    }
}
