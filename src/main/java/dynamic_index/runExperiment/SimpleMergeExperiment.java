package dynamic_index.runExperiment;

import dynamic_index.IndexReader;
import dynamic_index.IndexWriter;
import dynamic_index.Statics;

import java.io.File;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static dynamic_index.Statics.printElapsedTimeToLog;

public class SimpleMergeExperiment extends Experiment {


    public SimpleMergeExperiment(String localDir) {
        super(localDir, localDir + File.separatorChar + Statics.INDEXES_DIR_NAME);
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Simple Merge ");

        IndexWriter indexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(indexDirectory);
        queryAfterBuildIndex(indexReader);

        indexReader = insertToIndex(indexWriter, 6);
        queryAfterInsert(indexReader);

        deleteReviews(indexWriter);
        queryAfterDelete(indexReader);

        indexReader = mergeIndex(indexDirectory, indexWriter, indexReader);
        queryAfterMerge(indexReader);

        tlog.close();
    }

    private IndexReader mergeIndex(String indexDirectory, IndexWriter indexWriter, IndexReader indexReader) {
        long startTime = System.currentTimeMillis();
        System.out.println("=====\n" + "Merging All Indexes " + "\n=====");
        indexWriter.merge(indexReader);
        printElapsedTimeToLog(tlog, startTime, "\n\tIndex Merging");
        return new IndexReader(indexDirectory);
    }

    private void queryAfterMerge(IndexReader indexReader) {
        queryAfterBuildIndex(indexReader);
        queryAfterInsert(indexReader);
        queryAfterDelete(indexReader);
    }


    private void deleteReviews(IndexWriter indexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        indexWriter.removeReviews(indexDirectory, scalingCases.getDelReviews());
    }

    private void queryAfterDelete(IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testGetReviewsWithToken(indexReader, scalingCases.getDelQueries());
    }

    private IndexReader insertToIndex(IndexWriter indexWriter, int numberOfInsertions) {
        assert numberOfInsertions <= scalingCases.getNumberOfInsertionFiles();
        for (int i = 0; i < numberOfInsertions; i++) {
            System.out.println("=====\n" + "Index insertion number " + i + "\n=====");
            insertToIndex(indexWriter,
                    getAuxiliaryIndexDirPattern(i),
                    scalingCases.getInsertFileName(i));
        }
        return new IndexReader(indexDirectory);
    }

    private void insertToIndex(IndexWriter indexWriter, String auxIndexDirectoryName, String rawDataFilename) {
        long startTime = System.currentTimeMillis();
        indexWriter.insert(rawDataFilename, auxIndexDirectoryName);
        printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
    }

    private void queryAfterInsert(IndexReader indexReader){
        tlog.println("===== words inserted queries... =====");
        testGetReviewsWithToken(indexReader, scalingCases.getInsertQueries());
    }

    private IndexWriter buildIndex() {
        long startTime = System.currentTimeMillis();
        IndexWriter indexWriter = new IndexWriter(indexDirectory, INPUT_SCALE);
        indexWriter.constructIndex(scalingCases.getInputFilename(), indexDirectory);
        printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
        return indexWriter;
    }

    private void queryAfterBuildIndex(IndexReader indexReader) {
        tlog.println("===== words in index queries... =====");
        testGetReviewsWithToken(indexReader, scalingCases.getWordQueries());
        tlog.println("===== words not in index queries... =====");
        testGetReviewsWithToken(indexReader, scalingCases.getNegWordQueries());
    }

    private void testGetReviewsWithToken(IndexReader indexReader,
                                         String[] wordTestCases) {
        for (String word : wordTestCases) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word);
            tlog.print(word + ": " + System.lineSeparator());
            printEnumeration(res);
        }
    }


    private void queryRandomWords(String indexDirectoryName, IndexReader indexReader, int numOfWords) {
        tlog.println(numOfWords + " random queries...");
        String[] randomWords = getRandomWords(indexDirectoryName, numOfWords);
        long startReviewsWithToken = System.currentTimeMillis();
        testGetReviewsWithToken(indexReader, randomWords);
        printElapsedTimeToLog(tlog, startReviewsWithToken, numOfWords + " random getReviewsWithToken");

    }

    private String[] getRandomWords(String indexDirectoryName, int numOfWords) {
        File indexDirectory = new File(indexDirectoryName);
        Map<Integer, String> wordIdToString = Statics.loadMapFromFile(indexDirectory, Statics.WORDS_MAPPING);
        String[] randomWords = new String[numOfWords];
        for (int i = 0; i < numOfWords; i++) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, wordIdToString.size());
            randomWords[i] = (wordIdToString.get(randomNum));
        }
        return randomWords;
    }
}
