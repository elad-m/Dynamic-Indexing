package dynamic_index.index_experiments;

import dynamic_index.IndexReader;
import dynamic_index.IndexWriter;
import dynamic_index.global_util.PrintingUtil;
import dynamic_index.global_util.MiscUtils;

import java.io.File;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


public class SimpleMergeExperiment extends Experiment {


    public SimpleMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + MiscUtils.INDEXES_DIR_NAME,
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

        deleteReviews(indexWriter);
        queryAfterDelete(indexReader);

        indexReader = mergeIndex(allIndexesDirectory, indexWriter, indexReader);
        queryAfterMerge(indexReader);

        tlog.close();
    }

    private IndexReader mergeIndex(String indexDirectory, IndexWriter indexWriter, IndexReader indexReader) {
        long startTime = System.currentTimeMillis();
        System.out.println("=====\n" + "Merging All Indexes " + "\n=====");
        indexWriter.merge(indexReader);
        PrintingUtil.printElapsedTimeToLog(tlog, startTime, "\n\tIndex Merging");
        return new IndexReader(indexDirectory);
    }

    private void queryAfterMerge(IndexReader indexReader) {
        queryAfterBuildIndex(indexReader);
    }


    private void deleteReviews(IndexWriter indexWriter) {
        System.out.println("=====\n" + "Index deletion " + "\n=====");
        indexWriter.removeReviews(allIndexesDirectory, scalingCases.getDelReviews());
    }

    private void queryAfterDelete(IndexReader indexReader) {
        tlog.println("===== words after deleted reviews... =====");
        testGetReviewsWithToken(indexReader, scalingCases.getWordQueries());
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
        PrintingUtil.printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
    }

    private void queryAfterInsert(IndexReader indexReader){
        tlog.println("===== words inserted queries... =====");
        testGetReviewsWithToken(indexReader, scalingCases.getWordQueries());
    }

    private IndexWriter buildIndex() {
        long startTime = System.currentTimeMillis();
        IndexWriter indexWriter = new IndexWriter(allIndexesDirectory, inputScale);
        indexWriter.constructIndex(scalingCases.getInputFilename(), allIndexesDirectory);
        PrintingUtil.printElapsedTimeToLog(tlog, startTime, "\n\tEntire index construction");
        return indexWriter;
    }

    private void queryAfterBuildIndex(IndexReader indexReader) {
        tlog.println("===== After Build/Merge... =====");
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
        PrintingUtil.printElapsedTimeToLog(tlog, startReviewsWithToken, numOfWords + " random getReviewsWithToken");

    }

    private String[] getRandomWords(String indexDirectoryName, int numOfWords) {
        File indexDirectory = new File(indexDirectoryName);
        Map<Integer, String> wordIdToString = MiscUtils.loadMapFromFile(indexDirectory, MiscUtils.WORDS_MAPPING);
        String[] randomWords = new String[numOfWords];
        for (int i = 0; i < numOfWords; i++) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, wordIdToString.size());
            randomWords[i] = (wordIdToString.get(randomNum));
        }
        return randomWords;
    }
}
