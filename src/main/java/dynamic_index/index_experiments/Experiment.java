package dynamic_index.index_experiments;

import dynamic_index.*;
import dynamic_index.global_tools.PrintingTool;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static dynamic_index.global_tools.MiscTools.ENTIRE_INSERTIONS_MESSAGE;

/**
 * Index merging experiment. Consists of index building, inserting deleting.
 * Most of the sub-classes functionality lies here.
 */
abstract public class Experiment {

    protected final int NUMBER_OF_REVIEWS_TO_QUERY_META = 2;
    protected final int NUMBER_OF_REVIEWS_TO_QUERY_DELETE = 10;

    protected final int NUMBER_OF_WORDS_TO_QUERY = 50;

    protected PrintWriter tlog = null;
    protected final int inputScale;
    protected final String allIndexesDirectory;
    protected final ScalingCases scalingCases;
    protected final WordsRandomizer wordsRandomizer;
    protected final ResultsWriter resultsWriter;
    protected final ResultsVerifier resultsVerifier;
    private final boolean shouldVerify;
    @SuppressWarnings("unused")
    final String indexParentDirectory;

    public Experiment(String localDir,
                      String indexDirectoryName,
                      int inputScale,
                      boolean shouldVerify) {
        this.indexParentDirectory = localDir;
        this.allIndexesDirectory = indexDirectoryName;
        this.inputScale = inputScale;
        this.scalingCases = new ScalingCases(this.inputScale);
        this.resultsWriter = new ResultsWriter();
        this.wordsRandomizer = new WordsRandomizer(allIndexesDirectory, inputScale);
        this.resultsVerifier = new ResultsVerifier(localDir, allIndexesDirectory, inputScale);
        this.shouldVerify = shouldVerify;
    }
    public abstract void runExperiment();

    protected void queryAfterBuildIndex(IndexReader indexReader, IndexWriter indexWriter) {
        tlog.println("===== After Build/Merge... =====");
        testWordQueriesOnAverage(indexReader,
                indexWriter,
                wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY));
        testMetaData(indexReader);
    }

    protected IndexReader doInsertions(IndexWriter indexWriter) {
        IndexReader indexReader;
        int numberOfInsertions = scalingCases.getNumberOfInsertionFiles();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfInsertions; i++) {
            System.out.println("=====  Index insertion number " + i + "=====");
            insertToIndex(indexWriter, i);
            indexReader = deleteReviews(indexWriter);
            testWordQueriesOnAverage(indexReader,
                    indexWriter,
                    wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY)
            );
        }
        resultsWriter.addToElapsedConstructionTimeList(startTime);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, ENTIRE_INSERTIONS_MESSAGE);
        return recreateIndexReader(indexWriter);
    }

    protected void insertToIndex(IndexWriter indexWriter, int insertionNumber) {
        long startTime = System.currentTimeMillis();
        indexWriter.insert(scalingCases.getInsertFileName(insertionNumber),
                getAuxiliaryIndexDirPattern(insertionNumber));
        resultsWriter.addToElapsedConstructionTimeList(startTime);
    }


    protected IndexReader deleteReviews(IndexWriter indexWriter) {
        System.out.print("Deleting reviews: ");
        List<Integer> ridsToDelete = scalingCases.getRandomRidsNoRepetition(NUMBER_OF_REVIEWS_TO_QUERY_DELETE,
                1,
                indexWriter.getNumberOfReviewsIndexed());
        PrintingTool.printList(ridsToDelete);
        System.out.println();

        indexWriter.removeReviews(allIndexesDirectory, ridsToDelete);
        return recreateIndexReader(indexWriter);
    }

    private IndexReader recreateIndexReader(IndexWriter indexWriter) {
        if (indexWriter instanceof LogMergeIndexWriter)
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
                                            List<String> wordTestCases) {
        long startTime = System.currentTimeMillis();
        System.out.print("words queried: ");
        PrintingTool.printList(wordTestCases);
        for (String word : wordTestCases) {
            Map<Integer, Integer> resultedPostingsList = indexReader.getReviewsWithToken(word, indexWriter);
            if (this.shouldVerify) {
                resultsVerifier.verifySingleQuery(word,
                        wordsRandomizer.getWordNumber(word),
                        resultedPostingsList,
                        indexWriter.getNumberOfReviewsIndexed());
            }
        }
        resultsWriter.addToElapsedQueryTimeList(startTime, wordTestCases.size());
        resultsWriter.addToIndexDiskSize(getAllIndexSize());
        resultsWriter.addToNumberOfIndexes(getNumberOfIndexes());
    }

    protected void printResultsOfQuery(String word, Map<Integer, Integer> res, IndexReader indexReader, IndexWriter indexWriter) {
        tlog.println(word + ":");
        PrintingTool.printMap(tlog, res);
//        printWordMetaData(word, indexReader, indexWriter);
    }

    protected void printWordMetaData(String word, IndexReader indexReader, IndexWriter indexWriter) {
        if (indexWriter instanceof LogMergeIndexWriter) {
            LogMergeIndexWriter logMergeIndexWriter = (LogMergeIndexWriter) indexWriter;
            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word, logMergeIndexWriter));
            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word, logMergeIndexWriter));
        } else {
            tlog.println("#mentions: " + indexReader.getNumberOfMentions(word));
            tlog.println("#reviews: " + indexReader.getNumberOfReviews(word));
        }
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
            File testLogFile = new File(indexDirectoryFile.getParent() + File.separator + "LOG.txt");
            tlog = new PrintWriter(new BufferedWriter(new FileWriter(testLogFile, true)), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tlog.println("=======================================");
        tlog.println(experimentType);
        tlog.println("Review Scale: E" + scalingCases.getTestType());
        tlog.println("=======================================");
        logDateAndTime();
    }

    private void logDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        tlog.println(dtf.format(now));
    }

    protected long getAllIndexSize() {
        File allIndexesDirectory = new File(this.allIndexesDirectory);
        Path path = allIndexesDirectory.toPath();
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

    protected int getNumberOfIndexes(){
        long numberOfIndexes = -1;
        try {
            numberOfIndexes = Files.find(
                    Paths.get(allIndexesDirectory),
                    1,  // how deep do we want to descend
                    (path, attributes) -> attributes.isDirectory()
            ).count() - 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (int)numberOfIndexes;
    }

}
