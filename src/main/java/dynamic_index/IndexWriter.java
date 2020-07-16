package dynamic_index;

import dynamic_index.external_sort.ExternalMergeSort;
import dynamic_index.external_sort.TermToReviewBlockWriter;
import dynamic_index.global_tools.IndexInvalidationTool;
import dynamic_index.global_tools.PrintingTool;
import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_writing.IndexMergeWriter;
import dynamic_index.index_writing.ExternalIndexWriter;
import dynamic_index.index_writing.ReviewsMetaDataIndexWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static dynamic_index.global_tools.MiscTools.*;
import static dynamic_index.global_tools.ParsingTool.extractHelpfulness;
import static dynamic_index.global_tools.ParsingTool.textToNormalizedTokens;

public class IndexWriter {


    private final File allIndexesDirectory;
    private File currentIndexDirectory;

    private ExternalIndexWriter wordsDataIndexWriter;
    private ReviewsMetaDataIndexWriter reviewsMetaDataIndexWriter;
    private final Map<String, Integer> wordTermToTermID = new HashMap<>();
    private TermToReviewBlockWriter wordsTermToReviewBlockWriter;

    private int reviewCounter = 1; // not necessarily the number of reviews in index in practice because deletion
    private int tokenCounter = 0; // token counter only incremented in the mapping stage
    private final int inputScaleType;

    public IndexWriter(String allIndexesDirectory, int inputScaleType) {
        this.allIndexesDirectory = createDirectory(allIndexesDirectory);
        this.inputScaleType = inputScaleType;
    }

    /**
     * Builds an index from a given product review raw data in the main/all index directory.
     * @param inputFile - product review raw data
     */
    public void sortAndConstructIndex(String inputFile) {
        this.currentIndexDirectory = allIndexesDirectory;
        this.reviewsMetaDataIndexWriter = new ReviewsMetaDataIndexWriter(allIndexesDirectory.getAbsolutePath());
        instantiateWriters();
        sortAndConstructIndex(inputFile, reviewCounter);
    }

    /**
     * Builds an auxiliary index of a given product review data
     *
     * @param inputFile         - product review raw data
     * @param auxIndexDirectory - directory for the auxiliary index. Should be inside the
     *                          main index directory.
     */
    public void insert(String inputFile, String auxIndexDirectory) {
        this.currentIndexDirectory = createDirectory(auxIndexDirectory);
        this.reviewsMetaDataIndexWriter = new ReviewsMetaDataIndexWriter(allIndexesDirectory.getAbsolutePath());
        instantiateWriters();
        sortAndConstructIndex(inputFile, reviewCounter);
    }

    private void instantiateWriters() {
        wordsDataIndexWriter = new ExternalIndexWriter(currentIndexDirectory);
    }


    /*
    Using the first external sort algorithm (sort-merge).
     */
    private void sortAndConstructIndex(String inputFile, final int initialReviewCounter) {
        try {
            Map<Integer, String> wordTermIdToTerm;
//            if (!SKIP_SORTING) {
                constructTermToTermIDMapping(inputFile); // token and review counter complete
                writeHashmapFor100Random();
                firstSortIteration(inputFile, initialReviewCounter); // review counter resets, second input reading
                externalSort();
                wordTermIdToTerm = swapHashMapDirections(wordTermToTermID);
//            }
//            else { // when skipping sorting
//                tokenCounter = getTokenCounter(currentIndexDirectory, false);  // also token counter
//                reviewCounter = 1 + getReviewCounter(currentIndexDirectory, false);
//                wordTermIdToTerm = loadMapFromFile(currentIndexDirectory, WORDS_MAPPING);
//            }
            System.gc();
            constructIndexFromSorted(wordTermIdToTerm);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void writeHashmapFor100Random() {
        // adds to the same file at the main directory
        writeMapToFile(wordTermToTermID, allIndexesDirectory, WORDS_MAPPING, inputScaleType);
    }

    private void constructTermToTermIDMapping(String inputFile) throws IOException {
        System.out.println("Starting term mapping:");
        Set<String> wordTerms = getTermsSorted(inputFile); // first reading of whole input file
        createMappingForSet(wordTerms, wordTermToTermID);
        wordTerms.clear();
    }

    private Set<String> getTermsSorted(String inputFile) throws IOException {
        long startTime = System.currentTimeMillis();

        BufferedReader bufferedReaderOfRawInput = new BufferedReader(new FileReader(inputFile));
        Set<String> wordTerms = new TreeSet<>();

        String line = bufferedReaderOfRawInput.readLine();
        while ((line != null)) {
            if (!line.isEmpty()) {
                String[] splitArray = line.split(":", 2);
                String field = splitArray[0];
                String value = splitArray[1];
                if (field.equals(REVIEW_TEXT_FIELD)) {
                    addLineOfTextToTermSet(wordTerms, value);
                }
            } else {
                incrementReviewCounter();
            }
            line = bufferedReaderOfRawInput.readLine();
        }

        bufferedReaderOfRawInput.close();
        System.out.println("token counter: " + tokenCounter);
        System.out.format("wordTerms.size(): %d\n", wordTerms.size());
        PrintingTool.printElapsedTime(startTime, "Building set of words");
        return wordTerms;
    }



    private void createMappingForSet(Set<String> terms, Map<String, Integer> termToTermId) {
        int termCounter = 1; // so tids and rids are always non-zero.
        for (Iterator<String> setIterator = terms.iterator(); setIterator.hasNext(); ) {
            termToTermId.put(setIterator.next(), termCounter);
            termCounter++;
            setIterator.remove();
        }
    }

    private void externalSort() {
        long startTime = System.currentTimeMillis(); // words
        File mergeFilesDirectory = wordsTermToReviewBlockWriter.getMergeFilesDirectory();
        int blockSizeInPairs = wordsTermToReviewBlockWriter.getBLOCK_SIZE_IN_INT_PAIRS();
        new ExternalMergeSort(currentIndexDirectory, mergeFilesDirectory, blockSizeInPairs);
        PrintingTool.printElapsedTime(startTime, "Words Sort Merging");
    }

    private void firstSortIteration(String inputFile, int initialReviewCounter) throws IOException {
        System.out.println("First sort iteration...");
        long blockWriterStartTime = System.currentTimeMillis();

        BufferedReader bufferedReaderOfRawInput = new BufferedReader(new FileReader(inputFile));
        wordsTermToReviewBlockWriter = new TermToReviewBlockWriter(currentIndexDirectory.getAbsolutePath(), tokenCounter, WORDS_MAPPING);
        resetReviewCounterTo(initialReviewCounter);
        StringBuilder reviewConcatFields = new StringBuilder();

        String line = bufferedReaderOfRawInput.readLine();
        while ((line != null)) {
            if (!line.isEmpty()) {
                String[] splitArray = line.split(":", 2);
                String field = splitArray[0];
                String value = splitArray[1];
                handleLine(field, value, reviewConcatFields);
            }
            line = bufferedReaderOfRawInput.readLine();
        }
        //closing first iteration
        bufferedReaderOfRawInput.close();
        wordsTermToReviewBlockWriter.closeWriter();
        PrintingTool.printElapsedTime(blockWriterStartTime, "First Sort Iteration Time: ");
    }

    private void handleLine(String field, String value, StringBuilder reviewConcatFields){
        switch (field) {
            case PID_FIELD:
                reviewConcatFields.append(reviewCounter);
                reviewConcatFields.append(value);
                break;
            case HELPFULNESS_FIELD:
                reviewConcatFields.append(extractHelpfulness(value));
                break;
            case SCORE_FIELD:
                reviewConcatFields.append(value.split("\\.")[0]);
                break;
            case REVIEW_TEXT_FIELD:
                reviewConcatFields.append(WHITE_SPACE_SEPARATOR).append(feedTextToBlockWriter(value));
                reviewsMetaDataIndexWriter.writeData(reviewConcatFields.toString());
                reviewConcatFields.replace(0, reviewConcatFields.length(), "");
                incrementReviewCounter();
                break;
            default:
                break;
        }
    }

    private int feedTextToBlockWriter(String reviewTextLine) {
        List<String> filteredSortedTokens = textToNormalizedTokens(reviewTextLine);
        int addedWordsCounter = 0;
        for (String tokenInReview : filteredSortedTokens) {
            if (tokenInReview.length() <= WORD_MAX_SIZE) {
                int termID = wordTermToTermID.get(tokenInReview);
                wordsTermToReviewBlockWriter.add(termID, reviewCounter);
                addedWordsCounter++;
            }
        }
        return addedWordsCounter;
    }

    private void addLineOfTextToTermSet(Set<String> terms, String reviewTextLine) {
        List<String> filteredTokens = textToNormalizedTokens(reviewTextLine);
        for (String token : filteredTokens) {
            assert !token.equals("");
            if (token.length() <= WORD_MAX_SIZE) {
                terms.add(token.toLowerCase()); // large words are counted but not added
            }
            incrementTokenCounter();
        }
    }



    private void constructIndexFromSorted(Map<Integer, String> wordsTermIdToTerm) throws IOException {
        // rids
        reviewsMetaDataIndexWriter.closeWriter(); // was written during first sort iteration

        // words
        int readBlockSize = estimateBestSizeOfWordsBlocks(tokenCounter, false);
        int numOfWordsInFrontCodeBlock = calculateNumOfTokensInFrontCodeBlock(tokenCounter);
        wordsDataIndexWriter.writeFromSortedFileByBlocks(numOfWordsInFrontCodeBlock, readBlockSize, wordsTermIdToTerm, reviewCounter);

        deleteSortedFile(currentIndexDirectory);
    }


    private void incrementTokenCounter() {
        tokenCounter++;
    }

    private void incrementReviewCounter() {
        reviewCounter++;
    }

    private void resetReviewCounterTo(int initialReviewCounter) {
        this.reviewCounter = initialReviewCounter;
    }

    /** Looks for the corresponding BYTES in the invalidation vector file and "flips" them to 1
     * @param indexDirectory - directory of the main index, aka "indexes"
     * @param ridsToDelete - review ids to delete. If not in range, will ignore.
     */
    public void removeReviews(String indexDirectory, int[] ridsToDelete) {
        IndexInvalidationTool.addToInvalidationFile(indexDirectory, ridsToDelete);
    }

    /**
     * Merges all indexes into one index.
     * @param indexReader - Uses IndexReader to read from all indexes
     */
    public void merge(IndexReader indexReader) {
        // reading rows of all indexes
        IndexMergingModerator indexMergingModerator = indexReader.getIndexMergingModeratorRegularMerge();
        // makes each row read from the moderator written as one index.
        IndexMergeWriter indexMergeWriter = new IndexMergeWriter(allIndexesDirectory.getAbsolutePath());
        File mergedDirectory = indexMergeWriter.merge(indexMergingModerator);
        emptyInvalidationFile();
        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeFilesAfterMerge(allIndexesDirectory.getAbsolutePath());
        moveMergedFilesToMainIndex(mergedDirectory);
        this.reviewsMetaDataIndexWriter = new ReviewsMetaDataIndexWriter(allIndexesDirectory.getAbsolutePath());
    }

    private void emptyInvalidationFile() {
        IndexInvalidationTool.emptyInvalidationFile(allIndexesDirectory.getAbsolutePath());
    }

    private void moveMergedFilesToMainIndex(File mergedDirectory){
        Path mainDirectory = allIndexesDirectory.toPath();
        try {
            File[] mergedIndexFiles = mergedDirectory.listFiles();
            if(mergedIndexFiles != null){
                for(File mergedIndexFile: mergedIndexFiles){
                    Files.move(mergedIndexFile.toPath(), mainDirectory.resolve(mergedIndexFile.getName()));
                }
            }
            Files.delete(mergedDirectory.toPath());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(3);
        }
    }
}