package dynamic_index;

import dynamic_index.external_sort.ExternalMergeSort;
import dynamic_index.external_sort.TermToReviewBlockWriter;
import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_writing.IndexMergeWriter;
import dynamic_index.index_writing.ExternalIndexWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static dynamic_index.Statics.*;

public class IndexWriter {

    private final String mainIndexDirectory;
    private File currentIndexDirectory;

    private ExternalIndexWriter wordsDataIndexWriter;
    private final Map<String, Integer> wordTermToTermID = new HashMap<>();
    private TermToReviewBlockWriter wordsTermToReviewBlockWriter;

    private int reviewCounter = 1; // not necessarily the number of reviews in index in practice because deletion
    private int tokenCounter = 0; // token counter only incremented in the mapping stage
    private final int inputScaleType;

    // DEBUGGING VARIABLES
//    private final boolean SKIP_SORTING = false; // when true, the next two are unused

    public IndexWriter(String indexDirectory, int inputScaleType) {
        this.mainIndexDirectory = indexDirectory;
        this.inputScaleType = inputScaleType;
    }

    /**
     * Builds an index from a given product raw review data.
     *
     * @param inputFile          - product review raw data
     * @param mainIndexDirectory - directory to create the index in
     */
    public void constructIndex(String inputFile, String mainIndexDirectory) {
        createIndexFilesDirectory(mainIndexDirectory);
        instantiateWriters();
        constructIndex(inputFile, reviewCounter);
    }

    /**
     * Builds an auxiliary index of a given product review data
     *
     * @param inputFile         - product review raw data
     * @param auxIndexDirectory - directory for the auxiliary index. Should be inside the
     *                          main index directory.
     */
    public void insert(String inputFile, String auxIndexDirectory) {
        createIndexFilesDirectory(auxIndexDirectory);
        instantiateWriters();
        constructIndex(inputFile, reviewCounter);
    }

    private void instantiateWriters() {
        wordsDataIndexWriter = new ExternalIndexWriter(currentIndexDirectory);
    }


    private void createIndexFilesDirectory(String dir) {
        this.currentIndexDirectory = createDirectory(dir);
    }

    /*
    Using the first external sort algorithm (sort-merge).
     */
    private void constructIndex(String inputFile, final int initialReviewCounter) {
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
            constructIndexFromSorted(wordTermIdToTerm, initialReviewCounter);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void writeHashmapFor100Random() {
        writeHashmapToFile(wordTermToTermID, currentIndexDirectory, WORDS_MAPPING, inputScaleType);
    }

    private void constructTermToTermIDMapping(String inputFile) throws IOException {
        System.out.println("Starting term mapping:");
//        long startTime = System.currentTimeMillis();

        Set<String> wordTerms = getTermsSorted(inputFile); // first reading of whole input file
        createMappingForSet(wordTerms, wordTermToTermID);

        wordTerms.clear();
//        printElapsedTime(startTime, " Building Term To Term ID mapping: ");
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
        printElapsedTime(startTime, "Building set of words");
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
        printElapsedTime(startTime, "Words Sort Merging");
    }

    private void firstSortIteration(String inputFile, int initialReviewCounter) throws IOException {
        System.out.println("First sort iteration...");
        long blockWriterStartTime = System.currentTimeMillis();
        BufferedReader bufferedReaderOfRawInput = new BufferedReader(new FileReader(inputFile));
        wordsTermToReviewBlockWriter = new TermToReviewBlockWriter(currentIndexDirectory.getAbsolutePath(), tokenCounter, WORDS_MAPPING);

        resetReviewCounterTo(initialReviewCounter);
        String line = bufferedReaderOfRawInput.readLine();
        while ((line != null)) {
            if (!line.isEmpty()) {
                String[] splitArray = line.split(":", 2);
                String field = splitArray[0];
                String value = splitArray[1];
                if (field.equals(REVIEW_TEXT_FIELD)) {
                    feedTextToBlockWriter(value); // first stage of sort
                    incrementReviewCounter();
                }
            }
            line = bufferedReaderOfRawInput.readLine();
        }
        //closing first iteration
        bufferedReaderOfRawInput.close();
        wordsTermToReviewBlockWriter.closeWriter();
        printElapsedTime(blockWriterStartTime, "First Sort Iteration Time: ");
    }

    private void feedTextToBlockWriter(String reviewTextLine) {
        List<String> filteredSortedTokens = textToNormalizedTokens(reviewTextLine);
        for (String tokenInReview : filteredSortedTokens) {
            if (tokenInReview.length() <= WORD_MAX_SIZE) {
                int termID = wordTermToTermID.get(tokenInReview);
                wordsTermToReviewBlockWriter.add(termID, reviewCounter);
            }
        }
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



    private void constructIndexFromSorted(Map<Integer, String> wordsTermIdToTerm, int initialReviewCounter) throws IOException {
        // words
        int readBlockSize = estimateBestSizeOfWordsBlocks(tokenCounter, false);
        int numOfWordsInFrontCodeBlock = calculateNumOfTokensInFrontCodeBlock(tokenCounter);
        wordsDataIndexWriter.writeFromSortedFileByBlocks(numOfWordsInFrontCodeBlock, readBlockSize, wordsTermIdToTerm, reviewCounter);

//        if (!SKIP_SORTING)
//            writeMetaData();
        writeToInvalidationVector(mainIndexDirectory, initialReviewCounter);
        deleteSortedFile(currentIndexDirectory);
    }

    private void writeToInvalidationVector(String mainIndexPath, int initialReviewCounter) throws IOException {
        int numberOfReviewsAdded = reviewCounter - initialReviewCounter;
        RandomAccessFile raValidationVectorFile = new RandomAccessFile(
                mainIndexPath + File.separator + INVALIDATION_VECTOR_FILENAME, "rw");
        raValidationVectorFile.seek(initialReviewCounter);
        for(int i = 0; i < numberOfReviewsAdded; i++){
            raValidationVectorFile.writeByte(0);
        }
    }

//    private void writeMetaData()
//            throws IOException {
//        RandomAccessFile raMetaFile = new RandomAccessFile(
//                currentIndexDirectory.getPath() + File.separator + MAIN_INDEX_META_DATA_FILENAME, "rw");
//        raMetaFile.writeInt(reviewCounter - 1);
//        raMetaFile.writeInt(tokenCounter);
//        raMetaFile.writeInt(numOfWordsInFrontCodeBlock);
//        raMetaFile.close();
//    }

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
        try{
            RandomAccessFile raValidationVectorFile = new RandomAccessFile(
                    indexDirectory + File.separator + INVALIDATION_VECTOR_FILENAME, "rw");
            for(int rid: ridsToDelete){
                raValidationVectorFile.seek(rid - 1);
                raValidationVectorFile.writeByte(1);
            }
            setInvalidationVectorIsDirty(true);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Merges all indexes into one index.
     * @param indexReader - Uses IndexReader to read from all indexes
     */
    public void merge(IndexReader indexReader) {
        // reading rows of all indexes
        IndexMergingModerator indexMergingModerator = indexReader.getIndexMergingModeratorRegularMerge();
        // makes each row read from the moderator written as one index.
        IndexMergeWriter indexMergeWriter = new IndexMergeWriter(mainIndexDirectory);
        File mergedDirectory = indexMergeWriter.merge(indexMergingModerator);
        setInvalidationVectorIsDirty(false);
        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeFilesAfterMerge(mainIndexDirectory);
        moveMergedFilesToMainIndex(mergedDirectory);
    }

    private void moveMergedFilesToMainIndex(File mergedDirectory){
        Path mainDirectory = (new File(mainIndexDirectory)).toPath();
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