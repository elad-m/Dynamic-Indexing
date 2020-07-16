package dynamic_index;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_reading.ReviewsMetaDataIndexReader;
import dynamic_index.index_reading.SingleIndexReader;

import java.io.*;
import java.nio.file.Files;
import java.util.*;


/**
 * Reads all indexes - main and auxiliaries if exist - and put the results together while considering
 * deleted review IDs.
 */
public class IndexReader {

    // place of main index and its auxiliary indexes directories
    private final File mainIndexDirectory;
    private File mainInvertedIndexFile;
    private final ReviewsMetaDataIndexReader reviewMetaDataIndexReader;

    // main index data
    private byte[] mainIndexDictionary;
    private byte[] mainConcatString;
    private int mainNumOfWordsInFrontCodeBlock;

    // auxiliary index data
    private int numOfSubIndexes = 0;
    private File[] subInvertedIndexFiles;
    private byte[][] subIndexesDictionary;
    private byte[][] subIndexesConcatString;
    private int[] subNumOfWordsInFrontCodeBlock;

    //======================= Loading and Initializing  =======================//
    /**
     * Creates an IndexReader which will read from the given directory, including all auxiliary indexes
     * it might have.
     * @param dir - directory where the indexes should be
     */
    public IndexReader(String dir) {
        this.mainIndexDirectory = new File(dir);
        this.reviewMetaDataIndexReader = new ReviewsMetaDataIndexReader(mainIndexDirectory);
        loadAllIndexesWithMain();
    }

    /**
     * For getting a log-merger.
     * @param dir - directory in which all indexes are.
     * @param indexFilesToMerge - index directories that would be merged.
     */
    IndexReader(String dir, Collection<File> indexFilesToMerge) {
        this.mainIndexDirectory = new File(dir);
        this.reviewMetaDataIndexReader = new ReviewsMetaDataIndexReader(mainIndexDirectory);
        this.numOfSubIndexes = indexFilesToMerge.size();
        loadNFirstIndexes(indexFilesToMerge);
    }

    /**
     * For querying the index from outside the package.
     * @param dir - directory in which all the index directories are
     * @param dummyForLogMerge - a way to invoke the constructor that does not use/look for
     *                         a main index, i.e. files in the main directory.
     */
    public IndexReader(String dir, boolean dummyForLogMerge){
        this.mainIndexDirectory = new File(dir);
        this.reviewMetaDataIndexReader = new ReviewsMetaDataIndexReader(mainIndexDirectory);
        List<File> allDirectories = Arrays.asList(Objects.requireNonNull(mainIndexDirectory.listFiles(File::isDirectory)));
        this.numOfSubIndexes = allDirectories.size();
        loadNFirstIndexes(allDirectories);
    }

    private void loadNFirstIndexes(Collection<File> indexFilesToMerge) {
        try {
            instantiateSubIndexArrays();
            int i = 0;
            for(File indexDir: indexFilesToMerge){
                loadSingleSubIndex(indexDir, i);
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadAllIndexesWithMain() {
        try {
            loadMainIndex();
            File[] subIndexDirectories = getAuxIndexDirectories();
            numOfSubIndexes = subIndexDirectories.length;
            instantiateSubIndexArrays();
            for (int i = 0; i < numOfSubIndexes; i++) {
                loadSingleSubIndex(subIndexDirectories[i], i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private File[] getAuxIndexDirectories() {
        return mainIndexDirectory.getAbsoluteFile().listFiles(File::isDirectory);
    }

    private void instantiateSubIndexArrays() {
        subIndexesDictionary = new byte[numOfSubIndexes][];
        subIndexesConcatString = new byte[numOfSubIndexes][];
        subNumOfWordsInFrontCodeBlock = new int[numOfSubIndexes];
        subInvertedIndexFiles = new File[numOfSubIndexes];
    }

    private void loadSingleSubIndex(File auxIndexDirectory, int index_i) throws IOException {
        File auxDictionaryFile = new File(auxIndexDirectory.getPath()
                + File.separator + MiscTools.WORDS_FRONT_CODED_FILENAME);
        File auxStringConcatFile = new File(auxIndexDirectory.getPath()
                + File.separator + MiscTools.WORDS_CONCAT_FILENAME);
        File auxInvertedIndexFile = new File(auxIndexDirectory.getPath()
                + File.separator + MiscTools.WORDS_INVERTED_INDEX_FILENAME);

        assert auxDictionaryFile.exists() && auxStringConcatFile.exists() && auxInvertedIndexFile.exists();

        subIndexesDictionary[index_i] = Files.readAllBytes(auxDictionaryFile.toPath());
        subIndexesConcatString[index_i] = Files.readAllBytes(auxStringConcatFile.toPath());
        subInvertedIndexFiles[index_i] = auxInvertedIndexFile;
        loadAuxNumOfTokensPerBlock(index_i);
    }

    private void loadAuxNumOfTokensPerBlock(int index_i) {
        subNumOfWordsInFrontCodeBlock[index_i] = MiscTools.BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    }

    private void loadMainIndex() throws IOException {
        File mainDictionaryFile = new File(mainIndexDirectory.getPath()
                + File.separator + MiscTools.WORDS_FRONT_CODED_FILENAME);
        File mainStringConcatFile = new File(mainIndexDirectory.getPath()
                + File.separator + MiscTools.WORDS_CONCAT_FILENAME);
        mainInvertedIndexFile = new File(mainIndexDirectory.getPath()
                + File.separator + MiscTools.WORDS_INVERTED_INDEX_FILENAME);

        assert mainDictionaryFile.exists() && mainStringConcatFile.exists()
                && mainInvertedIndexFile.exists();

        mainIndexDictionary = Files.readAllBytes(mainDictionaryFile.toPath());
        mainConcatString = Files.readAllBytes(mainStringConcatFile.toPath());
        loadMainNumOfTokensPerBlock();
    }


    private void loadMainNumOfTokensPerBlock() {
        mainNumOfWordsInFrontCodeBlock = MiscTools.BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    }

    //======================= Querying (Reading)  =======================//
    /**
     * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
     * that id-n is the n-th review containing the given token and freq-n is the
     * number of times that the token appears in review id-n
     * Note that the integers should be sorted by id
     * <p>
     * Returns an empty Enumeration if there are no reviews containing this token
     */
    public Enumeration<Integer> getReviewsWithToken(String token) {
        Map<Integer, Integer> unionOfResults = getPostingsListOfToken(token);
        return mapToEnumeration(unionOfResults);
    }

    private Map<Integer,Integer> getPostingsListOfToken(String token){
        TreeMap<Integer, Integer> unionOfResults = new TreeMap<>();
        addMainIndexResults(unionOfResults, token);
        addAuxIndexesResults(unionOfResults, token);
        return unionOfResults;
    }

    /**
     * The same as {@link #getReviewsWithToken(String)} ()} but here we use also an in-memory index.
     * This method should be called when using logMerge indexing.
     * @param token - token to find its postings list.
     * @param logMergeIndexWriter - i.e. index writer using LogMerging, since it holds in-memory index
     * @return the same as above.
     */
    public Enumeration<Integer> getReviewsWithToken(String token,
                                                    LogMergeIndexWriter logMergeIndexWriter){
        Map<Integer, Integer>  postingList =getPostingsListOfToken(token, logMergeIndexWriter);
        return mapToEnumeration(postingList);
    }

    private Map<Integer,Integer> getPostingsListOfToken(String token,
                                                        LogMergeIndexWriter logMergeIndexWriter){
        Map<Integer, Integer> unionOfResults = new TreeMap<>();
        addAuxIndexesResults(unionOfResults, token);
        unionOfResults.putAll(logMergeIndexWriter.getReviewsWithToken(token));
        return unionOfResults;
    }


    private void addAuxIndexesResults(Map<Integer, Integer> unionOfResults, String token) {
        SingleIndexReader singleIndexReader;
        for (int i = 0; i < numOfSubIndexes; i++) {
            singleIndexReader = new SingleIndexReader(subIndexesDictionary[i],
                    subIndexesConcatString[i],
                    subInvertedIndexFiles[i],
                    subNumOfWordsInFrontCodeBlock[i],
                    mainIndexDirectory);
            Map<Integer, Integer> auxResults = singleIndexReader.getReviewsWithWord(token);
            unionOfResults.putAll(auxResults);
        }
    }

    private void addMainIndexResults(TreeMap<Integer, Integer> unionOfResults, String token) {
        SingleIndexReader singleIndexReader =
                new SingleIndexReader(mainIndexDictionary,
                        mainConcatString,
                        mainInvertedIndexFile,
                        mainNumOfWordsInFrontCodeBlock, mainIndexDirectory);
        TreeMap<Integer, Integer> mainResults = singleIndexReader.getReviewsWithWord(token);
        unionOfResults.putAll(mainResults);
    }

    private Enumeration<Integer> mapToEnumeration(Map<Integer, Integer> unionOfResults) {
        Vector<Integer> toEnumerate = new Vector<>();
        for (Map.Entry<Integer, Integer> entry : unionOfResults.entrySet()) {
            toEnumerate.add(entry.getKey());
            toEnumerate.add(entry.getValue());
        }
        return toEnumerate.elements();
    }

    /*
     * The following 4 methods are here in any cases but most times shouldn't be used.
     * Since the data that these methods bring is used usually for query processing
     * and since most of the time the full postings list of a token in the query
     * would be queried any way, it is better to not use these.
     *
     * The alternative of storing this data in a file in construction is impractical for
     * the deletion functionality: there is no efficient way to update a tokens #mentions
     * after deleting some review because the only way to know this token is in this review is
     * to look for the token's postings list.
     *
     * (you can also delete not only by number but also
     * by the review text itself but I decided this is not realistic).
     */

    /**
     * @param token - a word
     * @return Number of times a token was mentioned in the index.
     */
    public int getNumberOfMentions(String token){
        int sum = 0;
        for(Integer freq: getPostingsListOfToken(token).values()) {
            sum += freq;
        }
        return sum;
    }

    /**
     * LogMerge variation to the above
     * @param token - a word
     * @return Number of times a token was mentioned in the index.
     */
    public int getNumberOfMentions(String token, LogMergeIndexWriter logMergeIndexWriter){
        int sum = 0;
        for(Integer freq: getPostingsListOfToken(token, logMergeIndexWriter).values()) {
            sum += freq;
        }
        return sum;
    }

    /**
     * @param token - a word
     * @return - number of reviews that have token in them.
     */
    public int getNumberOfReviews(String token){
        return getPostingsListOfToken(token).size();
    }

    /**
     * LogMerge variation to the above
     * @param token - a word
     * @return - number of reviews that have token in them.
     */
    public int getNumberOfReviews(String token, LogMergeIndexWriter logMergeIndexWriter){
        return getPostingsListOfToken(token, logMergeIndexWriter).size();
    }


    /**
     * Returns the product identifier for the given review
     * Returns null if there is no review with the given identifier
     */
    public String getProductId(int reviewId) {
        return reviewMetaDataIndexReader.getProductId(reviewId);
    }

    /**
     * Returns the score for a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewScore(int reviewId) {
        return reviewMetaDataIndexReader.getReviewScore(reviewId);
    }

    /**
     * Returns the numerator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessNumerator(int reviewId) {
        return reviewMetaDataIndexReader.getReviewHelpfulnessNumerator(reviewId);
    }

    /**
     * Returns the denominator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessDenominator(int reviewId) {
        return reviewMetaDataIndexReader.getReviewHelpfulnessDenominator(reviewId);
    }

    /**
     * Returns the number of tokens in a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewLength(int reviewId) {
        return reviewMetaDataIndexReader.getReviewLength(reviewId);
    }


    /**
     * @return Number of reviews in the index minus the deleted ones
     */
    public int getNumberOfReviews() {
        return reviewMetaDataIndexReader.getTotalNumberOfReviews();
    }

    /**
     * @return - Number of tokens in all reviews except for the deleted ones (which were already taken care of
     * in the constructor)
     */
    public int getTotalNumberOfTokens(){
        return reviewMetaDataIndexReader.getTotalNumberOfTokens();
    }


    //======================= Merge Moderators  =======================//
    /**
     * Creates and returns IndexMergingModerator for the regular merge: merging all indexes and having main index.
     * @return - IndexMergingModerator with all indexes - main and all auxiliaries - added to it.
     */
    public IndexMergingModerator getIndexMergingModeratorRegularMerge() {
        IndexMergingModerator indexMergingModerator = new IndexMergingModerator();

        // adding main index
        SingleIndexReader singleIndexReader = new SingleIndexReader(mainIndexDictionary,
                mainConcatString,
                mainInvertedIndexFile,
                mainNumOfWordsInFrontCodeBlock,
                mainIndexDirectory);
        indexMergingModerator.add(singleIndexReader);

        // adding auxiliary indexes
        indexMergingModerator.addAll(getAllSingleIndexReaders());
        // merging the review meta data (filtering deleted reviews)
        reviewMetaDataIndexReader.rewriteReviewMetaData();
        return indexMergingModerator;
    }

    private List<SingleIndexReader> getAllSingleIndexReaders(){
        List<SingleIndexReader> singleIndexReaders = new ArrayList<>();
        for (int i = 0; i < numOfSubIndexes; i++) {
            SingleIndexReader singleIndexReader = new SingleIndexReader(subIndexesDictionary[i],
                    subIndexesConcatString[i],
                    subInvertedIndexFiles[i],
                    subNumOfWordsInFrontCodeBlock[i],
                    mainIndexDirectory);
            singleIndexReaders.add(singleIndexReader);
        }
        return singleIndexReaders;
    }

    /**
     * Creates and returns IndexMergingModerator for the log-merge: merging all numOfSubIndexes directories: so
     * not necessarily all sub-indexes, but all sub-indexes that should be merged.
     * Should be only used when initializes with the log-merge constructor, i.e. no main index.
     * @return IndexMergingModerator with all indexes according to constructor index initialization.
     */
    public IndexMergingModerator getIndexMergingModeratorLogMerge(){
        IndexMergingModerator indexMergingModerator = new IndexMergingModerator();
        indexMergingModerator.addAll(getAllSingleIndexReaders());
        return indexMergingModerator;
    }

    public void rewriteReviewMetaDataMerge(){
        reviewMetaDataIndexReader.rewriteReviewMetaData();
    }




}