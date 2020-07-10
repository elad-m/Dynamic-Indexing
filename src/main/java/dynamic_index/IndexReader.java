package dynamic_index;

import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_reading.SingleIndexReader;
import dynamic_index.index_structure.InvertedIndex;

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

    // main index data
    private byte[] mainIndexDictionary;
    private byte[] mainConcatString;
    private int mainNumOfWordsInFrontCodeBlock;
    private File invalidationVector;

    // auxiliary index data
    private int numOfSubIndexes = 0;
    private File[] subInvertedIndexFiles;
    private byte[][] subIndexesDictionary;
    private byte[][] subIndexesConcatString;
    private int[] subNumOfWordsInFrontCodeBlock;

    /**
     * Creates an IndexReader which will read from the given directory, including all auxiliary indexes
     * it might have.
     * @param dir - directory where the indexes should be
     */
    public IndexReader(String dir) {
        this.mainIndexDirectory = new File(dir);
        loadAllIndexesWithMain();
    }

    /**
     * For getting a log-merger.
     * @param dir - directory in which all indexes are.
     * @param allIndexFiles - all index files in mainIndexDirectory
     * @param numberOfIndexDirectories - read to memory only this number of indexes
     */
    IndexReader(String dir, Collection<File> allIndexFiles, int numberOfIndexDirectories) {
        this.mainIndexDirectory = new File(dir);
        this.numOfSubIndexes = numberOfIndexDirectories;
        loadNFirstIndexes(allIndexFiles);
    }

    /**
     * For querying the index from outside the package.
     * @param dir - directory in which all the index directories are
     * @param dummyForLogMerge - a way to invoke the constructor that does not use/look for
     *                         a main index, i.e. files in the main directory.
     */
    public IndexReader(String dir, boolean dummyForLogMerge){
        this.mainIndexDirectory = new File(dir);
        List<File> allFiles = Arrays.asList(Objects.requireNonNull(mainIndexDirectory.listFiles(File::isDirectory)));
        this.numOfSubIndexes = allFiles.size();
        loadNFirstIndexes(allFiles);
    }

    private void loadNFirstIndexes(Collection<File> allIndexFiles) {
        try {
            assert numOfSubIndexes <= allIndexFiles.size();
            instantiateSubIndexArrays();
            int i = 0;
            for(File indexDir: allIndexFiles){
                if(i == numOfSubIndexes){
                    break;
                }
                loadSingleSubIndex(indexDir, i);
                i++;
            }
            instantiateInvalidationVector();
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
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File auxStringConcatFile = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        File auxInvertedIndexFile = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);

        assert auxDictionaryFile.exists() && auxStringConcatFile.exists() && auxInvertedIndexFile.exists();

        subIndexesDictionary[index_i] = Files.readAllBytes(auxDictionaryFile.toPath());
        subIndexesConcatString[index_i] = Files.readAllBytes(auxStringConcatFile.toPath());
        subInvertedIndexFiles[index_i] = auxInvertedIndexFile;
        loadAuxNumOfTokensPerBlock(auxIndexDirectory, index_i);
    }

    private void loadAuxNumOfTokensPerBlock(File indexDirectory, int index_i) {
        subNumOfWordsInFrontCodeBlock[index_i] = Statics.BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    }

    private void loadMainIndex() throws IOException {
        File mainDictionaryFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File mainStringConcatFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        mainInvertedIndexFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        instantiateInvalidationVector();

        assert mainDictionaryFile.exists() && mainStringConcatFile.exists()
                && mainInvertedIndexFile.exists() && invalidationVector.exists();

        mainIndexDictionary = Files.readAllBytes(mainDictionaryFile.toPath());
        mainConcatString = Files.readAllBytes(mainStringConcatFile.toPath());
        loadMainNumOfTokensPerBlock();
    }

    private void instantiateInvalidationVector(){
        invalidationVector = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.INVALIDATION_VECTOR_FILENAME);
    }

    private void loadMainNumOfTokensPerBlock() {
        mainNumOfWordsInFrontCodeBlock = Statics.BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    }


    /**
     * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
     * that id-n is the n-th review containing the given token and freq-n is the
     * number of times that the token appears in review id-n
     * Note that the integers should be sorted by id
     * <p>
     * Returns an empty Enumeration if there are no reviews containing this token
     */
    public Enumeration<Integer> getReviewsWithToken(String token) {
        TreeMap<Integer, Integer> unionOfResults = new TreeMap<>();
        addMainIndexResults(unionOfResults, token);
        addAuxIndexesResults(unionOfResults, token);
        return mapToEnumeration(unionOfResults);
    }

    /**
     * The same as {@link #getReviewsWithToken(String)} ()} but here we use also an in-memory index.
     * This method should be called when using logMerge indexing.
     * @param token - token to find its postings list.
     * @param continuousIndexWriter - i.e. index writer using LogMerging
     * @return the same as above.
     */
    public Enumeration<Integer> getReviewsWithToken(String token,
                                                    ContinuousIndexWriter continuousIndexWriter){
        TreeMap<Integer, Integer> unionOfResults = new TreeMap<>();
        addAuxIndexesResults(unionOfResults, token);
        unionOfResults.putAll(continuousIndexWriter.getReviewsWithToken(token));
        return mapToEnumeration(unionOfResults);
    }


    private void addAuxIndexesResults(TreeMap<Integer, Integer> unionOfResults, String token) {
        SingleIndexReader singleIndexReader;
        for (int i = 0; i < numOfSubIndexes; i++) {
            singleIndexReader = new SingleIndexReader(subIndexesDictionary[i],
                    subIndexesConcatString[i],
                    subInvertedIndexFiles[i],
                    invalidationVector,
                    subNumOfWordsInFrontCodeBlock[i], mainIndexDirectory);
            TreeMap<Integer, Integer> auxResults = singleIndexReader.getReviewsWithWord(token);
            unionOfResults.putAll(auxResults);
        }
    }

    private void addMainIndexResults(TreeMap<Integer, Integer> unionOfResults, String token) {
        SingleIndexReader singleIndexReader =
                new SingleIndexReader(mainIndexDictionary,
                        mainConcatString,
                        mainInvertedIndexFile,
                        invalidationVector,
                        mainNumOfWordsInFrontCodeBlock, mainIndexDirectory);
        TreeMap<Integer, Integer> mainResults = singleIndexReader.getReviewsWithWord(token);
        unionOfResults.putAll(mainResults);
    }

    private Enumeration<Integer> mapToEnumeration(TreeMap<Integer, Integer> unionOfResults) {
        Vector<Integer> toEnumerate = new Vector<>();
        for (Map.Entry<Integer, Integer> entry : unionOfResults.entrySet()) {
            toEnumerate.add(entry.getKey());
            toEnumerate.add(entry.getValue());
        }
        return toEnumerate.elements();
    }

    /**
     * Creates and returns IndexMergingModerator for the regular merge: merging all indexes and having main index.
     * @return - IndexMergingModerator with all indexes - main and all auxiliaries - added to it.
     */
    public IndexMergingModerator getIndexMergingModeratorRegularMerge() {
        IndexMergingModerator indexMergingModerator = new IndexMergingModerator();

        // adding main index
        SingleIndexReader singleIndexReader = new SingleIndexReader(mainIndexDictionary,
                mainConcatString, mainInvertedIndexFile, invalidationVector,mainNumOfWordsInFrontCodeBlock, mainIndexDirectory);
        indexMergingModerator.add(singleIndexReader);

        // adding auxiliary indexes
        indexMergingModerator.addAll(getAllSingleIndexReaders());
        return indexMergingModerator;
    }

    private List<SingleIndexReader> getAllSingleIndexReaders(){
        List<SingleIndexReader> singleIndexReaders = new ArrayList<>();
        for (int i = 0; i < numOfSubIndexes; i++) {
            SingleIndexReader singleIndexReader = new SingleIndexReader(subIndexesDictionary[i],
                    subIndexesConcatString[i],
                    subInvertedIndexFiles[i],
                    invalidationVector,
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


}