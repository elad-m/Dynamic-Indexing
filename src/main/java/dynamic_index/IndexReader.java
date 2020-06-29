package dynamic_index;

import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_reading.SingleIndexReader;

import java.io.*;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;


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
    private int numOfAuxIndexes = 0;
    private File[] auxInvertedIndexFiles;
    private byte[][] auxIndexesDictionary;
    private byte[][] auxIndexesConcatString;
    private int[] auxNumOfWordsInFrontCodeBlock;

    /**
     * Creates an IndexReader which will read from the given directory, including all auxiliary indexes
     * it might have.
     */
    public IndexReader(String dir) {
        this.mainIndexDirectory = new File(dir);
        loadIndexes();
    }

    private void loadIndexes() {
        try {
            loadMainIndex();
            File[] auxIndexDirectories = getAuxIndexDirectories();
            numOfAuxIndexes = auxIndexDirectories.length;
            instantiateAuxIndexArrays();
            for (int i = 0; i < numOfAuxIndexes; i++) {
                loadSingleAuxIndex(auxIndexDirectories[i], i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private File[] getAuxIndexDirectories() {
        return mainIndexDirectory.getAbsoluteFile().listFiles(File::isDirectory);
    }

    private void instantiateAuxIndexArrays() {
        auxIndexesDictionary = new byte[numOfAuxIndexes][];
        auxIndexesConcatString = new byte[numOfAuxIndexes][];
        auxNumOfWordsInFrontCodeBlock = new int[numOfAuxIndexes];
        auxInvertedIndexFiles = new File[numOfAuxIndexes];
    }

    private void loadSingleAuxIndex(File auxIndexDirectory, int index_i) throws IOException {
        File auxDictionaryFile = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File auxStringConcat = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        assert auxDictionaryFile.exists() && auxStringConcat.exists();
        auxIndexesDictionary[index_i] = Files.readAllBytes(auxDictionaryFile.toPath());
        auxIndexesConcatString[index_i] = Files.readAllBytes(auxStringConcat.toPath());
        auxInvertedIndexFiles[index_i] = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        loadAuxNumOfTokensPerBlock(auxIndexDirectory, index_i);
    }

    private void loadAuxNumOfTokensPerBlock(File indexDirectory, int index_i) {
        auxNumOfWordsInFrontCodeBlock[index_i] = Statics.BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
//        File indexMetaFile = new File(indexDirectory.getPath() + File.separator + Statics.MAIN_INDEX_META_DATA_FILENAME);
//        RandomAccessFile raIndexMeta;
//        raIndexMeta = new RandomAccessFile(indexMetaFile, "r");
//        raIndexMeta.seek(Statics.INTEGER_SIZE * 2);
//        auxNumOfWordsInFrontCodeBlock[index_i] = raIndexMeta.readInt();
//        System.out.format("Front block size for index%d: %d" + System.lineSeparator(),
//                index_i,
//                auxNumOfWordsInFrontCodeBlock[index_i]);
//        raIndexMeta.close();
    }

    private void loadMainIndex() throws IOException {
        File mainDictionaryFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File mainStringConcatFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        mainInvertedIndexFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        invalidationVector = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.INVALIDATION_VECTOR_FILENAME);

        assert mainDictionaryFile.exists() && mainStringConcatFile.exists()
                && mainInvertedIndexFile.exists() && invalidationVector.exists();

        mainIndexDictionary = Files.readAllBytes(mainDictionaryFile.toPath());
        mainConcatString = Files.readAllBytes(mainStringConcatFile.toPath());
        loadMainNumOfTokensPerBlock();
    }

    private void loadMainNumOfTokensPerBlock() {
        mainNumOfWordsInFrontCodeBlock = Statics.BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
//        File indexMetaFile = new File(mainIndexDirectory.getPath() + File.separator + Statics.MAIN_INDEX_META_DATA_FILENAME);
//        RandomAccessFile raIndexMeta;
//        raIndexMeta = new RandomAccessFile(indexMetaFile, "r");
//        raIndexMeta.seek(Statics.INTEGER_SIZE * 2);
//        mainNumOfWordsInFrontCodeBlock = raIndexMeta.readInt();
//        System.out.format("Front block size main: %d" + System.lineSeparator(),
//                mainNumOfWordsInFrontCodeBlock);
//        raIndexMeta.close();
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

    private void addAuxIndexesResults(TreeMap<Integer, Integer> unionOfResults, String token) {
        SingleIndexReader singleIndexReader;
        for (int i = 0; i < numOfAuxIndexes; i++) {
            singleIndexReader = new SingleIndexReader(auxIndexesDictionary[i],
                    auxIndexesConcatString[i],
                    auxInvertedIndexFiles[i],
                    invalidationVector,
                    auxNumOfWordsInFrontCodeBlock[i], mainIndexDirectory);
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

    public IndexMergingModerator getIndexMergingModerator() {
        IndexMergingModerator indexMergingModerator = new IndexMergingModerator();

        // adding main index
        SingleIndexReader singleIndexReader = new SingleIndexReader(mainIndexDictionary,
                mainConcatString, mainInvertedIndexFile, invalidationVector,mainNumOfWordsInFrontCodeBlock, mainIndexDirectory);
        indexMergingModerator.add(singleIndexReader);

        // adding auxiliary indexes
        for (int i = 0; i < numOfAuxIndexes; i++) {
            singleIndexReader = new SingleIndexReader(auxIndexesDictionary[i],
                    auxIndexesConcatString[i],
                    auxInvertedIndexFiles[i],
                    invalidationVector,
                    auxNumOfWordsInFrontCodeBlock[i],
                    mainIndexDirectory);
            indexMergingModerator.add(singleIndexReader);
        }
        return indexMergingModerator;
    }


}