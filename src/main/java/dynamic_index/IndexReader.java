package dynamic_index;

import dynamic_index.index_reading.SingleIndexReader;

import java.io.*;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;


/**
 * Reads all indexes - main and auxiliaries if exist - and put the results together.
 */
public class IndexReader {

    // place of main index and its auxiliary indexes directories
    private final File mainIndexDirectory;
    private File mainInvertedIndexFile;

    // main index data
    private byte[] mainIndexDictionary;
    private byte[] mainConcatString;
    private int mainNumOfWordsInFrontCodeBlock;

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
        File mainDictionaryFile = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File mainStringConcatFile = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        auxIndexesDictionary[index_i] = Files.readAllBytes(mainDictionaryFile.toPath());
        auxIndexesConcatString[index_i] = Files.readAllBytes(mainStringConcatFile.toPath());
        auxInvertedIndexFiles[index_i] = new File(auxIndexDirectory.getPath()
                + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        loadAuxNumOfTokensPerBlock(auxIndexDirectory, index_i);
    }

    private void loadAuxNumOfTokensPerBlock(File indexDirectory, int index_i) throws IOException {
        File indexMetaFile = new File(indexDirectory.getPath() + File.separator + Statics.MAIN_INDEX_META_DATA_FILENAME);
        RandomAccessFile raIndexMeta;
        raIndexMeta = new RandomAccessFile(indexMetaFile, "r");
        raIndexMeta.seek(Statics.INTEGER_SIZE * 2);
        auxNumOfWordsInFrontCodeBlock[index_i] = raIndexMeta.readInt();
        System.out.format("Front block size for index%d: %d" + System.lineSeparator(),
                index_i,
                auxNumOfWordsInFrontCodeBlock[index_i]);
        raIndexMeta.close();
    }

    private void loadMainIndex() throws IOException {
        File mainDictionaryFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File mainStringConcatFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        mainIndexDictionary = Files.readAllBytes(mainDictionaryFile.toPath());
        mainConcatString = Files.readAllBytes(mainStringConcatFile.toPath());
        mainInvertedIndexFile = new File(mainIndexDirectory.getPath()
                + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        loadMainNumOfTokensPerBlock();
    }

    private void loadMainNumOfTokensPerBlock() throws IOException {
        File indexMetaFile = new File(mainIndexDirectory.getPath() + File.separator + Statics.MAIN_INDEX_META_DATA_FILENAME);
        RandomAccessFile raIndexMeta;
        raIndexMeta = new RandomAccessFile(indexMetaFile, "r");
        raIndexMeta.seek(Statics.INTEGER_SIZE * 2);
        mainNumOfWordsInFrontCodeBlock = raIndexMeta.readInt();
        System.out.format("Front block size main: %d" + System.lineSeparator(),
                mainNumOfWordsInFrontCodeBlock);
        raIndexMeta.close();
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
        //get main index results
        SingleIndexReader singleIndexReader =
                new SingleIndexReader(mainIndexDictionary,
                mainConcatString,
                mainInvertedIndexFile,
                mainNumOfWordsInFrontCodeBlock);
        Enumeration<Integer> mainResults = singleIndexReader.getReviewsWithWord(token);
        addEnumerationToMap(mainResults, unionOfResults);

        // add the rest of auxiliary indexes results
        Enumeration<Integer> auxResults;
        for (int i = 0; i < numOfAuxIndexes; i++) {
            singleIndexReader = new SingleIndexReader(auxIndexesDictionary[i],
                    auxIndexesConcatString[i],
                    auxInvertedIndexFiles[i],
                    auxNumOfWordsInFrontCodeBlock[i]);
            auxResults = singleIndexReader.getReviewsWithWord(token);
            addEnumerationToMap(auxResults, unionOfResults);
        }
        filterInvalidatedRids(unionOfResults);
        return mapToEnumeration(unionOfResults);
    }

    private void filterInvalidatedRids(TreeMap<Integer, Integer> unionOfResults) {
        try{
            final String validationVectorFilename = mainIndexDirectory + File.separator + Statics.INVALIDATION_VECTOR_FILENAME;
            BufferedInputStream validationVectorBIS = new BufferedInputStream( new FileInputStream(new File(validationVectorFilename)));

            int byteRead = validationVectorBIS.read();
            int byteCounter = 1;
            while(byteRead != -1){
                if(byteRead == 1){
                    unionOfResults.remove(byteCounter);
                }
                byteRead = validationVectorBIS.read();
                byteCounter++;
            }
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    private Enumeration<Integer> mapToEnumeration(TreeMap<Integer, Integer> unionOfResults) {
        Vector<Integer> toEnumerate = new Vector<>();
        for(Map.Entry<Integer,Integer> entry: unionOfResults.entrySet()){
            toEnumerate.add(entry.getKey());
            toEnumerate.add(entry.getValue());
        }
        return toEnumerate.elements();
    }

    private void addEnumerationToMap(Enumeration<Integer> integerEnumeration, TreeMap<Integer, Integer> ridToFreqUnion) {
        while (integerEnumeration.hasMoreElements()) {
            int rid = integerEnumeration.nextElement();
            assert integerEnumeration.hasMoreElements();
            int frequency = integerEnumeration.nextElement();
            ridToFreqUnion.put(rid, frequency);
        }
    }

}