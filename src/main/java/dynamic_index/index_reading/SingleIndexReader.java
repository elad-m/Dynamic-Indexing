package dynamic_index.index_reading;


import dynamic_index.global_tools.IndexInvalidationTool;
import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_structure.FrontCodeBlock;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static dynamic_index.global_tools.LengthPrecodedVarintCodec.decodeBytesToIntegers;



/**
 * Answers IndexReader's queries, by performing binary searches in the index
 * files, and extracting correct data from their bytes.
 */
public class SingleIndexReader {


    private static final int TWO_BYTES_READ = 2;
    private final int NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    private final int FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE;
    private final int FRONT_CODE_ROW_SIZE_IN_BYTES;

    private String tokenToFind;

    final File mainIndexDirectory;

    final File currentIndexDirectory;
    private final File invertedIndexFile;

    private byte[] indexDictionary;
    private byte[] concatString;

    public SingleIndexReader(byte[] mainIndexDictionary,
                             byte[] mainConcatString,
                             File invertedIndexFile,
                             int numOfTokensPerBlock,
                             File mainIndexDirectory) {
        this.invertedIndexFile = invertedIndexFile;
        this.mainIndexDirectory = mainIndexDirectory;
        this.currentIndexDirectory = invertedIndexFile.getParentFile();
        assignArrays(mainIndexDictionary, mainConcatString);
        this.NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK = numOfTokensPerBlock;
        FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE =
                (FrontCodeBlock.BYTES_IN_WORD_BLOCK * NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK);
        FRONT_CODE_ROW_SIZE_IN_BYTES =
                MiscTools.INTEGER_SIZE + FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE;
    }

    public TreeMap<Integer, Integer> getReviewsWithWord(String word) {
        try {
            assert word != null;
            tokenToFind = word;
            return findInvertedIndexLine(word);
        } catch (IOException e) {
            e.printStackTrace();
            return new TreeMap<>();
        }
    }


    private void assignArrays(byte[] indexDictionary, byte[] concatString) {
        this.indexDictionary = indexDictionary;
        this.concatString = concatString;

    }

    private TreeMap<Integer, Integer> findInvertedIndexLine(String word)
            throws IOException {
        TreeMap<Integer,Integer> ridToFrequencyMap;
        TokenMetaData tokenMetaData = binarySearch(word,
                0,
                (indexDictionary.length / FRONT_CODE_ROW_SIZE_IN_BYTES) - 1);
        if (tokenMetaData == null) {
            ridToFrequencyMap = new TreeMap<>();
        } else {
            ridToFrequencyMap = getRidToFreqMapFromTokenMetaData(tokenMetaData);
        }
        return ridToFrequencyMap;
    }


    private TokenMetaData binarySearch(String word, int rowsLowerBound, int rowsUpperBound)
            throws IOException {
        if (rowsLowerBound > rowsUpperBound || rowsUpperBound < 0)
            return null;
        int middleInRows = (rowsLowerBound + (rowsUpperBound - rowsLowerBound) / 2);
        int middleInBytes = FRONT_CODE_ROW_SIZE_IN_BYTES * middleInRows;

        TreeMap<String, TokenMetaData> wordToMetaData = getWordsFromRowOfBytes(middleInBytes, middleInRows);
        assert !wordToMetaData.isEmpty();
        return jumpTo(wordToMetaData, word,
                rowsLowerBound, rowsUpperBound, middleInRows);
    }

    TreeMap<String, TokenMetaData> getWordsFromRowOfBytes(int middleInBytes, int middleInRows) {
        /* In each stage in the binary search, we have to read all the words
           in the block of a string, since it is possible that the first letter
           is not a prefix of some word in the block.
        */
        final int intSize = MiscTools.INTEGER_SIZE;
        int pointerToBlockInString = ByteBuffer.wrap(indexDictionary, middleInBytes, intSize).getInt();
        byte[] blockData = new byte[FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE];
        System.arraycopy(indexDictionary, middleInBytes + intSize, blockData, 0, FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE);
        return getWordsToTokenMetaData(pointerToBlockInString, blockData, middleInRows);
    }

    private TreeMap<String, TokenMetaData> getWordsToTokenMetaData(
            int pointerToBlockInString,
            byte[] blockData, int middleInRows) {
        TreeMap<String, TokenMetaData> wordToPointerAndLength = new TreeMap<>();
        int totalCharReadInString = pointerToBlockInString;
        int firstTokenNumber = NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK * middleInRows;
        StringBuilder firstWord = new StringBuilder();
        for (int i = 0;
             i < blockData.length;
             i += FrontCodeBlock.BYTES_IN_WORD_BLOCK) {
            byte length = blockData[i];
            byte prefixLength = blockData[i + 1];
            int freqPointer = ByteBuffer.wrap(blockData, i + TWO_BYTES_READ, Integer.BYTES).getInt();
            int FREQ_LEN_OFFSET = MiscTools.INTEGER_SIZE + 2 * Byte.BYTES;
            int freqLength = ByteBuffer.wrap(blockData, i + FREQ_LEN_OFFSET, Integer.BYTES).getInt();
            assert length >= prefixLength: "token: " + tokenToFind;
            byte suffixLength = (byte) (length - prefixLength);
            int tokenNumberInDictionary = firstTokenNumber + (i / FrontCodeBlock.BYTES_IN_WORD_BLOCK);

            if (length == 0 || freqLength == 0) {
                break; // finish the run
            }
            if (i == 0) {  // first word in block
                readWordToStringBuilder(totalCharReadInString, length, firstWord);
                wordToPointerAndLength.put(firstWord.toString(),
                        new TokenMetaData(freqPointer, freqLength));
                totalCharReadInString += length;
            } else {
                StringBuilder currentWord = new StringBuilder(firstWord.substring(0, prefixLength));
                assert suffixLength != 0;
                readWordToStringBuilder(totalCharReadInString, suffixLength, currentWord);
                wordToPointerAndLength.put(currentWord.toString(),
                        new TokenMetaData(freqPointer, freqLength));
                totalCharReadInString += suffixLength;
            }
        }
        if (wordToPointerAndLength.remove("") != null)
            System.err.println("An empty string has slipped in");
        return wordToPointerAndLength;
    }

    private void readWordToStringBuilder(int readOffset, int readLength, StringBuilder stringBuilder) {
        for (int j = 0; j < readLength; j++) {
            int posToRead = readOffset + j;
            stringBuilder.append((char) concatString[posToRead]);
        }
    }

    private TokenMetaData jumpTo(TreeMap<String, TokenMetaData> wordToPointerAndLengths,
                                 String word,
                                 int rowsLowerBound, int rowsUpperBound,
                                 int middleInRows)
            throws IOException {
        try{
            if (wordToPointerAndLengths.containsKey(word)) {
                return wordToPointerAndLengths.get(word);
            } else if (word.compareTo(wordToPointerAndLengths.lastKey()) > 0) { // jump forward
                return binarySearch(word,
                        middleInRows + 1, rowsUpperBound);
            } else if (word.compareTo(wordToPointerAndLengths.firstKey()) < 0) { // jump backwards
                return binarySearch(word,
                        rowsLowerBound, middleInRows - 1);
            } else { // word does not exist in the review data!
                return null;  // empty enumeration?
            }
        } catch (NoSuchElementException e) {
            System.err.println("The word was: " + word + " Map size was: " + wordToPointerAndLengths.size());
            e.printStackTrace();
            return null;
        }
    }

    private TreeMap<Integer, Integer> getRidToFreqMapFromTokenMetaData(TokenMetaData pointerAndLength)
            throws IOException {
        byte[] rowToReadInto = getBytesOfInvertedIndexRAF(pointerAndLength);
        List<Integer> integersInBytesRow = decodeBytesToIntegers(rowToReadInto);
        TreeMap<Integer, Integer> results =  getMapFromListOfIntegers(integersInBytesRow);
        if(IndexInvalidationTool.isInvalidationDirty()){ // no querying when there has been no deletion
            IndexInvalidationTool.filterResults(mainIndexDirectory.getAbsolutePath(), results);
        }
        return results;
    }

    TreeMap<Integer, Integer> getRidToFreqMapFromRawInvertedIndex(byte[] rowToReadInto) {
        List<Integer> integersInBytesRow = decodeBytesToIntegers(rowToReadInto);
        TreeMap<Integer, Integer> results =  getMapFromListOfIntegers(integersInBytesRow);
        if(IndexInvalidationTool.isInvalidationDirty()){ // no querying when there has been no deletion
            IndexInvalidationTool.filterResults(mainIndexDirectory.getAbsolutePath(), results);
        }
        return results;
    }


    private byte[] getBytesOfInvertedIndexRAF(TokenMetaData pointerAndLength) throws IOException {
        RandomAccessFile raInvertedIndexFile = new RandomAccessFile(invertedIndexFile, "r");
        raInvertedIndexFile.seek(pointerAndLength.getFreqPointer());
        byte[] rowToReadInto = new byte[pointerAndLength.getFreqLength()];
        raInvertedIndexFile.read(rowToReadInto);
        raInvertedIndexFile.close();
        return rowToReadInto;
    }

    private TreeMap<Integer,Integer> getMapFromListOfIntegers(List<Integer> integersInBytesRow) {
        int size = integersInBytesRow.size();
        assert size % 2 == 0 : "Bad read of bytes line";

        List<Integer> gaps = new ArrayList<>(integersInBytesRow.subList(0, size / 2));
        List<Integer> frequencies = new ArrayList<>(integersInBytesRow.subList(size / 2, size));
        assert gaps.size() == frequencies.size();
        TreeMap<Integer, Integer> finalMap = new TreeMap<>();

        int gapCumSum = 0;
        for (int i = 0; i < gaps.size(); i++) {
            int gap = gaps.get(i);
            gapCumSum += gap;
            int frequency = frequencies.get(i);
            finalMap.put(gapCumSum, frequency);

        }
        return finalMap;
    }

    public File getInvertedIndexFile() {
        return invertedIndexFile;
    }

    public int getFRONT_CODE_ROW_SIZE_IN_BYTES() {
        return FRONT_CODE_ROW_SIZE_IN_BYTES;
    }

    public int getIndexDictionaryLength() {
        return indexDictionary.length;
    }

    public File getCurrentIndexDirectory() {
        return currentIndexDirectory;
    }

    @Override
    public String toString() {
        return  "of Dir = " + currentIndexDirectory.getName() ;
    }
}
