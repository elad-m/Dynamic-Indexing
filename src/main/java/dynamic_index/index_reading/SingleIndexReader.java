package dynamic_index.index_reading;


import dynamic_index.Statics;
import dynamic_index.index_structure.FrontCodeBlock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import static java.lang.System.exit;


/**
 * Answers IndexReader's queries, by performing binary searches in the index
 * files, and extracting correct data from their bytes.
 */
public class SingleIndexReader {


    private static final int TWO_BYTES_READ = 2;
    private int NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    private int FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE;
    private int FRONT_CODE_ROW_SIZE_IN_BYTES;


    private static final short BITWISE_AND_OPERAND_TO_DECODE_SHORT = -16385; // 101111..
    // next line is from: 0b, 01111111, -1b, -1b
    private static final int BITWISE_AND_OPERAND_TO_DECODE_THREE_BYTES = 8388607;
    private static final int BITWISE_AND_OPERAND_TO_DECODE_INTEGER = 1073741823;
    public static final int AND_OPERAND_FOR_RIGHT_SHIFTING_TRUE_BYTE_VALUE = 255;


    private String tokenToFind;
    private File invertedIndexFile;
    private byte[] indexDictionary;
    private byte[] concatString;

    public SingleIndexReader(byte[] mainIndexDictionary,
                             byte[] mainConcatString,
                             File invertedIndexFile,
                             int numOfTokensPerBlock) {
        this.invertedIndexFile = invertedIndexFile;
        assignArrays(mainIndexDictionary, mainConcatString);
        this.NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK = numOfTokensPerBlock;
        FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE =
                (FrontCodeBlock.BYTES_IN_WORD_BLOCK * NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK);
        FRONT_CODE_ROW_SIZE_IN_BYTES =
                Statics.INTEGER_SIZE + FRONT_CODE_WITHOUT_STRING_POINTER_ROW_SIZE;
    }

    public Enumeration<Integer> getReviewsWithWord(String word) {
        try {
            tokenToFind = word;
            return findInvertedIndexLine(word);
        } catch (IOException e) {
            e.printStackTrace();
            return new Vector<Integer>().elements();
        }
    }


    private void assignArrays(byte[] indexDictionary, byte[] concatString) {
        this.indexDictionary = indexDictionary;
        this.concatString = concatString;

    }

    private Enumeration<Integer> findInvertedIndexLine(String word)
            throws IOException {
        Enumeration<Integer> enumeration;
        TokenMetaData tokenMetaData = binarySearch(word,
                0,
                (indexDictionary.length / FRONT_CODE_ROW_SIZE_IN_BYTES) - 1);
        if (tokenMetaData == null) {
            enumeration = new Vector<Integer>().elements();
        } else {
            enumeration = getEnumerationFromRow(tokenMetaData);
        }
        return enumeration;
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

    private TreeMap<String, TokenMetaData> getWordsFromRowOfBytes(int middleInBytes, int middleInRows) {
        /* In each stage in the binary search, we have to read all the words
           in the block of a string, since it is possible that the first letter
           is not a prefix of some word in the block.
        */
        final int intSize = Statics.INTEGER_SIZE;
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
            int FREQ_LEN_OFFSET = Statics.INTEGER_SIZE + 2 * Byte.BYTES;
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
                        new TokenMetaData(freqPointer, freqLength, tokenNumberInDictionary));
                totalCharReadInString += length;
            } else {
                StringBuilder currentWord = new StringBuilder(firstWord.substring(0, prefixLength));
                readWordToStringBuilder(totalCharReadInString, suffixLength, currentWord);
                wordToPointerAndLength.put(currentWord.toString(),
                        new TokenMetaData(freqPointer, freqLength, tokenNumberInDictionary));
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
    }

    private Enumeration<Integer> getEnumerationFromRow(TokenMetaData pointerAndLength)
            throws IOException {
        RandomAccessFile raInvertedIndexFile = new RandomAccessFile(invertedIndexFile, "r");
        raInvertedIndexFile.seek(pointerAndLength.getFreqPointer());
        byte[] rowToReadInto = new byte[pointerAndLength.getFreqLength()];
        raInvertedIndexFile.read(rowToReadInto);
        raInvertedIndexFile.close();
        List<Integer> integersInBytesRow = decodeBytesToIntegers(rowToReadInto);
        return getEnumerationFromListOfIntegers(integersInBytesRow);
    }

    private List<Integer> decodeBytesToIntegers(byte[] rowInBytes) {
        // using length pre-coded varint
        ByteBuffer byteBufferOfRow = ByteBuffer.wrap(rowInBytes);
        List<Integer> integersInBytesRow = new ArrayList<>();

        int i = 0;
        while (i < rowInBytes.length) {
            byte someByte = byteBufferOfRow.get(i);
            int numOfBytesRoRead = getNumberOfBytesToRead(someByte);
            switch (numOfBytesRoRead) {
                case 1:
                    integersInBytesRow.add((int) someByte);
                    i++;
                    break;
                case 2:
                    short rid2 = byteBufferOfRow.getShort(i);
                    int debug1 = getDecodedInteger(rid2);
                    integersInBytesRow.add(debug1);
                    i += 2;
                    break;
                case 3:
                    byte[] threeArray = new byte[4];
                    threeArray[0] = 0;
                    threeArray[1] = byteBufferOfRow.get(i);
                    threeArray[2] = byteBufferOfRow.get(i + 1);
                    threeArray[3] = byteBufferOfRow.get(i + 2);
                    int rid3 = ByteBuffer.wrap(threeArray).getInt();
                    int debug2 = getDecodedInteger(rid3, true);
                    integersInBytesRow.add(debug2);
                    i += 3;
                    break;
                case 4:
                    int rid4 = byteBufferOfRow.getInt(i);
                    int debug3 = getDecodedInteger(rid4, false);
                    integersInBytesRow.add(debug3);
                    i += 4;
                    break;
                default:
                    System.err.println("got not between 1-4 bytes to read");
                    break;
            }
        }
//        Statics.printList(integersInBytesRow, indexDirectory, tokenToFind + "vector.txt");
        return integersInBytesRow;
    }

    private int getNumberOfBytesToRead(byte someByte) {
        /* Bitwise operations in java convert up to int anything it gets. To get correct results with this impediment
         *  the constant 255 operand bellow makes all the 1s above the 8 bit of negative byte numbers to zero, so now they appear to be
         * they true self as negative byte, e.g: -128 & 255 = 128. so now 128>>>6 = 2, what we wanted. Credits for the IDE
         * for bringing this to my attention.*/
        int javaBadTypePromotionByProduct = someByte & AND_OPERAND_FOR_RIGHT_SHIFTING_TRUE_BYTE_VALUE;
        int firstTwoBitsValue = javaBadTypePromotionByProduct >>> 6;
        int numOfBytesToRead;
        switch (firstTwoBitsValue) {
            case 0:
                numOfBytesToRead = 1;
                break;
            case 1:
                numOfBytesToRead = 2;
                break;
            case 2:
                numOfBytesToRead = 3;
                break;
            case 3:
                numOfBytesToRead = 4;
                break;
            default:
                System.err.println("two bits extraction failed");
                exit(2);
                numOfBytesToRead = 0;
        }
        return numOfBytesToRead;
    }

    private int getDecodedInteger(short shortRid) {
        return (shortRid & BITWISE_AND_OPERAND_TO_DECODE_SHORT);
    }

    private int getDecodedInteger(int intRid, boolean isThree) {
        if (isThree)
            return (intRid & BITWISE_AND_OPERAND_TO_DECODE_THREE_BYTES);
        else
            return (intRid & BITWISE_AND_OPERAND_TO_DECODE_INTEGER);
    }

    private Enumeration<Integer> getEnumerationFromListOfIntegers(List<Integer> integersInBytesRow) {
        int size = integersInBytesRow.size();
        assert size % 2 == 0 : "Bad read of bytes line";

        List<Integer> gaps = new ArrayList<>(integersInBytesRow.subList(0, size / 2));
        List<Integer> frequencies = new ArrayList<>(integersInBytesRow.subList(size / 2, size));
        assert gaps.size() == frequencies.size();
        Vector<Integer> finalVector = new Vector<>(size, size);

        int gapCumSum = 0;
        for (int i = 0; i < gaps.size(); i++) {
            int gap = gaps.get(i);
            gapCumSum += gap;
            int frequency = frequencies.get(i);
            finalVector.add(gapCumSum);
            finalVector.add(frequency);

        }
        return finalVector.elements();
    }


    /**
     * Data about token (word/pid) that is necessary to complete a search for inverted index
     * of a token, i.e. the frequency pointer and this pointer's length in bytes.
     */
    private static class TokenMetaData {

        private final int freqPointer;
        private final int freqLength;
        //the following is only to get meta data by numbering the words in the dictionary
        private final int tokenNumberInFile;

        private TokenMetaData(int freqPointer,
                              int freqLength,
                              int tokenId) {
            this.freqPointer = freqPointer;
            this.freqLength = freqLength;
            this.tokenNumberInFile = tokenId;
        }

        int getFreqPointer() {
            return freqPointer;
        }

        int getFreqLength() {
            return freqLength;
        }


    }


}
