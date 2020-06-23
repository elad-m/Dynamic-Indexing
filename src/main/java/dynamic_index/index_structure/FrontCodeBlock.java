package dynamic_index.index_structure;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static dynamic_index.Statics.intToByteArray;

/**
 * Represent a block of tokens (words, products) in the Front Code Block
 * compressing method.
 */
public class FrontCodeBlock {

    public static final int BYTES_IN_WORD_BLOCK = 10;

    final int frontCodeBlockTokenCapacity;

    private final TreeMap<String, InvertedIndexOfWord> blockOfTokensAndData;
    private final StringBuilder compressedString = new StringBuilder();
    private byte[] stringPointer;
    private final byte[] lengthsInBlock;
    private final byte[] prefixLengthsInBlock;
    private final byte[] pointersInBlock; // in bytes, but has the size for ints
    private final byte[] pointersLengthsInBlock;  // in bytes, but has the size for ints
    private final int blockSize;
    private int bytesOfInvertedIndexWrittenSoFar;


    public FrontCodeBlock(Map<String, ? extends InvertedIndexOfWord> blockOfWordsAndData,
                   int blocksReadInBytesSoFar, int numOfTokensInFrontCodeBlock) {
        this.frontCodeBlockTokenCapacity = numOfTokensInFrontCodeBlock;
        this.blockSize = blockOfWordsAndData.size();
        assert this.blockSize > 0 : "empty words block";
        this.stringPointer = new byte[Integer.BYTES];
        this.lengthsInBlock = new byte[blockSize];
        this.prefixLengthsInBlock = new byte[blockSize];
        this.pointersInBlock = new byte[Integer.BYTES * blockSize];
        this.pointersLengthsInBlock = new byte[Integer.BYTES * blockSize];

        this.bytesOfInvertedIndexWrittenSoFar = blocksReadInBytesSoFar;

        this.blockOfTokensAndData = new TreeMap<>(blockOfWordsAndData);
        createCompression();
    }


    private void createCompression() {
        String firstWord = blockOfTokensAndData.firstKey();
        int i = 0;
        for (String word : blockOfTokensAndData.keySet()) {
            InvertedIndexOfWord invertedIndex = blockOfTokensAndData.get(word);
            int sizeOfInvertedOfWord = invertedIndex.getNumberOfBytesWrittenToOutput();
            FrontCodeBlockWord frontCodeBlockWord =
                    new FrontCodeBlockWord(firstWord, word,
                            bytesOfInvertedIndexWrittenSoFar,
                            sizeOfInvertedOfWord);
            bytesOfInvertedIndexWrittenSoFar += sizeOfInvertedOfWord;
            updateBlock(i, frontCodeBlockWord);
            i++;
        }
    }

    private void updateBlock(int i, FrontCodeBlockWord frontCodeBlockWord) {
        compressedString.append(frontCodeBlockWord.getSubstringToConcatenate());
        lengthsInBlock[i] = frontCodeBlockWord.getLength();
        prefixLengthsInBlock[i] = frontCodeBlockWord.getPrefixLength();

        byte[] pointerLengthsBytes = intToByteArray(frontCodeBlockWord.getFrequencyLength());
        System.arraycopy(pointerLengthsBytes, 0, pointersLengthsInBlock,
                Integer.BYTES * i, Integer.BYTES);
        byte[] pointerBytes = intToByteArray(frontCodeBlockWord.getFreqPointer());
        System.arraycopy(pointerBytes, 0, pointersInBlock,
                Integer.BYTES * i, Integer.BYTES);
    }

    public String getCompressedString() {
        return compressedString.toString();
    }

    public byte[] getBlockRow(int lengthOfStringSoFar) {
        stringPointer = intToByteArray(lengthOfStringSoFar);
        byte[] blockRow = new byte[4 + ((BYTES_IN_WORD_BLOCK) * frontCodeBlockTokenCapacity)];  // int + (byte, byte, int, byte)* N
        System.arraycopy(
                stringPointer, 0, blockRow, 0, 4);
        for (int i = 0; i < blockSize; i++) {  // get the lengths into the row
            blockRow[4 + (BYTES_IN_WORD_BLOCK * i)] = lengthsInBlock[i];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 1)] = prefixLengthsInBlock[i];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 2)] = pointersInBlock[(4 * i)];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 3)] = pointersInBlock[(4 * i) + 1];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 4)] = pointersInBlock[(4 * i) + 2];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 5)] = pointersInBlock[(4 * i) + 3];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 6)] = pointersLengthsInBlock[(4 * i)];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 7)] = pointersLengthsInBlock[(4 * i) + 1];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 8)] = pointersLengthsInBlock[(4 * i) + 2];
            blockRow[4 + ((BYTES_IN_WORD_BLOCK * i) + 9)] = pointersLengthsInBlock[(4 * i) + 3];
        }
        for (int i = 4 + (BYTES_IN_WORD_BLOCK * blockSize); i < blockRow.length; i++) {  // pad zeros, for last line
            blockRow[i] = 0;
        }
        return blockRow;
    }

    public int getBytesOfInvertedIndexWrittenSoFar() {
        return this.bytesOfInvertedIndexWrittenSoFar;
    }


    static class FrontCodeBlockWord {


        final byte length;
        final byte prefixLength;

        /* these two cannot have variable bytes since they are in the binary search file */
        final int freqPointer;
        final int frequencyLength; // number of bytes to read from pointer

        final String substringToConcatenate; // suffix or whole word

        final boolean isFirst;

        FrontCodeBlockWord(String firstWord, String word,
                           int freqPointer, int frequencyLength) {
            this.freqPointer = freqPointer;
            this.frequencyLength = frequencyLength;
            this.length = (byte) word.length(); // not too safe here
            this.isFirst = firstWord.equals(word);
            this.prefixLength = (byte) getGreatestCommonPrefixLength(firstWord, word);
            this.substringToConcatenate = calculateSubstringToConcatenate(word);
        }

        String calculateSubstringToConcatenate(String word) {
            if (isFirst) {
                return word;
            } else {
                return word.substring(prefixLength, length);
            }
        }

        String getSubstringToConcatenate() {
            return substringToConcatenate;
        }

        byte getLength() {
            return this.length;
        }

        int getFrequencyLength() {
            return this.frequencyLength;
        }

        byte getPrefixLength() {
            return this.prefixLength;
        }

        int getFreqPointer() {
            return freqPointer;
        }

        private int getGreatestCommonPrefixLength(String a, String b) {
            int minLength = Math.min(a.length(), b.length());
            for (int i = 0; i < minLength; i++) {
                if (a.charAt(i) != b.charAt(i)) {
                    return i;
                }
            }
            return minLength;
        }
    }

    @Override
    public String toString() {
        return "FrontCodeBlock{" +
                "blockOfTokensAndData=" + blockOfTokensAndData.keySet() +
                ", stringPointer=" + Arrays.toString(stringPointer) +
                ", lengthsInBlock=" + Arrays.toString(lengthsInBlock) +
                ", prefixLengthsInBlock=" + Arrays.toString(prefixLengthsInBlock) +
                ", pointersInBlock=" + Arrays.toString(pointersInBlock) +
                ", pointersLengthsInBlock=" + Arrays.toString(pointersLengthsInBlock) +
                '}' + '\n';
    }
}
