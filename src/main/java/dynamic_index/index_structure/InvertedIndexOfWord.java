package dynamic_index.index_structure;


import dynamic_index.Statics;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;


/**
 * The rids and frequencies of a unique word . Handles the byte conversion mainly.
 * Now supported bulk loading and updating the frequency of a word.
 */
public class InvertedIndexOfWord {

    protected static final int[] LENGTH_PRECODED_MAXIMA =
            {63, 16383, 4194303, 1073741823};// 2^6-1, 2^14-1, 2^22-1, 2^30-1
    protected static final int[] BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT =
            {0, 16384, 8388608, -1073741824}; // 00|zeros, 2^14, 2^23, 2^31+2^30
    public static int MAX_NUM_OF_PAIRS = 500;

    private final String word;


    /**
     * Essentially the inverted index.
     * Except for boolean values, all the fields below are initialized to null/zero right after writing to a file.
     */
    private final Map<Integer, Integer> ridToFrequencyMap = new TreeMap<>();
    protected int amountOfBytesWrittenExternalOutput = 0;
    protected int ioBufferSize = 8192;
    private int numOfMentions = 0;
    private int numOfReviews = 0;


    private int[] ridGaps;
    private int[] frequencies;
    int previousReviewId = 0;
    // bytes representation
    private byte[] ridsAsBytes;
    private byte[] frequenciesAsBytes;
    private boolean isCalculated = false;
//    private int amountOfBytesWrittenExternalOutput = 0;

    //Fields for writing to a file if necessary (for common words)
    private final File indexDirectory;
    private File ridDumpFile;
    private File freqDumpFile;
    private boolean hasFiles = false;
    private BufferedOutputStream bosRidDumpFile;
    private BufferedOutputStream bosFreqDumpFile;
//    private int ioBufferSize = 8192;


    // used when writing index
    public InvertedIndexOfWord(Map<Integer, Integer> ridToFrequency, String word, File indexDirectory) {
        this.word = word;
        this.indexDirectory = indexDirectory;
        putAll(ridToFrequency);
    }

    void put(int rid, int frequency) {
        setIsCalculated(false);
        if (ridToFrequencyMap.size() == MAX_NUM_OF_PAIRS) {
            writeMapToDumpFiles(); // counting on sorted here
            emptyInvertedIndexAfterWriting(); // the whole point of writing to file is free memory resources
        }
        if (ridToFrequencyMap.containsKey(rid)) {
            int existingFreq = ridToFrequencyMap.get(rid);
            ridToFrequencyMap.put(rid, existingFreq + frequency);
        } else {
            ridToFrequencyMap.put(rid, frequency);
        }
    }

    public void putAll(Map<Integer, Integer> ridToFrequency) {
        for (Map.Entry<Integer, Integer> anRidToFrequency : ridToFrequency.entrySet()) {
            put(anRidToFrequency.getKey(), anRidToFrequency.getValue());
        }
    }

    private void writeMapToDumpFiles() {
        convertMapToByteArrays();
        try {
            if (!hasFiles) {
                createFileAndStream();
            }
            hasFiles = true;
            bosRidDumpFile.write(ridsAsBytes);
            bosFreqDumpFile.write(frequenciesAsBytes);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void emptyInvertedIndexAfterWriting() {
        ridToFrequencyMap.clear();
        ridGaps = null;
        frequencies = null;
        ridsAsBytes = null;
        frequenciesAsBytes = null;
    }

    private void createFileAndStream() throws FileNotFoundException {
        ridDumpFile = new File(indexDirectory.getPath() + File.separator + word + "-rid"
                + Statics.BINARY_FILE_SUFFIX);
        freqDumpFile = new File(indexDirectory.getPath() + File.separator + word + "-freq"
                + Statics.BINARY_FILE_SUFFIX);
        ioBufferSize = Statics.roundUpToProductOfPairSize(ridToFrequencyMap.size() * Statics.INTEGER_SIZE);
        bosRidDumpFile = new BufferedOutputStream(new FileOutputStream(ridDumpFile), ioBufferSize);
        bosFreqDumpFile = new BufferedOutputStream(new FileOutputStream(freqDumpFile), ioBufferSize);
        System.out.println("\tcreated file with path: " + ridDumpFile.getPath() + " and " + freqDumpFile.getPath());
    }

    public void externalWrite(BufferedOutputStream bosOfAllInverted) {
        try {
            convertMapToByteArrays();
            if (hasFiles) {
                externalWritePerFile(bosOfAllInverted, ridDumpFile, bosRidDumpFile); // has to be before frequencies
                bosOfAllInverted.write(ridsAsBytes); // writing the in-memory rids

                externalWritePerFile(bosOfAllInverted, freqDumpFile, bosFreqDumpFile);
                bosOfAllInverted.write(frequenciesAsBytes);

                amountOfBytesWrittenExternalOutput += ridsAsBytes.length;
                amountOfBytesWrittenExternalOutput += frequenciesAsBytes.length;
                deleteDumpFiles();
            } else { // !hasFile, so only writing the in-memory arrays
                bosOfAllInverted.write(ridsAsBytes); // writing the in-memory rids
                bosOfAllInverted.write(frequenciesAsBytes);
                amountOfBytesWrittenExternalOutput += ridsAsBytes.length;
                amountOfBytesWrittenExternalOutput += frequenciesAsBytes.length;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void deleteDumpFiles() {
        try {
            bosRidDumpFile.close();
            bosFreqDumpFile.close();
            Files.delete(ridDumpFile.toPath());
            Files.delete(freqDumpFile.toPath());
            System.out.println(word + " dump file deleted");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // whenever the byte array is asked for, the bytes array is calculated again
    private void convertMapToByteArrays() {
        loadMapToIntArrays();
        calculateByteArrays();
        setIsCalculated(true);
        emptyIntArrays();
    }

    private void emptyIntArrays() {
        ridGaps = null;
        frequencies = null;
    }

    /**
     * Should only be called after externalWriting(), where the bytes calculation takes place.
     *
     * @return size of all the inverted index values -  gaps and frequencies - as bytes
     * when encoded by Length-Precoded Varint method.
     */
    int getNumberOfBytesWrittenToOutput() {
        if (!isCalculated) {
            System.err.println("[DEBUG] should have been calculated by now ");
        }
        return amountOfBytesWrittenExternalOutput;
    }

    private void loadMapToIntArrays() {
        int ridFrequencyPairsSize = ridToFrequencyMap.size();
        ridGaps = new int[ridFrequencyPairsSize];
        frequencies = new int[ridFrequencyPairsSize];
        int currentRidIndex = 0;
        for (Map.Entry<Integer, Integer> rfEntry : ridToFrequencyMap.entrySet()) {
            int currentRid = rfEntry.getKey();
            int currentFrequency = rfEntry.getValue();
            ridGaps[currentRidIndex] = currentRid - previousReviewId;
            frequencies[currentRidIndex] = currentFrequency; // no gaps
            numOfMentions += currentFrequency;
            numOfReviews++;
            previousReviewId = currentRid;
            currentRidIndex++;
        }
    }


    private void setIsCalculated(boolean setTo) {
        isCalculated = setTo;
    }

    private void calculateByteArrays() {
        /* using length-precoded varint. not group because wasteful for none long lines
         * and words are mostly sparse.
         */
        assert ridGaps.length == frequencies.length;
        int sizeOfValues = ridGaps.length;
        byte[][] gapValues = new byte[sizeOfValues][];
        byte[][] frequencyValues = new byte[sizeOfValues][];

        int futureGapsByteArraySize = loadIntArrayTo2DByteArray(ridGaps, gapValues);
        int futureFreqsBytesArraySize = loadIntArrayTo2DByteArray(frequencies, frequencyValues);
        ridsAsBytes = getFlattenedByteArray(gapValues, futureGapsByteArraySize, sizeOfValues);
        frequenciesAsBytes = getFlattenedByteArray(frequencyValues, futureFreqsBytesArraySize, sizeOfValues);
    }


    @Override
    public String toString() {
        return "InvertedIndexOfWord{" +
                "word: " + word +
                '}' + '\n';
    }

    int getNumOfMentions() {
        return numOfMentions;
    }

    int getNumOfReviews() {
        return numOfReviews;
    }


    void externalWritePerFile(BufferedOutputStream bosOfAllInverted, File dumpFile, BufferedOutputStream bosDumpFile) throws IOException {
        bosDumpFile.close(); // close writing before reading
        RandomAccessFile raDumpFile = new RandomAccessFile(dumpFile, "r");
        byte[] blockRead = new byte[ioBufferSize];
        int amountOfBytesRead = raDumpFile.read(blockRead);
        while (amountOfBytesRead != -1) { // guaranteed to not be -1 in the first iteration
            bosOfAllInverted.write(blockRead, 0, amountOfBytesRead); // might have less new data than allocated
            amountOfBytesWrittenExternalOutput += amountOfBytesRead;
            amountOfBytesRead = raDumpFile.read(blockRead);
        }
        raDumpFile.close();
    }

    byte[] getFlattenedByteArray(byte[][] valuesToFlatten, int sizeToAllocate, int sizeOfValues) {
        ByteBuffer flattenedValues = ByteBuffer.allocate(sizeToAllocate);
        for (int i = 0; i < sizeOfValues; i++) {
            byte[] currentByteArray = valuesToFlatten[i];
            flattenedValues.put(currentByteArray);
        }
        return flattenedValues.array();
    }

    int loadIntArrayTo2DByteArray(int[] intArray, byte[][] array2D) {
        int numOfBytesWrittenArray2D = 0;
        for (int i = 0; i < intArray.length; i++) {
            int currentIntArray = intArray[i];
            if (currentIntArray <= LENGTH_PRECODED_MAXIMA[0]) {
                array2D[i] = toLengthPrecodedVarint(currentIntArray, 1);
            } else if (currentIntArray <= LENGTH_PRECODED_MAXIMA[1]) {
                array2D[i] = toLengthPrecodedVarint(currentIntArray, 2);
            } else if (currentIntArray <= LENGTH_PRECODED_MAXIMA[2]) {
                array2D[i] = toLengthPrecodedVarint(currentIntArray, 3);
            } else if (currentIntArray <= LENGTH_PRECODED_MAXIMA[3]) {
                array2D[i] = toLengthPrecodedVarint(currentIntArray, 4);
            } else {
                System.err.println("Value too large to be represented in Length Precoded Varint");
                System.exit(8);
            }
            numOfBytesWrittenArray2D += array2D[i].length;
        }
        return numOfBytesWrittenArray2D;
    }

    byte[] toLengthPrecodedVarint(int input, int numberOfBytes) {
        byte[] resultLenPrecodeVarint;
        if (numberOfBytes == 1) {
            resultLenPrecodeVarint = new byte[]{(byte) input};
        } else if (numberOfBytes == 2) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
            byteBuffer.putShort((short) (input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[1]));
            resultLenPrecodeVarint = byteBuffer.array();
            assert resultLenPrecodeVarint.length == 2;
        } else if (numberOfBytes == 3) {
            ByteBuffer byteBufferTarget = ByteBuffer.allocate(Short.BYTES + 1);
            ByteBuffer byteBufferInteger = ByteBuffer.allocate(Integer.BYTES);
            byteBufferInteger.putInt(input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[2]);
            byteBufferTarget.put(byteBufferInteger.get(1));
            byteBufferTarget.put(byteBufferInteger.get(2));
            byteBufferTarget.put(byteBufferInteger.get(3));
            resultLenPrecodeVarint = byteBufferTarget.array();
            assert resultLenPrecodeVarint.length == 3;
        } else if (numberOfBytes == 4) {
            ByteBuffer byteBufferInteger = ByteBuffer.allocate(Integer.BYTES);
            byteBufferInteger.putInt(input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[3]);
            resultLenPrecodeVarint = byteBufferInteger.array();
            assert resultLenPrecodeVarint.length == 4;
        } else {
            System.err.println("Number bigger than length-precoded varint integer detected");
            resultLenPrecodeVarint = new byte[0];
        }
        return resultLenPrecodeVarint;
    }
}
