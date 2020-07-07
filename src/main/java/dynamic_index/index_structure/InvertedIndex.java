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
public class InvertedIndex implements WritingMeasurable {

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
    private final TreeMap<Integer, Integer> ridToFrequencyMap = new TreeMap<>();

    private int amountOfBytesWrittenExternalOutput = 0;
    private int ioBufferSize = 8192;
    private int numOfReviews = 0;

    private boolean finishedWriting = false;

    //Fields for writing to a file if necessary (for common words)
    private final File indexDirectory;
    private File ridDumpFile;
    private File freqDumpFile;
    private DataOutputStream dosRawRidDumpFile;
    private DataOutputStream dosRawFreqDumpFile;
    private boolean hasFile = false;
    private int firstRidInFile;


    public InvertedIndex(Map<Integer, Integer> ridToFrequency, String word, File indexDirectory) {
        this.word = word;
        this.indexDirectory = indexDirectory;
        putAll(ridToFrequency);
    }

    void put(int rid, int frequency) {
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
        try {
            if (!hasFile) {
                createFileAndStream();
                hasFile = true;
                firstRidInFile = ridToFrequencyMap.firstKey(); // happens only once
            }
            for(Map.Entry<Integer, Integer> ridAndFrequency: ridToFrequencyMap.entrySet()){ // written as is
                dosRawRidDumpFile.writeInt(ridAndFrequency.getKey());
                dosRawFreqDumpFile.writeInt(ridAndFrequency.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void emptyInvertedIndexAfterWriting() {
        ridToFrequencyMap.clear();
    }

    private void createFileAndStream() throws FileNotFoundException {
        ridDumpFile = new File(indexDirectory.getPath() + File.separator + word + "RidTmp"
                + Statics.BINARY_FILE_SUFFIX);
        freqDumpFile = new File(indexDirectory.getPath() + File.separator + word + "FreqTmp"
                + Statics.BINARY_FILE_SUFFIX);
        ioBufferSize = Statics.roundUpToProductOfPairSize(ridToFrequencyMap.size() * Statics.INTEGER_SIZE);
        dosRawRidDumpFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ridDumpFile), ioBufferSize));
        dosRawFreqDumpFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(freqDumpFile), ioBufferSize));
        System.out.println("\tcreated file with path: " + ridDumpFile.getPath()  + " and " + freqDumpFile.getPath());
    }

    /**
     * Writes this inverted index's rids to the given output stream. If this class has a file (for common words)
     * it writes from it first, and then writes from the in-memory map.
     * @param bosOfAllInverted - output stream to the whole index inverted file. (main/aux/merged)
     * @param prevInvertedIndexRid - when merging, a word might be in several indexes but we need the previous
     *                             rid to calculate the current gap. When not merging this value should be
     *                             0.
     * @return last rid written, needed when merging indexes.
     */
    public int writeCompressedRidsTo(BufferedOutputStream bosOfAllInverted, int prevInvertedIndexRid) {
        int lastRid = prevInvertedIndexRid;
        try {
            if (hasFile) {
                lastRid = writeCompressedRidsFromDumpFile(bosOfAllInverted);
            }
            lastRid = writeCompressedRidsFromInMemory(bosOfAllInverted, lastRid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lastRid;
    }

    private int writeCompressedRidsFromDumpFile(BufferedOutputStream bosOfAllInverted) throws IOException {
        int lastRid = 0;
        dosRawRidDumpFile.close(); // close writing before reading
        DataInputStream disRidDumpFile = new DataInputStream(new BufferedInputStream(new FileInputStream(ridDumpFile)));
        int currentRid = disRidDumpFile.readInt();
        while (currentRid != -1) { // guaranteed to not be -1 in the first iteration
            writeSingleRidToExternalOutput(bosOfAllInverted, currentRid, lastRid);
            lastRid = currentRid;
            currentRid = disRidDumpFile.readInt();
        }
        disRidDumpFile.close();
        return lastRid;
    }

    private int writeCompressedRidsFromInMemory(BufferedOutputStream bosOfAllInverted, int previousRid) throws IOException {
        int lastRid = previousRid;
        for(Integer currentRid: ridToFrequencyMap.keySet()){ // writing in-memory rids
            writeSingleRidToExternalOutput(bosOfAllInverted, currentRid, lastRid);
            lastRid = currentRid;
        }
        return lastRid;
    }

    private void writeSingleRidToExternalOutput(BufferedOutputStream bosOfAllInverted,
                                                int currentRid,
                                                int lastRid) throws IOException {
        numOfReviews++;
        byte[] bytesToWrite = getRidGapCompressed(currentRid, lastRid);
        bosOfAllInverted.write(bytesToWrite);
        amountOfBytesWrittenExternalOutput += bytesToWrite.length;
    }

    private byte[] getRidGapCompressed(int currentRid, int previousRid) {
        int gapRid = currentRid - previousRid;
        return intToCompressedByteArray(gapRid);
    }

    /**
     * Writes this inverted index's frequencies to the given output stream, whether from a file (common word)
     * or from in memory map
     * @param bosOfAllInverted - output stream to the whole index inverted file. (main/aux/merged)
     */
    public void writeCompressedFrequenciesTo(BufferedOutputStream bosOfAllInverted) {
        try {
            if (hasFile) {
                writeCompressedFrequenciesFromDumpFile(bosOfAllInverted);
                deleteDumpFiles();
            }
            writeCompressedFrequenciesFromInMemory(bosOfAllInverted);
            setFinishedWriting(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeCompressedFrequenciesFromDumpFile(BufferedOutputStream bosOfAllInverted) throws IOException {
        dosRawFreqDumpFile.close(); // close writing before reading
        DataInputStream disFreqDumpFile = new DataInputStream(new BufferedInputStream(new FileInputStream(freqDumpFile)));
        int freq = disFreqDumpFile.readInt();
        while (freq != -1) { // guaranteed to not be -1 in the first iteration
            writeSingleFrequencyToExternalOutput(bosOfAllInverted, freq);
            freq = disFreqDumpFile.readInt();
        }
        disFreqDumpFile.close();
    }

    private void writeCompressedFrequenciesFromInMemory(BufferedOutputStream bosOfAllInverted) throws IOException {
        for(Integer freq: ridToFrequencyMap.values()){ // writing in-memory frequencies
            writeSingleFrequencyToExternalOutput(bosOfAllInverted, freq);
        }
    }

    private void writeSingleFrequencyToExternalOutput(BufferedOutputStream bosOfAllInverted,
                                                      int freq) throws IOException {
        byte[] bytesToWrite = intToCompressedByteArray(freq);
        bosOfAllInverted.write(bytesToWrite);
        amountOfBytesWrittenExternalOutput += bytesToWrite.length;
    }

    void deleteDumpFiles() {
        try {
            dosRawRidDumpFile.close();
            dosRawFreqDumpFile.close();
            Files.delete(ridDumpFile.toPath());
            Files.delete(freqDumpFile.toPath());
            System.out.println(word + " dump file deleted");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public int getNumberOfBytesWrittenToOutput() {
        if (!finishedWriting) {
            System.err.println("[DEBUG] should have been calculated by now ");
        }
        return amountOfBytesWrittenExternalOutput;
    }

    private void setFinishedWriting(boolean setTo) {
        finishedWriting = setTo;
    }

    @Override
    public String toString() {
        return "InvertedIndexOfWord{" +
                "word: " + word +
                " rids: " + ridToFrequencyMap.keySet() +
                '}' + '\n';
    }

    private byte[] intToCompressedByteArray(int value){
        byte[] ret = new byte[0];
        if (value <= LENGTH_PRECODED_MAXIMA[0]) {
            ret = toLengthPrecodedVarint(value, 1);
        } else if (value <= LENGTH_PRECODED_MAXIMA[1]) {
            ret = toLengthPrecodedVarint(value, 2);
        } else if (value <= LENGTH_PRECODED_MAXIMA[2]) {
            ret = toLengthPrecodedVarint(value, 3);
        } else if (value <= LENGTH_PRECODED_MAXIMA[3]) {
            ret = toLengthPrecodedVarint(value, 4);
        } else {
            System.err.println("Value too large to be represented in Length Precoded Varint");
            System.exit(8);
        }
        return ret;
    }

    private byte[] toLengthPrecodedVarint(int input, int numberOfBytes) {
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


    int getFirstRid(){
        int firstRid;
        if(hasFile){
            firstRid = firstRidInFile;
        } else {
            firstRid = ridToFrequencyMap.firstKey();
        }
        return firstRid;
    }

}
