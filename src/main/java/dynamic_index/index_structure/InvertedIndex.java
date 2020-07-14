package dynamic_index.index_structure;


import dynamic_index.global_util.MiscUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;

import static dynamic_index.global_util.LengthPrecodedVarintCodec.intToCompressedByteArray;


/**
 * The rids and frequencies of a unique word. Executes writing with compression to
 * external stream.
 */
public class InvertedIndex implements WritingMeasurable {

    public static int MAX_NUM_OF_PAIRS = 4096;

    private final String word;
    private final String indexName;


    /**
     * Essentially the inverted index.
     * Except for boolean values, all the fields below are initialized to null/zero right after writing to a file.
     */
    private final TreeMap<Integer, Integer> ridToFrequencyMap = new TreeMap<>();

    private int amountOfBytesWrittenExternalOutput = 0;
    private int numOfReviews = 0;

    private boolean finishedWriting = false;

    //Fields for writing to a file if necessary (for common words)
    private final File indexDirectory;
    private File ridDumpFile;
    private File freqDumpFile;
    private DataOutputStream dosRawRidDumpFile;
    private DataOutputStream dosRawFreqDumpFile;
    private boolean withFile = false;
    private int firstRidInFile;


    /**
     * Create inverted index with minimal data: a word that appears in a review with rid.
     * @param word - a word in the review
     * @param rid - review ID (docID)
     * @param indexDirectory - where to keep temporary files if necessary
     */
    public InvertedIndex(String word, int rid, File indexDirectory){
        this.word = word;
        this.indexDirectory = indexDirectory;
        this.indexName = indexDirectory.getName();
        put(rid, 1);
    }

    /**
     * Create inverted index with some bulk of data of a word.
     * @param word - the word this inverted index belongs to.
     * @param ridToFrequency - review IDs and the frequency that the word appears in
     * @param mainIndexDirectory - where to keep temporary files if necessary, parent of the next parameter
     * @param currentIndexDirectory - where the index directory source of this inverted index is.
     */
    public InvertedIndex(String word,
                         Map<Integer, Integer> ridToFrequency,
                         File mainIndexDirectory,
                         File currentIndexDirectory) {
        this.word = word;
        this.indexDirectory = mainIndexDirectory;
        this.indexName = currentIndexDirectory.getName();
        putAll(ridToFrequency);
    }

    /**
     * @return the size of the inverted index calculated by reviews (and not sum of frequencies).
     */
    public int getSizeByReviews(){
        if(!withFile)
            return ridToFrequencyMap.size();
        else return -1; // should not have a file
    }

    private void put(int rid, int frequency) {
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

    /**
     * Insertes all the rids and their frequencies to the class's map
     * @param ridToFrequency - rids and the frequency of this word in it.
     */
    public void putAll(Map<Integer, Integer> ridToFrequency) {
        for (Map.Entry<Integer, Integer> anRidToFrequency : ridToFrequency.entrySet()) {
            put(anRidToFrequency.getKey(), anRidToFrequency.getValue());
        }
    }

    public void put(int rid){
        put(rid, 1);
    }

    private void writeMapToDumpFiles() {
        try {
            if (!withFile) {
                createFileAndStream();
                withFile = true;
                firstRidInFile = ridToFrequencyMap.firstKey(); // happens only once
            }
            for(Map.Entry<Integer, Integer> ridAndFrequency: ridToFrequencyMap.entrySet()){ // written to file as ints
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
        ridDumpFile = new File(indexDirectory.getPath() + File.separator + word + "RidTmp" + indexName
                + MiscUtils.BINARY_FILE_SUFFIX);
        freqDumpFile = new File(indexDirectory.getPath() + File.separator + word + "FreqTmp" + indexName
                + MiscUtils.BINARY_FILE_SUFFIX);
        int ioBufferSize = MiscUtils.roundUpToProductOfPairSize(ridToFrequencyMap.size() * MiscUtils.INTEGER_SIZE);
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
            if (withFile) {
                lastRid = writeCompressedRidsFromDumpFile(bosOfAllInverted, lastRid);
            }
            lastRid = writeCompressedRidsFromInMemory(bosOfAllInverted, lastRid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lastRid;
    }

    private int writeCompressedRidsFromDumpFile(BufferedOutputStream bosOfAllInverted, int lastRid) throws IOException {
//        int lastRid = 0;
        int currentRid;
        dosRawRidDumpFile.close(); // close writing before reading
        DataInputStream disRidDumpFile = new DataInputStream(new BufferedInputStream(new FileInputStream(ridDumpFile)));
        int fileSizeInInts = ((int) ridDumpFile.length() / MiscUtils.INTEGER_SIZE);

        for(int i = 0; i < fileSizeInInts; i++) { // guaranteed to not be -1 in the first iteration
            currentRid = disRidDumpFile.readInt();
            writeSingleRidToExternalOutput(bosOfAllInverted, currentRid, lastRid);
            lastRid = currentRid;
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
            if (withFile) {
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
        int freq;
        dosRawFreqDumpFile.close(); // close writing before reading
        DataInputStream disFreqDumpFile = new DataInputStream(new BufferedInputStream(new FileInputStream(freqDumpFile)));
        int fileSizeInInts = ((int) freqDumpFile.length() / MiscUtils.INTEGER_SIZE);
        for(int i = 0; i < fileSizeInInts; i++) { // guaranteed to not be -1 in the first iteration
            freq = disFreqDumpFile.readInt();
            writeSingleFrequencyToExternalOutput(bosOfAllInverted, freq);
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

//    private static final int[] LENGTH_PRECODED_MAXIMA =
//            {63, 16383, 4194303, 1073741823};// 2^6-1, 2^14-1, 2^22-1, 2^30-1
//    private static final int[] BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT =
//            {0, 16384, 8388608, -1073741824}; // 00|zeros, 2^14, 2^23, 2^31+2^30
//
//    /**
//     * Given an integer in the range [0...2^30-1], returns the length-precoded byte array of the integer.
//     * Length pre-coding codes the length of the integer value in byte in the first 2 bits of the byte array.
//     * This way smaller numbers take less number of bytes for writing.
//     * The ranges for conversions and the conversion tools are detailed in the constants above.
//     * @param value - an integer value in the range [0...2^30-1] (last 2 MSB are for encoding)
//     * @return the value of the integer encoded in length-precoded varint method.
//     */
//    public static byte[] intToCompressedByteArray(int value){
//        byte[] ret = new byte[0];
//        if (value <= LENGTH_PRECODED_MAXIMA[0]) {
//            ret = toLengthPrecodedVarint(value, 1);
//        } else if (value <= LENGTH_PRECODED_MAXIMA[1]) {
//            ret = toLengthPrecodedVarint(value, 2);
//        } else if (value <= LENGTH_PRECODED_MAXIMA[2]) {
//            ret = toLengthPrecodedVarint(value, 3);
//        } else if (value <= LENGTH_PRECODED_MAXIMA[3]) {
//            ret = toLengthPrecodedVarint(value, 4);
//        } else {
//            System.err.println("Value too large to be represented in Length Precoded Varint");
//            System.exit(8);
//        }
//        return ret;
//    }
//
//    private static byte[] toLengthPrecodedVarint(int input, int numberOfBytes) {
//        byte[] resultLenPrecodeVarint;
//        if (numberOfBytes == 1) {
//            resultLenPrecodeVarint = new byte[]{(byte) input};
//        } else if (numberOfBytes == 2) {
//            ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
//            byteBuffer.putShort((short) (input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[1]));
//            resultLenPrecodeVarint = byteBuffer.array();
//            assert resultLenPrecodeVarint.length == 2;
//        } else if (numberOfBytes == 3) {
//            ByteBuffer byteBufferTarget = ByteBuffer.allocate(Short.BYTES + 1);
//            ByteBuffer byteBufferInteger = ByteBuffer.allocate(Integer.BYTES);
//            byteBufferInteger.putInt(input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[2]);
//            byteBufferTarget.put(byteBufferInteger.get(1));
//            byteBufferTarget.put(byteBufferInteger.get(2));
//            byteBufferTarget.put(byteBufferInteger.get(3));
//            resultLenPrecodeVarint = byteBufferTarget.array();
//            assert resultLenPrecodeVarint.length == 3;
//        } else if (numberOfBytes == 4) {
//            ByteBuffer byteBufferInteger = ByteBuffer.allocate(Integer.BYTES);
//            byteBufferInteger.putInt(input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[3]);
//            resultLenPrecodeVarint = byteBufferInteger.array();
//            assert resultLenPrecodeVarint.length == 4;
//        } else {
//            System.err.println("Number bigger than length-precoded varint integer detected");
//            resultLenPrecodeVarint = new byte[0];
//        }
//        return resultLenPrecodeVarint;
//    }


    int getFirstRid(){
        int firstRid;
        if(withFile){
            firstRid = firstRidInFile;
        } else {
            firstRid = ridToFrequencyMap.firstKey();
        }
        return firstRid;
    }

    /**
     * @return true if this class is also written to files, or false if all data is in-memory
     */
    public boolean isWithFile() {
        return withFile;
    }

    /**
     * @return - in-memory data structure of this class.
     */
    public TreeMap<Integer, Integer> getRidToFrequencyMap() {
        return ridToFrequencyMap;
    }

}
