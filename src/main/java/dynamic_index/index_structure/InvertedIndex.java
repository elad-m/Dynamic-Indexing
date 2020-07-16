package dynamic_index.index_structure;


import dynamic_index.global_tools.MiscTools;

import java.io.*;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;

import static dynamic_index.global_tools.LengthPrecodedVarintCodec.intToCompressedByteArray;


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
     * Create inverted index with minimal data: a word that appears in a review with rid and the
     * frequency of that word int this rid. Intended as to not increase the size of the
     * InvertedIndex (by reviews) by more than 1.
     * @param word - a word in the review
     * @param rid - review ID (docID)
     * @param freqForRid - the word's frequency in the review of rid.
     * @param indexDirectory - where to keep temporary files if necessary
     */
    public InvertedIndex(String word, int rid, int freqForRid, File indexDirectory){
        this.word = word;
        this.indexDirectory = indexDirectory;
        this.indexName = indexDirectory.getName();
        put(rid, freqForRid);
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

    /**
     * Inserts the rid and frequency to the map
     * @param rid - rid that a word is in
     * @param frequency - frequency of that word in this rid
     */
    public void put(int rid, int frequency) {
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
     * Inserts all the rids and their frequencies to the class's map
     * @param ridToFrequency - rids and the frequency of this word in it.
     */
    public void putAll(Map<Integer, Integer> ridToFrequency) {
        for (Map.Entry<Integer, Integer> anRidToFrequency : ridToFrequency.entrySet()) {
            put(anRidToFrequency.getKey(), anRidToFrequency.getValue());
        }
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
                + MiscTools.BINARY_FILE_SUFFIX);
        freqDumpFile = new File(indexDirectory.getPath() + File.separator + word + "FreqTmp" + indexName
                + MiscTools.BINARY_FILE_SUFFIX);
        int ioBufferSize = MiscTools.roundUpToProductOfPairSize(ridToFrequencyMap.size() * MiscTools.INTEGER_SIZE);
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
        int currentRid;
        dosRawRidDumpFile.close(); // close writing before reading
        DataInputStream disRidDumpFile = new DataInputStream(new BufferedInputStream(new FileInputStream(ridDumpFile)));
        int fileSizeInInts = ((int) ridDumpFile.length() / MiscTools.INTEGER_SIZE);

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
            setFinishedWriting();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeCompressedFrequenciesFromDumpFile(BufferedOutputStream bosOfAllInverted) throws IOException {
        int freq;
        dosRawFreqDumpFile.close(); // close writing before reading
        DataInputStream disFreqDumpFile = new DataInputStream(new BufferedInputStream(new FileInputStream(freqDumpFile)));
        int fileSizeInInts = ((int) freqDumpFile.length() / MiscTools.INTEGER_SIZE);
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

    private void setFinishedWriting() {
        finishedWriting = true;
    }

    @Override
    public String toString() {
        return "InvertedIndexOfWord{" +
                "word: " + word +
                "indexName" + indexName +
                " rids: " + ridToFrequencyMap.keySet() +
                '}' + '\n';
    }

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
