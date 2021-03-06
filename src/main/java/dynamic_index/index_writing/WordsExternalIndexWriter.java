package dynamic_index.index_writing;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_structure.FrontCodeBlock;
import dynamic_index.index_structure.InvertedIndex;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Holds the token to review and frequency mapping, and write it to a designated index
 * files. The source of data for this index is a huge file of pairs of (tid,rid) sorted by tid and
 * then by rid.
 */
public class WordsExternalIndexWriter {

    private final File indexDirectory;
    private StringBuilder allWordsSuffixConcatInBlock = new StringBuilder(MiscTools.STRING_BUILDER_DEFAULT_CAPACITY);
    private int numOfCharactersWrittenInSuffixFile = 0;
    private int numOfBytesWrittenInInvertedIndexFile = 0;

    private BufferedOutputStream frontCodeOutputStream;
    private BufferedOutputStream invertedOutputStream;
    private BufferedWriter bufferedStringConcatWriter;
    private boolean isInLastWriteIteration = false;
    private int numOfTokensInFrontCodeBlock = 8;

    private final Map<String, InvertedIndex> wordToInvertedIndex = new TreeMap<>();

    public WordsExternalIndexWriter(File directoryPath) {
        this.indexDirectory = directoryPath;
    }

    private void instantiateIndexFiles(int readingBlockSize) {
        File frontCodedFile = new File(indexDirectory + File.separator + MiscTools.WORDS_FRONT_CODED_FILENAME);
        File invIndexFile = new File(indexDirectory + File.separator + MiscTools.WORDS_INVERTED_INDEX_FILENAME);
        File stringConcatFile = new File(indexDirectory + File.separator + MiscTools.WORDS_CONCAT_FILENAME);
        try {
            if (frontCodedFile.createNewFile()
                    && invIndexFile.createNewFile()
                    && stringConcatFile.createNewFile()) {
                frontCodeOutputStream = new BufferedOutputStream(new FileOutputStream(frontCodedFile), readingBlockSize);
                invertedOutputStream = new BufferedOutputStream(new FileOutputStream(invIndexFile), readingBlockSize);
                bufferedStringConcatWriter = new BufferedWriter(new FileWriter(stringConcatFile), readingBlockSize);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Words index files already exist. Delete them and try again.");
        }
    }

    /**
     * Writes an index from a sorted file of tids and rids.
     * @param numOfTokensInFrontCodeBlock - will be used when building the dictionary
     * @param readBlockSizeInPairs - the size of a block to read from  the sorted file.
     * @param termIdToTerm - mapping of termId to term so we can know what words are represented in the sorted
     *                     file by a tid
     */
    public void writeFromSortedFileByBlocks(int numOfTokensInFrontCodeBlock,
                                            int readBlockSizeInPairs,
                                            Map<Integer, String> termIdToTerm) {
        this.numOfTokensInFrontCodeBlock = numOfTokensInFrontCodeBlock;

        int readingBlockSizeInBytes = readBlockSizeInPairs * MiscTools.PAIR_OF_INT_SIZE_IN_BYTES;
        instantiateIndexFiles(readingBlockSizeInBytes);
        File sortedFile = new File(indexDirectory + File.separator
                + MiscTools.WORDS_SORTED_FILE_NAME);

        writeFromSortedFileByBlocks(sortedFile, readingBlockSizeInBytes, termIdToTerm);
    }


    private void writeFromSortedFileByBlocks(File sortedFile,
                                             int readingBlockSizeInBytes,
                                             Map<Integer, String> termIdToTerm) {
        try (BufferedInputStream sortedFileBIS = new BufferedInputStream(new FileInputStream(sortedFile))) {
            byte[] blockAsBytes = new byte[readingBlockSizeInBytes];
            int amountOfBytesRead = sortedFileBIS.read(blockAsBytes);
            while (amountOfBytesRead != -1) {
                loadBlockToMap(blockAsBytes, amountOfBytesRead, termIdToTerm);
                writeMapToFiles();
                amountOfBytesRead = sortedFileBIS.read(blockAsBytes);
            }
            setInLastWriteIteration();
            writeRemainderAndClose();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void loadBlockToMap(byte[] blockAsBytes,
                                int amountOfBytesRead,
                                Map<Integer, String> termIdToTerm) {
        ByteBuffer blockByteBuffer = ByteBuffer.wrap(blockAsBytes, 0, amountOfBytesRead);
        List<Integer> ridsOfATid = new ArrayList<>(); // should result in ordered list

        int previousTid = blockByteBuffer.getInt(0);
        for (int i = 0; i < amountOfBytesRead; i += MiscTools.PAIR_OF_INT_SIZE_IN_BYTES) {
            if (blockByteBuffer.getInt(i) == 0) {
                break; // possibly hides other reasons for zeros...
            }
            int tid = blockByteBuffer.getInt();
            int rid = blockByteBuffer.getInt();
            if (tid != previousTid) {
                insertHistogramToMap(previousTid, ridsOfATid, termIdToTerm);
            }
            ridsOfATid.add(rid);
            previousTid = tid;
        }
        if (!ridsOfATid.isEmpty()) {
            insertHistogramToMap(previousTid, ridsOfATid, termIdToTerm);
        }

    }

    void writeMapToFiles() throws IOException {
        int stopReadingFromMapAt = getStopReadingFromMapAt();
        writeBlockOfInvertedIndexToFile(stopReadingFromMapAt); // first because byte calculation
        writeFrontCodeFile(stopReadingFromMapAt);
        writeStringConcatFile(); // has to be last, after the front code writing
        resetIteration(stopReadingFromMapAt);
    }

    private Map<Integer, Integer> getRidToFrequencyHistogram(List<Integer> rids) {
        Map<Integer, Integer> histogram = new TreeMap<>();
        for (Integer rid : rids) {
            if (!histogram.containsKey(rid)) { //new token
                histogram.put(rid, 1);
            } else {
                int newValueForToken = (histogram.get(rid) + 1);
                histogram.put(rid, newValueForToken);
            }
        }
        return histogram;
    }

    void insertHistogramToMap(int tid, List<Integer> ridsOfATid, Map<Integer, String> termIdToTerm) {
        Map<Integer, Integer> ridToFrequencyHistogram = getRidToFrequencyHistogram(ridsOfATid);
        String word = termIdToTerm.get(tid);
        if (wordToInvertedIndex.containsKey(word)) { // word already in.. can this happen? how to prevent it
            wordToInvertedIndex.get(word).putAll(ridToFrequencyHistogram);
        } else { // new word
            InvertedIndex invertedIndexOfWord = new InvertedIndex(word, ridToFrequencyHistogram, indexDirectory);
            wordToInvertedIndex.put(word, invertedIndexOfWord);
        }
        ridsOfATid.clear();
    }

    int getStopReadingFromMapAt() {
        int stopAt = wordToInvertedIndex.size(); // by default, read all words from map
        if (!isInLastWriteIteration) {
            if (wordToInvertedIndex.size() % numOfTokensInFrontCodeBlock != 0) {
                // can't write less than a front block number of tokens
                // guarantees to reduce the number of words written by at least 1, possibly all
                stopAt = MiscTools.roundDownToMultiplicationOf(wordToInvertedIndex.size(),
                        numOfTokensInFrontCodeBlock);
            } else {
                /* can't write all the words since the last word in a block might have more data for it.
                 * So, we need to reduce one BLOCK of words */
                stopAt -= numOfTokensInFrontCodeBlock;
            }
        }
        return stopAt;
    }

    void writeBlockOfInvertedIndexToFile(int stopWritingWordsAt) {
        int i = 0; // i here is for stopping
        for (InvertedIndex invertedIndexOfWord : wordToInvertedIndex.values()) {
            if (!isInLastWriteIteration && i == stopWritingWordsAt) {
                break;
            }
            // handles the writing with dump files if they exist
            invertedIndexOfWord.writeCompressedRidsTo(invertedOutputStream, 0); // ignoring last rid here
            invertedIndexOfWord.writeCompressedFrequenciesTo(invertedOutputStream);
            i++;
        }
    }

    void writeFrontCodeFile(int stopWritingWordsAt) throws IOException {
        TreeMap<String, InvertedIndex> blockOfWordsToInvertedIndex = new TreeMap<>();
        int i = 0;
        for (Map.Entry<String, InvertedIndex> wordAndInvertedIndex : wordToInvertedIndex.entrySet()) {
            if (!isInLastWriteIteration && i == stopWritingWordsAt) {
                break;
            }
            blockOfWordsToInvertedIndex.put(wordAndInvertedIndex.getKey(), wordAndInvertedIndex.getValue());
            if (i % numOfTokensInFrontCodeBlock == numOfTokensInFrontCodeBlock - 1) {
                writeFrontCodeBlock(blockOfWordsToInvertedIndex, numOfTokensInFrontCodeBlock);
                blockOfWordsToInvertedIndex.clear();
            }
            i++;
        }
        if (isInLastWriteIteration && !blockOfWordsToInvertedIndex.isEmpty()) { // when: mod(number of words, 8) != 0
            writeFrontCodeBlock(blockOfWordsToInvertedIndex, numOfTokensInFrontCodeBlock);
        }

    }

    void resetIteration(int stopWritingWordsAt) {
        /* deleting from map all words that have been written */
        Map<String, InvertedIndex> iterationRemainder = new TreeMap<>();
        int i = 0;
        for (Map.Entry<String, InvertedIndex> wordAndInvertedIndex : wordToInvertedIndex.entrySet()) {
            if (i >= stopWritingWordsAt) {
                iterationRemainder.put(wordAndInvertedIndex.getKey(), wordAndInvertedIndex.getValue());
            }
            i++;
        }
        wordToInvertedIndex.clear();
        wordToInvertedIndex.putAll(iterationRemainder);
        allWordsSuffixConcatInBlock = new StringBuilder(MiscTools.STRING_BUILDER_DEFAULT_CAPACITY);
    }


    void closeAllFiles() throws IOException {
        frontCodeOutputStream.close();
        invertedOutputStream.close();
        bufferedStringConcatWriter.close();
    }


    void writeFrontCodeBlock(TreeMap<String, InvertedIndex> blockOfWordToInvertedIndex, int numOfTokensInFrontCodeBlock)
            throws IOException {
        FrontCodeBlock frontCodeBlock = new FrontCodeBlock(blockOfWordToInvertedIndex,
                numOfBytesWrittenInInvertedIndexFile,
                numOfTokensInFrontCodeBlock);
        numOfBytesWrittenInInvertedIndexFile = frontCodeBlock.getBytesOfInvertedIndexWrittenSoFar();

        frontCodeOutputStream.write(frontCodeBlock.getBlockRow(numOfCharactersWrittenInSuffixFile));
        String compressedStringForBlock = frontCodeBlock.getCompressedString();
        allWordsSuffixConcatInBlock.append(compressedStringForBlock);
        numOfCharactersWrittenInSuffixFile += compressedStringForBlock.length();
    }

    private void writeStringConcatFile()
            throws IOException {
        bufferedStringConcatWriter.write(allWordsSuffixConcatInBlock.toString());
    }

    void setInLastWriteIteration() {
        isInLastWriteIteration = true;
    }

    private void writeRemainderAndClose() throws IOException {
        writeMapToFiles();
        closeAllFiles();
    }
}

