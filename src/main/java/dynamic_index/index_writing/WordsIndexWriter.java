package dynamic_index.index_writing;

import dynamic_index.Statics;
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
 * files
 */
public class WordsIndexWriter {

    private final File indexDirectory;
    private StringBuilder allWordsSuffixConcatInBlock = new StringBuilder(Statics.STRING_BUILDER_DEFAULT_CAPACITY);
    private int numOfCharactersWrittenInSuffixFile = 0;
    private int numOfBytesWrittenInInvertedIndexFile = 0;

    private BufferedOutputStream frontCodeOutputStream;
    private BufferedOutputStream invertedOutputStream;
    private BufferedWriter bufferedStringConcatWriter;
    private boolean isInLastWriteIteration = false;
    private int numOfTokensInFrontCodeBlock = 8;

    private final Map<String, InvertedIndex> wordToInvertedIndex = new TreeMap<>();

    public WordsIndexWriter(File directoryPath) {
        this.indexDirectory = directoryPath;
    }

    private void instantiateIndexFiles(int readingBlockSize) {
        File frontCodedFile = new File(indexDirectory + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File invIndexFile = new File(indexDirectory + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        File stringConcatFile = new File(indexDirectory + File.separator + Statics.WORDS_CONCAT_FILENAME);
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


    public void loadSortedFileByBlocks(int numOfTokensInFrontCodeBlock,
                                       int readBlockSizeInPairs,
                                       Map<Integer, String> termIdToTerm,
                                       int reviewCounter) {
        this.numOfTokensInFrontCodeBlock = numOfTokensInFrontCodeBlock;
        InvertedIndex.MAX_NUM_OF_PAIRS *= Math.max(1, reviewCounter / 1000);
        System.out.println("MAX_NUM_OF_PAIRS Words: " + InvertedIndex.MAX_NUM_OF_PAIRS);

        int readingBlockSizeInBytes = readBlockSizeInPairs * Statics.PAIR_OF_INT_SIZE_IN_BYTES;
        instantiateIndexFiles(readingBlockSizeInBytes);
        File sortedFile = new File(indexDirectory + File.separator
                + Statics.WORDS_SORTED_FILE_NAME);

        System.out.println("reading block size in WORDS writer in bytes: " + readingBlockSizeInBytes);
        System.out.println("NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK: " + numOfTokensInFrontCodeBlock);
        loadSortedFileByBlocks(sortedFile, readingBlockSizeInBytes, termIdToTerm);
    }


    private void loadSortedFileByBlocks(File sortedFile,
                                        int readingBlockSizeInBytes,
                                        Map<Integer, String> termIdToTerm) {
        try (BufferedInputStream sortedFileBIS = new BufferedInputStream(new FileInputStream(sortedFile))) {
            byte[] blockAsBytes = new byte[readingBlockSizeInBytes];
            int amountOfBytesRead = sortedFileBIS.read(blockAsBytes);
            while (amountOfBytesRead != -1) {
//                System.out.println("numbOfWordsWrittenDebug: " + numbOfWordsWrittenDebug);
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
        for (int i = 0; i < amountOfBytesRead; i += Statics.PAIR_OF_INT_SIZE_IN_BYTES) {
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
            InvertedIndex invertedIndexOfWord = new InvertedIndex(ridToFrequencyHistogram, word, indexDirectory);
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
                stopAt = Statics.roundDownToMultiplicationOf(wordToInvertedIndex.size(),
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

//    private void writeMetaToFile(InvertedIndexOfWord invertedIndexOfWord) {
//        try {
//            int reviewWithWordCounter = invertedIndexOfWord.getNumOfReviews();
//            int frequencyCumSum = invertedIndexOfWord.getNumOfMentions();
//            metaOutputStream.write(Statics.intToByteArray(reviewWithWordCounter));
//            metaOutputStream.write(Statics.intToByteArray(frequencyCumSum));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


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
        allWordsSuffixConcatInBlock = new StringBuilder(Statics.STRING_BUILDER_DEFAULT_CAPACITY);
    }


    void closeAllFiles() throws IOException {
        frontCodeOutputStream.close();
        invertedOutputStream.close();
        bufferedStringConcatWriter.close();
//        metaOutputStream.close();
    }


    void writeFrontCodeBlock(TreeMap<String, InvertedIndex> blockOfPidsToInvertedIndex, int numOfTokensInFrontCodeBlock)
            throws IOException {
        FrontCodeBlock frontCodeBlock = new FrontCodeBlock(blockOfPidsToInvertedIndex,
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

