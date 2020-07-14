package dynamic_index.index_writing;

import dynamic_index.global_util.MiscUtils;
import dynamic_index.index_structure.FrontCodeBlock;
import dynamic_index.index_structure.InvertedIndex;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes an index from an in-memory map into som directory.
 * No bulk reading from sorted file, no merging. Should be used for small index size (below 1000?)
 */
public class SimpleIndexWriter {

    private final File indexOutputDirectory;

    private final StringBuilder allWordsSuffixConcatInBlock = new StringBuilder(MiscUtils.STRING_BUILDER_DEFAULT_CAPACITY);
    private int numOfCharactersWrittenInSuffixFile = 0;
    private int numOfBytesWrittenInInvertedIndexFile = 0;

    private BufferedOutputStream frontCodeOutputStream;
    private BufferedOutputStream invertedOutputStream;
    private BufferedWriter bufferedStringConcatWriter;
    private final int numOfTokensInFrontCodeBlock = 8;

    public SimpleIndexWriter(File indexOutputDirectory){
        this.indexOutputDirectory = indexOutputDirectory;
    }

    public void write(TreeMap<String, InvertedIndex> wordToInvertedIndex){
        instantiateIndexFiles();
        writeMapToFiles(wordToInvertedIndex);
        closeStreams();
    }

    private void instantiateIndexFiles() {
        File frontCodedFile = new File(indexOutputDirectory + File.separator + MiscUtils.WORDS_FRONT_CODED_FILENAME);
        File invIndexFile = new File(indexOutputDirectory + File.separator + MiscUtils.WORDS_INVERTED_INDEX_FILENAME);
        File stringConcatFile = new File(indexOutputDirectory + File.separator + MiscUtils.WORDS_CONCAT_FILENAME);
        try {
            if (frontCodedFile.createNewFile()
                    && invIndexFile.createNewFile()
                    && stringConcatFile.createNewFile()) {
                frontCodeOutputStream = new BufferedOutputStream(new FileOutputStream(frontCodedFile));
                invertedOutputStream = new BufferedOutputStream(new FileOutputStream(invIndexFile));
                bufferedStringConcatWriter = new BufferedWriter(new FileWriter(stringConcatFile));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Words index files already exist. Delete them and try again.");
        }
    }

    void writeMapToFiles(TreeMap<String, InvertedIndex> wordToInvertedIndex) {
        try {
            writeBlockOfInvertedIndexToFile(wordToInvertedIndex); // first because byte calculation
            writeFrontCodeFile(wordToInvertedIndex);
            writeStringConcatFile(); // has to be last, after the front code writing
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeBlockOfInvertedIndexToFile(TreeMap<String, InvertedIndex> wordToInvertedIndexMap){
        for (InvertedIndex invertedIndex : wordToInvertedIndexMap.values()) {
            invertedIndex.writeCompressedRidsTo(invertedOutputStream, 0); // ignoring last rid here
            invertedIndex.writeCompressedFrequenciesTo(invertedOutputStream);
        }
    }


    void writeFrontCodeFile(TreeMap<String, InvertedIndex> wordToInvertedIndex) throws IOException {
        TreeMap<String, InvertedIndex> blockOfWordsToInvertedIndex = new TreeMap<>();

        for (Map.Entry<String, InvertedIndex> wordAndInvertedIndex : wordToInvertedIndex.entrySet()) {
            blockOfWordsToInvertedIndex.put(wordAndInvertedIndex.getKey(), wordAndInvertedIndex.getValue());
            if (blockOfWordsToInvertedIndex.size()  == numOfTokensInFrontCodeBlock) {
                writeFrontCodeBlock(blockOfWordsToInvertedIndex);
                blockOfWordsToInvertedIndex.clear();
            }
        }
        if (!blockOfWordsToInvertedIndex.isEmpty()) { // when: mod(number of words, 8) != 0
            writeFrontCodeBlock(blockOfWordsToInvertedIndex);
        }

    }

    private void writeFrontCodeBlock(TreeMap<String, InvertedIndex> blockOfWordsToInvertedIndex)
            throws IOException {
        FrontCodeBlock frontCodeBlock = new FrontCodeBlock(blockOfWordsToInvertedIndex,
                numOfBytesWrittenInInvertedIndexFile,
                this.numOfTokensInFrontCodeBlock);
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

    void closeStreams() {
        try {
            frontCodeOutputStream.close();
            invertedOutputStream.close();
            bufferedStringConcatWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
