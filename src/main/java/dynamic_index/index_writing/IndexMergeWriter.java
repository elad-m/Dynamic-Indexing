package dynamic_index.index_writing;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_structure.FrontCodeBlock;
import dynamic_index.index_structure.InvertedIndex;
import dynamic_index.index_structure.InvertedIndexesToMerge;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;


/**
 * Writes a merge of all indexes using IndexMergingModerator as a source. Works in a similar way to
 * WordsIndexWriter.
 */
public class IndexMergeWriter {

    private final File mergedIndexDirectory;
    private StringBuilder allWordsSuffixConcatInBlock = new StringBuilder(MiscTools.STRING_BUILDER_DEFAULT_CAPACITY);
    private int numOfCharactersWrittenInSuffixFile = 0;
    private int numOfBytesWrittenInInvertedIndexFile = 0;

    private BufferedOutputStream frontCodeOutputStream;
    private BufferedOutputStream invertedOutputStream;
    private BufferedWriter bufferedStringConcatWriter;
    private final int numOfTokensInFrontCodeBlock = 8;

    private final TreeMap<String, InvertedIndexesToMerge> wordToInvertedIndexMergerMap = new TreeMap<>();

    /**
     * Should be called when wanting to merge all indexes in a given directory.
     * @param allIndexesDirectory - the directory in which to merge all indexes.
     */
    public IndexMergeWriter(String allIndexesDirectory) {
        this.mergedIndexDirectory = MiscTools.createDirectory(allIndexesDirectory
                + File.separator
                + MiscTools.MERGED_INDEX_DIRECTORY);
    }

    public File merge(IndexMergingModerator indexMergingModerator) {
        instantiateIndexFiles();
        Map.Entry<String, InvertedIndex> currentWordAndInverted = indexMergingModerator.getNextMergingWordAndIndex();
        while (currentWordAndInverted != null) {
            if (shouldWriteMap(currentWordAndInverted.getKey()))
                writeAndReset();
            insertToMap(currentWordAndInverted);// will not increase the number of word in the map
            currentWordAndInverted = indexMergingModerator.getNextMergingWordAndIndex();
        }
        writeRemainderAndClose();
        return mergedIndexDirectory;
    }

    private void instantiateIndexFiles() {
        File frontCodedFile = new File(mergedIndexDirectory + File.separator + MiscTools.WORDS_FRONT_CODED_FILENAME);
        File invIndexFile = new File(mergedIndexDirectory + File.separator + MiscTools.WORDS_INVERTED_INDEX_FILENAME);
        File stringConcatFile = new File(mergedIndexDirectory + File.separator + MiscTools.WORDS_CONCAT_FILENAME);
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

    void writeMapToFiles() {
        try {
            writeBlockOfInvertedIndexToFile(); // first because byte calculation
            writeFrontCodeFile();
            writeStringConcatFile(); // has to be last, after the front code writing
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeBlockOfInvertedIndexToFile(){
        for (InvertedIndexesToMerge invertedIndexesToMerge : wordToInvertedIndexMergerMap.values()) {
            invertedIndexesToMerge.writeTo(invertedOutputStream);
        }
    }

    private void writeFrontCodeFile() throws IOException {
        FrontCodeBlock frontCodeBlock = new FrontCodeBlock(wordToInvertedIndexMergerMap,
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

    private void resetIteration() {
        wordToInvertedIndexMergerMap.clear();
        allWordsSuffixConcatInBlock = new StringBuilder(MiscTools.STRING_BUILDER_DEFAULT_CAPACITY);
    }

    private boolean shouldWriteMap(String currentWord){
        // if we have 8 words and the next word is new
        return !wordToInvertedIndexMergerMap.containsKey(currentWord) &&
                wordToInvertedIndexMergerMap.size() == numOfTokensInFrontCodeBlock;
    }

    private void writeAndReset(){
        writeMapToFiles();
        resetIteration();
    }

    private void insertToMap(Map.Entry<String, InvertedIndex> currentWordAndInverted) {
        String currentWord = currentWordAndInverted.getKey();
        InvertedIndex currentInverted = currentWordAndInverted.getValue();
        if(wordToInvertedIndexMergerMap.containsKey(currentWord)){
            wordToInvertedIndexMergerMap.get(currentWord).put(currentInverted);
        } else {
            InvertedIndexesToMerge invertedIndexesToMerge = new InvertedIndexesToMerge(currentWord);
            invertedIndexesToMerge.put(currentInverted);
            wordToInvertedIndexMergerMap.put(currentWord, invertedIndexesToMerge);
        }
    }


    private void writeRemainderAndClose() {
        writeMapToFiles(); // last iteration, not necessary to resetIteration here
        closeStreams();
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
