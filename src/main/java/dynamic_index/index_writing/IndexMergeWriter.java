package dynamic_index.index_writing;

import dynamic_index.Statics;
import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_structure.WordAndInvertedIndex;
import dynamic_index.index_structure.FrontCodeBlock;
import dynamic_index.index_structure.InvertedIndexOfWord;

import java.io.*;
import java.util.TreeSet;

/**
 * Writes a merge of all indexes using IndexMergingModerator as a source. Works in a similar way to
 * WordsIndexWriter.
 */
public class IndexMergeWriter {

    private final IndexMergingModerator indexMergingModerator;
    private final File mergedIndexDirectory;
    private StringBuilder allWordsSuffixConcatInBlock = new StringBuilder(Statics.STRING_BUILDER_DEFAULT_CAPACITY);
    private int numOfCharactersWrittenInSuffixFile = 0;
    private int numOfBytesWrittenInInvertedIndexFile = 0;

    private BufferedOutputStream frontCodeOutputStream;
    private BufferedOutputStream invertedOutputStream;
    private BufferedWriter bufferedStringConcatWriter;
    private final int numOfTokensInFrontCodeBlock = 8;

    private final TreeSet<WordAndInvertedIndex> wordAndInvertedIndexSet = new TreeSet<>();


    public IndexMergeWriter(IndexMergingModerator indexMergingModerator, String mainIndexDirectory) {
        this.indexMergingModerator = indexMergingModerator;

        File indexDirectory = new File(mainIndexDirectory + File.separator + "merged");
        if (!indexDirectory.mkdir()) {
            System.out.format("Directory %s already exists.", mainIndexDirectory);
        }
        this.mergedIndexDirectory = indexDirectory;
    }

    private void instantiateIndexFiles() {
        File frontCodedFile = new File(mergedIndexDirectory + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        File invIndexFile = new File(mergedIndexDirectory + File.separator + Statics.WORDS_INVERTED_INDEX_FILENAME);
        File stringConcatFile = new File(mergedIndexDirectory + File.separator + Statics.WORDS_CONCAT_FILENAME);
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
        for (WordAndInvertedIndex wordAndInvertedIndex : wordAndInvertedIndexSet) {
            InvertedIndexOfWord invertedIndexOfWord = wordAndInvertedIndex.getInvertedIndex();
            invertedIndexOfWord.writeTo(invertedOutputStream);
        }
    }

    private void writeFrontCodeFile() throws IOException {
        FrontCodeBlock frontCodeBlock = new FrontCodeBlock(wordAndInvertedIndexSet,
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

    void resetIteration() {
        wordAndInvertedIndexSet.clear();
        allWordsSuffixConcatInBlock = new StringBuilder(Statics.STRING_BUILDER_DEFAULT_CAPACITY);
    }


    public void merge() {
        instantiateIndexFiles();
        WordAndInvertedIndex currentWordAndInverted = indexMergingModerator.getNextMergingRow();
        while (currentWordAndInverted != null) {// todo
            wordAndInvertedIndexSet.add(currentWordAndInverted);
            if (wordAndInvertedIndexSet.size() % numOfTokensInFrontCodeBlock == 0) {
                writeMapToFiles();
                resetIteration();
            }
            currentWordAndInverted = indexMergingModerator.getNextMergingRow();
        }
        writeRemainderAndClose();
    }


    private void writeRemainderAndClose() {
        writeMapToFiles(); // last iteration, not necessary to resetIteration here
        closeAllFiles();
    }

    void closeAllFiles() {
        try {
            frontCodeOutputStream.close();
            invertedOutputStream.close();
            bufferedStringConcatWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
