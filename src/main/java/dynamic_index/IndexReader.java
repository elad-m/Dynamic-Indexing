package dynamic_index;

import dynamic_index.index_reading.DataIndexReader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Enumeration;


public class IndexReader {

    private final File indexDirectory;
    private final File wordsDictionary;
    private final File wordsStringConcatFile;

    private byte[] indexDictionary;
    private byte[] concatString;
    private int numOfWordsInFrontCodeBlock;


    /**
     * Creates an IndexReader which will read from the given directory
     */
    public IndexReader(String dir) {
        this.indexDirectory = new File(dir);
        wordsDictionary = new File(indexDirectory.getPath()
                + File.separator + Statics.WORDS_FRONT_CODED_FILENAME);
        wordsStringConcatFile = new File(indexDirectory.getPath()
                + File.separator + Statics.WORDS_CONCAT_FILENAME);
        loadDictionariesToArrays();
        loadNumOfTokensPerBlock();
    }

    private void loadNumOfTokensPerBlock() {
        File indexMetaFile = new File(indexDirectory.getPath()+File.separator +Statics.INDEX_META_DATA_FILENAME);
        RandomAccessFile raIndexMeta;
        try {
            raIndexMeta = new RandomAccessFile(indexMetaFile, "r");
            raIndexMeta.seek(Statics.INTEGER_SIZE * 2);
            numOfWordsInFrontCodeBlock = raIndexMeta.readInt();
            System.out.format("front block size words: %d" + System.lineSeparator(),
                    numOfWordsInFrontCodeBlock);
            raIndexMeta.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadDictionariesToArrays() {
        try {
            indexDictionary = Files.readAllBytes(wordsDictionary.toPath());
            concatString = Files.readAllBytes(wordsStringConcatFile.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
     * that id-n is the n-th review containing the given token and freq-n is the
     * number of times that the token appears in review id-n
     * Note that the integers should be sorted by id
     * <p>
     * Returns an empty Enumeration if there are no reviews containing this token
     */
    public Enumeration<Integer> getReviewsWithToken(String token) {
        DataIndexReader wordsFrequencyIndexReader = new DataIndexReader(indexDirectory, numOfWordsInFrontCodeBlock);
        return wordsFrequencyIndexReader.getReviewsWithWord(token, indexDictionary, concatString);
    }

}