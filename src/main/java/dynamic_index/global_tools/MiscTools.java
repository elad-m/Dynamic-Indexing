package dynamic_index.global_tools;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@SuppressWarnings("SpellCheckingInspection")
public class MiscTools {

    public static final int BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK = 8;
    public static final int WORDS_DEFAULT_MAX_TEMP_FILES = 1024;
    public static final int STRING_BUILDER_DEFAULT_CAPACITY = 32;
    public static final int WORD_MAX_SIZE = 127;

    public static final int INTEGER_SIZE = Integer.BYTES;
    public static final int PAIR_OF_INT_SIZE_IN_BYTES = Integer.BYTES * 2;

    public static final String PID_FIELD = "product/productId";
    public static final String HELPFULNESS_FIELD = "review/helpfulness";
    public static final String SCORE_FIELD = "review/score";
    public static final String REVIEW_TEXT_FIELD = "review/text";
    public static final String WHITE_SPACE_SEPARATOR = " ";

    public static final String WORDS_CONCAT_FILENAME = "wordsConcatFile.bin";
    public static final String WORDS_FRONT_CODED_FILENAME = "wordsFrontCodedToPointers.bin";
    public static final String WORDS_INVERTED_INDEX_FILENAME = "wordsInvertedIndex.bin";
    public static final String REVIEW_META_DATA_FILENAME = "reviewMetaData.bin";
    public static final String REVIEW_META_DATA_TEMP_FILENAME = "ridToMetaDataTemp.bin";
    public static final String INVALIDATION_FILENAME = "invalidation.bin";

    public static final String MERGE_FILES_DIRECTORY_NAME = "mergeFilesDirectory";
    public static final String WORDS_SORTED_FILE_NAME = "WORDS_SORTED.bin";
    public static final String BINARY_FILE_SUFFIX = ".bin";

    public static final String TERM_MAP_FILE_DEBUG = "wordsTermToTermID.txt";
    public static final String EXTERNAL_E4_MAP_OF_WORDS = "e4.txt";
    public static final String EXTERNAL_E5_MAP_OF_WORDS = "e5.txt";
    public static final String DIR_NAME_FOR_RANDOM_WORDS = "words";

    public static final String MERGED_INDEX_DIRECTORY = "mergedIndex";
    public static final String INDEXES_DIR_NAME = "indexes";
    public static final String LOG_MERGE_INDEXES_DIR_NAME = "logMergeIndexes";

    public static final String LOG_FIRST_BUILD = "\nFirst index build: Log-Merged";
    public static final String SIMPLE_FIRST_BUILD = "\nFirst index build: Simple-Merged";

    public static final String ENTIRE_INSERTIONS_MESSAGE = "\nEntire index insertions ";
    public static final String SINGLE_INSERTION_MESSAGE = "Index insertion number ";


    //====================================  Sizes  =====================================//

    public static int roundDownToMultiplicationOf(int roundItDown, int multiplicationOf) {
        return (roundItDown / multiplicationOf) * multiplicationOf;
    }

    public static int estimateBestSizeOfWordsBlocks(final long numOfTokens, final boolean withFreeMemory) {
        long estimatedSizeOfFile = numOfTokens * MiscTools.PAIR_OF_INT_SIZE_IN_BYTES;
        long blockSize = calculateSizeOfBlock(estimatedSizeOfFile); // IN BYTES
        long blockSizeInPairs = blockSize / 8;
        assert blockSizeInPairs <= Integer.MAX_VALUE;
        return (int) blockSizeInPairs;
    }

    private static long calculateSizeOfBlock(final long sizeOfFile) {
        long baseSizeOfBlock = (long) Math.ceil((double) sizeOfFile / MiscTools.WORDS_DEFAULT_MAX_TEMP_FILES);
        return MiscTools.roundUpToProductOfPairSize(baseSizeOfBlock);
    }

    public static int roundUpToProductOfPairSize(long blockSize) {
        if (blockSize % PAIR_OF_INT_SIZE_IN_BYTES != 0) {
            blockSize += PAIR_OF_INT_SIZE_IN_BYTES - (blockSize % PAIR_OF_INT_SIZE_IN_BYTES);
        } else if (blockSize == 0) {
            blockSize = (int) Math.pow(PAIR_OF_INT_SIZE_IN_BYTES, 3);
        } // else it's already a non-zero mult of pair-size
        return (int) blockSize;
    }

    public static byte[] intToByteArray(int intToConvert) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(intToConvert);
        return b.array();
    }

    //=========================  External Sort  =====================================//

    public static Map<Integer, String> swapHashMapDirections(Map<String, Integer> termToTermID) {
        Map<Integer, String> swapped = termToTermID.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        termToTermID.clear();
        return swapped;
    }

    public static void deleteSortedFile(File indexDirectory) throws IOException {
        File wordsSortedFile = new File(indexDirectory + File.separator
                + MiscTools.WORDS_SORTED_FILE_NAME);
        Files.delete(wordsSortedFile.toPath());
    }

    //===============================  Misc  =====================================//


    public static File createDirectory(String createDirectoryInThisPath) {
        File directory = new File(createDirectoryInThisPath);
        if (!directory.mkdir()) {
            System.out.format("Directory %s already exists." + System.lineSeparator(), createDirectoryInThisPath);
        }
        return directory;
    }

    public static int getRandomNumber(int lowerBound, int upperBound){
        return ThreadLocalRandom.current().nextInt(lowerBound - 1, upperBound);
    }

    //=========================  Writing for Meta and Testing  =====================================//

    public static void writeMapToFile(Map<String, Integer> wordTermToTermID,
                                      File indexDirectory) {
        System.out.println("writing hashmap...");
        TreeMap<String, Integer> orderedTermMapping = new TreeMap<>(wordTermToTermID);
        try {
            FileWriter fw = new FileWriter(indexDirectory.getPath() + File.separator
                    + TERM_MAP_FILE_DEBUG, true);
            for (Map.Entry<String, Integer> entry : orderedTermMapping.entrySet()) {
                writeEntry(fw, entry.getKey(), entry.getValue());
            }
            fw.flush();
            fw.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void writeSetToFile(Set<String> words,
                                      File indexDirectory) {
        System.out.println("writing all different words...");
        try {
            FileWriter fw = new FileWriter(indexDirectory.getPath() + File.separator
                    + TERM_MAP_FILE_DEBUG, true);
            int wordsCounter = 1;
            for (String word : words) {
                writeEntry(fw, word, wordsCounter);
                wordsCounter++;
            }
            fw.flush();
            fw.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void writeEntry(FileWriter fw, String word, Integer number) throws IOException {
        fw.write(word);
        fw.write(' ');
        fw.write(number.toString());
        fw.write(';');
        fw.write('\n');

    }

    public static <T> void writeListToFile(List<T> list, File indexPath, String name) {
        try {
            FileWriter fw = new FileWriter(indexPath.getPath() + File.separator + name);
            for (T t : list) {
                fw.write(t.toString());
                fw.write(';');
                fw.write('\n');
            }
            fw.flush();
            fw.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
