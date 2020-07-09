package dynamic_index;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("SpellCheckingInspection")
public final class Statics {

    public static final int BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK = 8;
    public static final int WORDS_DEFAULT_MAX_TEMP_FILES = 1024;
    public static final int STRING_BUILDER_DEFAULT_CAPACITY = 32;
    public static final int WORD_MAX_SIZE = 127;
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    public static final int INTEGER_SIZE = Integer.BYTES;
    public static final int PAIR_OF_INT_SIZE_IN_BYTES = Integer.BYTES * 2;

    public static final String REVIEW_TEXT_FIELD = "review/text";

    public static final String WORDS_CONCAT_FILENAME = "wordsConcatFile.bin";
    public static final String WORDS_FRONT_CODED_FILENAME = "wordsFrontCodedToPointers.bin";
    public static final String WORDS_INVERTED_INDEX_FILENAME = "wordsInvertedIndex.bin";

//    public static final String MAIN_INDEX_META_DATA_FILENAME = "indexMetaData.bin";
//    public static final String ALL_INDEXES_META_DATA_FILENAME = "allIndexesMetaData.bin";

    public static final String INVALIDATION_VECTOR_FILENAME = "invalidationVector.bin";

    public static final String MERGE_FILES_DIRECTORY_NAME = "mergeFilesDirectory";
    public static final String WORDS_SORTED_FILE_NAME = "WORDS_SORTED.bin";
    public static final String BINARY_FILE_SUFFIX = ".bin";

    public static final String TERM_MAP_FILE_DEBUG = "TermToTermID.txt";
    public static final String WORDS_MAPPING = "words";

    public static final String MERGED_INDEX_DIRECTORY = "mergedIndex";
    public static final String INDEXES_DIR_NAME = "indexes";
    public static final String LOG_MERGE_INDEXES_DIR_NAME = "logMergeIndexes";


    private static boolean invalidationVectorIsDirty = false;

    public static int calculateNumOfTokensInFrontCodeBlock(int numOfTokens) {
        //        if (numOfTokens > 10000 && numOfTokens <= 1000000) { // 10,000 to 1 million
//            tokensPerBlock = (int) Math.pow(tokensPerBlock, 2);
//        } else if (numOfTokens > 1000000) {
//            tokensPerBlock = (int) Math.pow(tokensPerBlock, 3);
//        }
        return BASE_NUM_OF_TOKENS_IN_FRONT_CODE_BLOCK;
    }

    public static int roundDownToMultiplicationOf(int roundItDown, int multiplicationOf) {
        return (roundItDown / multiplicationOf) * multiplicationOf;
    }

    public static void printElapsedTime(long startTime, String methodName) {
        long endTime = System.currentTimeMillis();
        long elapsedTimeMilliSeconds = endTime - startTime;
        long elapsedTimeSeconds = elapsedTimeMilliSeconds / 1000;
        long elapsedTimeMinutes = elapsedTimeSeconds / 60;
        System.out.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);
    }

    public static void printElapsedTimeToLog(PrintWriter tlog, long startTime, String methodName) {
        long endTime = System.currentTimeMillis();
        long elapsedTimeMilliSeconds = endTime - startTime;
        long elapsedTimeSeconds = elapsedTimeMilliSeconds / 1000;
        long elapsedTimeMinutes = elapsedTimeSeconds / 60;
        System.out.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);

        tlog.format("Elapsed time for %s: %d in mins, %d in secs, %d in ms\n",
                methodName,
                elapsedTimeMinutes,
                elapsedTimeSeconds,
                elapsedTimeMilliSeconds);
    }


    public static int estimateBestSizeOfWordsBlocks(final long numOfTokens, final boolean withFreeMemory) {
        long estimatedSizeOfFile = numOfTokens * Statics.PAIR_OF_INT_SIZE_IN_BYTES;
        long blockSize = calculateSizeOfBlock(estimatedSizeOfFile); // IN BYTES
        long blockSizeInPairs = blockSize / 8;
        System.out.println("blockSize in BYTES:" + blockSize);
        System.out.println("blockSize in PAIRS:" + blockSizeInPairs);
        assert blockSizeInPairs <= Integer.MAX_VALUE;
        return (int) blockSizeInPairs;
    }

    private static long calculateSizeOfBlock(final long sizeOfFile) {
        long baseSizeOfBlock = (long) Math.ceil((double) sizeOfFile / Statics.WORDS_DEFAULT_MAX_TEMP_FILES);
        return Statics.roundUpToProductOfPairSize(baseSizeOfBlock);
    }

    public static int roundUpToProductOfPairSize(long blockSize) {
        if (blockSize % PAIR_OF_INT_SIZE_IN_BYTES != 0) {
            blockSize += PAIR_OF_INT_SIZE_IN_BYTES - (blockSize % PAIR_OF_INT_SIZE_IN_BYTES);
        } else if (blockSize == 0) {
            blockSize = (int) Math.pow(PAIR_OF_INT_SIZE_IN_BYTES, 3);
        } // else it's already a non-zero mult of pair-size
        return (int) blockSize;
    }

    public static Map<Integer, String> swapHashMapDirections(Map<String, Integer> termToTermID) {
        Map<Integer, String> swapped = termToTermID.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        termToTermID.clear();
        return swapped;
    }

    public static boolean isInvalidationVectorIsDirty() {
        return invalidationVectorIsDirty;
    }

    public static void setInvalidationVectorIsDirty(boolean invalidationVectorIsDirty) {
        Statics.invalidationVectorIsDirty = invalidationVectorIsDirty;
    }


    public static byte[] intToByteArray(int intToConvert) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(intToConvert);
        return b.array();
    }

    public static void deleteSortedFile(File indexDirectory) throws IOException {
        File wordsSortedFile = new File(indexDirectory + File.separator
                + Statics.WORDS_SORTED_FILE_NAME);
        Files.delete(wordsSortedFile.toPath());
    }


    /*
    No empty strings, all alphanumeric, lower cased BUT WITH long words > 127 chars
     */
    public static List<String> textToNormalizedTokens(String reviewTextLine) {
        List<String> filteredTokens = new ArrayList<>();
        String[] tokens = reviewTextLine.split("[^a-zA-Z0-9]+"); // alphanumeric
        for (String token : tokens) {
            if (!token.equals("")) // no empty
                filteredTokens.add(token.toLowerCase());  // lower case but includes > 127
        }
        Collections.sort(filteredTokens);
        return filteredTokens;
    }

    public static File createDirectory(String dir) {
        File directory = new File(dir);
        if (!directory.mkdir()) {
            System.out.format("Directory %s already exists.", dir);
        }
        return directory;
    }

    public static void writeHashmapToFile(Map<String, Integer> wordTermToTermID,
                                          File indexDirectory,
                                          String mappingType,
                                          int inputScaleType) {
        System.out.println("writing hashmap...");
        TreeMap<String, Integer> orderedTermMapping = new TreeMap<>(wordTermToTermID);
        try {
            FileWriter fw = new FileWriter(indexDirectory.getPath() + File.separator
                    + mappingType + TERM_MAP_FILE_DEBUG);
            for (Map.Entry<String, Integer> entry : orderedTermMapping.entrySet()) {
                fw.write(entry.getKey());
                fw.write(' ');
                fw.write(entry.getValue().toString());
                fw.write(';');
                fw.write('\n');
            }
            fw.flush();
            fw.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static Map<Integer, String> loadMapFromFile(File indexDirectory, String typeOfMapping) {
        File mapFile = new File(indexDirectory.getPath() + File.separator
                + typeOfMapping + TERM_MAP_FILE_DEBUG);
        Map<Integer, String> loadedMap = new HashMap<>(1000000);
        try (BufferedReader mapBufferedReader =
                     new BufferedReader(new FileReader(mapFile))) {
            String line = mapBufferedReader.readLine();
            while (line != null) {
                String[] wordAndId = line.split("[^a-zA-Z0-9]");
                assert wordAndId.length == 2;
                loadedMap.put(Integer.parseInt(wordAndId[1]), wordAndId[0]);
                line = mapBufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return loadedMap;
    }

    public static <T> void printList(List<T> list, File indexPath, String name) {
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
