package dynamic_index.external_sort;


import dynamic_index.global_tools.MiscTools;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static dynamic_index.global_tools.MiscTools.createDirectory;
import static dynamic_index.global_tools.MiscTools.estimateBestSizeOfWordsBlocks;

public class TermToReviewBlockWriter {

    public static final int BYTE_ARRAY_MAX_SIZE = 100000;
    private final List<TermIdReviewIdPair> termIdReviewIdPairs;
    public final int BLOCK_SIZE_IN_INT_PAIRS;
    public final int BLOCK_SIZE_IN_BYTES;

    int numOfFilesCreated = 0;
    private BufferedOutputStream currentFileBOF;
    File mergeFilesDirectory;

    public TermToReviewBlockWriter(String indexDirectory, int numOfTokens) {
        BLOCK_SIZE_IN_INT_PAIRS = estimateBestSizeOfWordsBlocks(numOfTokens, false);
        BLOCK_SIZE_IN_BYTES = BLOCK_SIZE_IN_INT_PAIRS * MiscTools.PAIR_OF_INT_SIZE_IN_BYTES;
        termIdReviewIdPairs = new ArrayList<>(BLOCK_SIZE_IN_INT_PAIRS);
        createMergeFilesDirectory(indexDirectory);
        createNewFile();
    }

    public File getMergeFilesDirectory() {
        return this.mergeFilesDirectory;
    }

    public int getBLOCK_SIZE_IN_INT_PAIRS() {
        return this.BLOCK_SIZE_IN_INT_PAIRS;
    }

    private void createMergeFilesDirectory(String indexDirectory) {
        final String TEMP_FILE_STORE = indexDirectory + File.separator
                +  MiscTools.MERGE_FILES_DIRECTORY_NAME + "0";
        this.mergeFilesDirectory = createDirectory(TEMP_FILE_STORE);
    }

    private void createNewFile() {
        numOfFilesCreated++;
        String FILE_NAME_PATTERN = "sortedBlock";
        String BINARY_FILE_SUFFIX = ".bin";
        String newFileName = mergeFilesDirectory.getPath() + File.separator
                + FILE_NAME_PATTERN + numOfFilesCreated + BINARY_FILE_SUFFIX;
//        System.out.println("NEW FILE IN WRITER #" + numOfFilesCreated);
        try {
            currentFileBOF = new BufferedOutputStream(new FileOutputStream(newFileName), BLOCK_SIZE_IN_BYTES);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Adds pair of int values to the class inner List data members.
     * Calls Write block to file when reaching block size
     *
     * @param tid - term ID
     * @param rid - Review ID
     */
    public void add(int tid, int rid) {
        termIdReviewIdPairs.add(new TermIdReviewIdPair(tid, rid));
        if (isEndOfBlock()) {
            writeBlockToCurrentFile();
            createNewFile();
        }
    }

    private boolean isEndOfBlock() {
        return termIdReviewIdPairs.size() == BLOCK_SIZE_IN_INT_PAIRS;
    }

    private void writeBlockToCurrentFile() {
        Collections.sort(termIdReviewIdPairs);
        try {
            do {
                byte[] pairsAsBytes = toByteArray(this.termIdReviewIdPairs);
                currentFileBOF.write(pairsAsBytes);
            } while (!termIdReviewIdPairs.isEmpty()); // if the array is too big
            currentFileBOF.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeWriter() {
        writeBlockToCurrentFile();
    }

    public static byte[] toByteArray(List<TermIdReviewIdPair> termIdReviewIdPairs) {
        int numOfPairs = termIdReviewIdPairs.size();
        byte[] listInBytes;
        if (numOfPairs <= BYTE_ARRAY_MAX_SIZE) {
            listInBytes = new byte[MiscTools.PAIR_OF_INT_SIZE_IN_BYTES * numOfPairs];
            int writeToByteArrayOffset = 0;
            for (TermIdReviewIdPair termIdReviewIdPair : termIdReviewIdPairs) {
                insertPairToByteArray(listInBytes, writeToByteArrayOffset, termIdReviewIdPair);
                writeToByteArrayOffset += MiscTools.PAIR_OF_INT_SIZE_IN_BYTES;
            }
            termIdReviewIdPairs.clear(); // for the while loop in the calling function
        } else {
//            System.out.println("<USED BUFFER WRITING>");
            int batchSize = getBatchSize(termIdReviewIdPairs.size()); // in pairs
            int listInBytesSize = MiscTools.PAIR_OF_INT_SIZE_IN_BYTES * batchSize;
            listInBytes = new byte[listInBytesSize];
            int writeToByteArrayOffset = 0;
            for (TermIdReviewIdPair termIdReviewIdPair : termIdReviewIdPairs) {
                if (writeToByteArrayOffset == listInBytesSize) {
                    break;
                }
                insertPairToByteArray(listInBytes, writeToByteArrayOffset, termIdReviewIdPair);
//                iterator.remove();
                writeToByteArrayOffset += MiscTools.PAIR_OF_INT_SIZE_IN_BYTES;
            }
            if (batchSize > 0) {
                termIdReviewIdPairs.subList(0, batchSize).clear();
            }
        }
        return listInBytes;
    }

    private static int getBatchSize(int listSize) {
        int splitFactor = 2;
        while (listSize / splitFactor > BYTE_ARRAY_MAX_SIZE) {
            splitFactor++;
        }
        return listSize / splitFactor;
    }

    private static void insertPairToByteArray(byte[] insertTo, int offset, TermIdReviewIdPair termIdReviewIdPair) {
        final int INTEGER_SIZE = MiscTools.INTEGER_SIZE;
        byte[] tidArray = ByteBuffer.allocate(4).putInt(termIdReviewIdPair.tid).array();
        byte[] ridArray = ByteBuffer.allocate(4).putInt(termIdReviewIdPair.rid).array();
        System.arraycopy(tidArray, 0, insertTo, offset, INTEGER_SIZE);
        System.arraycopy(ridArray, 0, insertTo, offset + INTEGER_SIZE, INTEGER_SIZE);
    }

}
