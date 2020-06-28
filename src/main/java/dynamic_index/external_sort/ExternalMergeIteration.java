package dynamic_index.external_sort;

import dynamic_index.Statics;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

public class ExternalMergeIteration {

    final int BLOCK_SIZE_IN_INT_PAIRS;

    int iterationNumber;
    File filesToMergeDirectory;
    int numOfMergeFiles;
    int numOfFilesToMergeToOneFile;
    File[] mergeFilesToRead;
    List<NumberedQueue> mergeFilesQueues;
    OutputBlockWriter outputBlockWriter;

    ExternalMergeIteration(int iterationNumber, File[] mergeFiles, File indexDirectory, int blockSizeInPairs) {
        assert mergeFiles != null && mergeFiles.length > 0;
        this.filesToMergeDirectory = mergeFiles[0].getParentFile();
        this.iterationNumber = iterationNumber;
        this.numOfMergeFiles = mergeFiles.length;
        System.out.println("numOfMergeFiles: " + numOfMergeFiles + " " + iterationNumber);
        this.numOfFilesToMergeToOneFile = Math.max(2, (int)Math.ceil(Math.sqrt(numOfMergeFiles)));
        this.mergeFilesToRead =  mergeFiles;
        this.mergeFilesQueues = new ArrayList<>();
        this.BLOCK_SIZE_IN_INT_PAIRS = Statics.roundUpToProductOfPairSize(blockSizeInPairs / numOfMergeFiles);
        System.out.println("BLOCK SIZE IN PAIRS:" + BLOCK_SIZE_IN_INT_PAIRS +
                " IN ITERATION: " + iterationNumber);
        this.outputBlockWriter =
                new OutputBlockWriter(indexDirectory.getPath(),
                        BLOCK_SIZE_IN_INT_PAIRS, this.iterationNumber);

    }

    File merge() {
        for (int n = 0; n < numOfMergeFiles; n += numOfFilesToMergeToOneFile) {
            // merge all subsets of temp files
//            System.out.println("MERGE SUBSET: " + n);
            outputBlockWriter.createNewFile();
            int stopSubIterationAt = (Math.min(n + numOfFilesToMergeToOneFile, numOfMergeFiles));
            for (int i = n; i < stopSubIterationAt; i++) {
                //load first blocks of subset of temp files
                NumberedQueue currentQueue = new NumberedQueue(i, mergeFilesToRead[i], BLOCK_SIZE_IN_INT_PAIRS);
                mergeFilesQueues.add(currentQueue);
            }
            mergeCurrentSubsetOfFiles();
            outputBlockWriter.closeSortedFile();
            verifyClosedQueues();
            mergeFilesQueues.clear();
            System.gc();
        }
        deleteFilesToMergeDirectory();
        return outputBlockWriter.getMergeFilesDirectory();
    }

    private void verifyClosedQueues() {
        for(NumberedQueue queue: mergeFilesQueues){
            if(!queue.closed){
                System.err.format("This queue did not close: %d. iteration: %d\n", queue.runNum, this.iterationNumber);
                if(!queue.queue.isEmpty()){
                    System.err.println("Its queue is also NOT empty.");
                }
            }
        }
    }

    private void deleteFilesToMergeDirectory() {
        try {
            for(File f: mergeFilesToRead){
                Files.delete(f.toPath());
            }
            Files.delete(this.filesToMergeDirectory.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mergeCurrentSubsetOfFiles() {
        NumberedQueue minimumQueue;
        TermIdReviewIdPair currMinPair;
        while ((minimumQueue = getMinimumQueue()) != null) {
            currMinPair = minimumQueue.poll();
            outputBlockWriter.add(currMinPair.tid, currMinPair.rid);
        }
    }


    private NumberedQueue getMinimumQueue() {
        assert mergeFilesQueues.size() > 0;
        NumberedQueue minQueue = getFirstNotDoneQueue();

        if (minQueue == null) { // all files have been read and written to output buffer
            return null;
        } else {
            for (NumberedQueue currentQueue: mergeFilesQueues){
                if(currentQueue.isQueueNotDone()){
                    TermIdReviewIdPair minPairForCurrentQueue = currentQueue.peek();
                    assert minPairForCurrentQueue != null;
                    TermIdReviewIdPair prevMinPair = minQueue.peek();
                    if ( minPairForCurrentQueue.compareTo(prevMinPair) < 0){
                        minQueue = currentQueue;
                    }
                }
            }
            return minQueue;
        }
    }

    private NumberedQueue getFirstNotDoneQueue(){
        for (NumberedQueue numberedQueue: mergeFilesQueues){
            if(numberedQueue.isQueueNotDone()){
                return numberedQueue;
            }
        }
        return null;
    }


    static class NumberedQueue {

        final int BLOCK_SIZE_IN_PAIRS;
        final int runNum;
        final Queue<TermIdReviewIdPair> queue;
        RandomAccessFile raInputFile;
        boolean closed =false;
        boolean isDoneReadingFile = false; // there are no more bytes to read from input file
        boolean isQueueDone = false; // the above plus the inner queue is empty

        NumberedQueue(int runNum, File inputFile, int blockSizeInPairs) {
            BLOCK_SIZE_IN_PAIRS = blockSizeInPairs;
            this.runNum = runNum;
            try {
                this.raInputFile =  new RandomAccessFile(inputFile, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            this.queue = new PriorityQueue<>(blockSizeInPairs);
            load();
        }

        TermIdReviewIdPair peek(){
            return queue.peek();
        }

        TermIdReviewIdPair poll(){
            TermIdReviewIdPair termIdReviewIdPair = queue.poll();
            if (queue.isEmpty()){
                if(!isDoneReadingFile) {
                    load();
                } else {
                    setQueueDone();
                }
            }
            return termIdReviewIdPair;
        }


        boolean isQueueNotDone(){
            return !isQueueDone;
        }

        private void load() {
            ByteBuffer blockByteBuffer = getByteBuffer(raInputFile);
            for (int i = 0; i < BLOCK_SIZE_IN_PAIRS; i++) {
                /* there will be bad zeros at the last block of the file */
                if (blockByteBuffer.getInt(i * Statics.PAIR_OF_INT_SIZE_IN_BYTES) == 0){
                    break; // possibly hides other reasons for zeros...
                }
                int tid = blockByteBuffer.getInt();
                int rid = blockByteBuffer.getInt();
                queue.add(new TermIdReviewIdPair(tid, rid));
            }
            if(queue.isEmpty()){ // read empty block and queue is empty
                setQueueDone();
            }
        }

        private void setQueueDone(){
            isQueueDone = true;
            close();
        }

        private ByteBuffer getByteBuffer(RandomAccessFile randomAccessFile) {
            final int blockSizeInBytes = this.BLOCK_SIZE_IN_PAIRS * Statics.PAIR_OF_INT_SIZE_IN_BYTES;
            byte[] blockFromFile = new byte[blockSizeInBytes];
            try {
                int numOfBytesRead = randomAccessFile.read(blockFromFile);
                if (numOfBytesRead < blockSizeInBytes){
                    this.isDoneReadingFile = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return ByteBuffer.wrap(blockFromFile);
        }

        void close(){
            try {
                raInputFile.close();
                closed = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class OutputBlockWriter {

        private final List<TermIdReviewIdPair> termIdReviewIdPairs;
        public final int BLOCK_SIZE_IN_INT_PAIRS;

//        private RandomAccessFile currentFile;
        private BufferedOutputStream currentFile;
        private File mergeFilesDirectory;
        private final int iterationNumber;
        private int sortedFilesCounter = 1;

        public OutputBlockWriter(String indexDirectoryName, int blockSize, int iterationNumber) {
            this.iterationNumber = iterationNumber;
            BLOCK_SIZE_IN_INT_PAIRS = blockSize;
            termIdReviewIdPairs = new ArrayList<>(BLOCK_SIZE_IN_INT_PAIRS);
            createMergeFileDirectory(indexDirectoryName);
        }

        private void createMergeFileDirectory(String indexDirectoryName) {
            final String TEMP_FILE_STORE = indexDirectoryName + File.separator +
                    Statics.MERGE_FILES_DIRECTORY_NAME + iterationNumber;
            File mergeFilesDirectory = new File(TEMP_FILE_STORE);
            if (!mergeFilesDirectory.mkdir()) {
                System.out.println("Directory " + TEMP_FILE_STORE + " already exists.");
            }
            this.mergeFilesDirectory = mergeFilesDirectory;
        }

        File getMergeFilesDirectory(){
            return this.mergeFilesDirectory;
        }

        void createNewFile() {
            final String BINARY_FILE_SUFFIX = Statics.BINARY_FILE_SUFFIX;
            final String FILE_NAME_PATTERN = "outputIteration";
            String newFileName = mergeFilesDirectory.getPath() + File.separator
                    + FILE_NAME_PATTERN + iterationNumber + "-"
                    + sortedFilesCounter + BINARY_FILE_SUFFIX;
            countNewFile(); // yes, counting after taking the name
            try {
//                currentFile = new RandomAccessFile(new File(newFileName), "rw");
                currentFile = new BufferedOutputStream(new FileOutputStream(newFileName),
                        BLOCK_SIZE_IN_INT_PAIRS*Statics.PAIR_OF_INT_SIZE_IN_BYTES);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        private void countNewFile(){
            sortedFilesCounter++;
        }

        /**
         * Adds pair of int values to the class inner Vector data members.
         * Calls Write block to file when reaching block size
         *
         * @param tid - term ID
         * @param rid - Review ID
         */
        public void add(int tid, int rid) {
            termIdReviewIdPairs.add(new TermIdReviewIdPair(tid, rid));
            if (isEndOfBlock()) {
                writeBlockToCurrentFile();
            }
        }

        private boolean isEndOfBlock() {
            return termIdReviewIdPairs.size() == BLOCK_SIZE_IN_INT_PAIRS;
        }

        private void writeBlockToCurrentFile() {
            if(!termIdReviewIdPairs.isEmpty()){
                try {
                    do {
                        byte[] pairsAsBytes = TermToReviewBlockWriter.toByteArray(this.termIdReviewIdPairs);
                        currentFile.write(pairsAsBytes);
                    } while (!termIdReviewIdPairs.isEmpty()); // if the array is too big
                } catch (IOException e) {
                    e.printStackTrace();
                }
//                byte[] pairsAsBytes = TermToReviewBlockWriter.toByteArray(this.termIdReviewIdPairs);
//                assert pairsAsBytes.length > 0;
//                try {
//                    currentFile.write(pairsAsBytes);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                termIdReviewIdPairs.clear();
            }
        }

        public void closeSortedFile(){
            writeBlockToCurrentFile();
            try {
                currentFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



    }
}
