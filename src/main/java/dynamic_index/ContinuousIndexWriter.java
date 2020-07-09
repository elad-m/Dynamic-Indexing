package dynamic_index;

import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_structure.InvertedIndex;
import dynamic_index.index_writing.IndexMergeWriter;
import dynamic_index.index_writing.SimpleIndexWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static dynamic_index.Statics.*;

public class ContinuousIndexWriter {

    private File mainIndexDirectory;

    private final TemporaryIndex temporaryIndex;

    private int reviewCounter = 1; // not necessarily the number of reviews in index in practice because deletion
    private int tokenCounter = 0; // token counter only incremented in the mapping stage

    public ContinuousIndexWriter() {
        this.temporaryIndex = new TemporaryIndex();
    }

    public void construct(String inputFile, String mainIndexDirectory) {
        this.mainIndexDirectory = createDirectory(mainIndexDirectory);
        constructContinuously(inputFile, reviewCounter);
    }

    private void constructContinuously(String inputFile, int reviewCounter) {
        System.out.println("Started continuous construction...");
        long blockWriterStartTime = System.currentTimeMillis();
        try {
            BufferedReader bufferedReaderOfRawInput = new BufferedReader(new FileReader(inputFile));
            String line = bufferedReaderOfRawInput.readLine();
            while ((line != null)) {
                if (!line.isEmpty()) {
                    String[] splitArray = line.split(":", 2);
                    String field = splitArray[0];
                    String value = splitArray[1];
                    if (field.equals(REVIEW_TEXT_FIELD)) {
                        feedTextToIndexWriter(value); // first stage of sort
                        incrementReviewCounter();
                    }
                }
                line = bufferedReaderOfRawInput.readLine();
            }
            bufferedReaderOfRawInput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        printElapsedTime(blockWriterStartTime, "First Sort Iteration Time: ");
    }

    private void feedTextToIndexWriter(String reviewTextLine) {
        List<String> filteredSortedTokens = textToNormalizedTokens(reviewTextLine);
        addToTokenCounter(filteredSortedTokens.size()); // counting big words also
        List<String> noBigWords = filteredSortedTokens
                .stream()
                .filter(s -> s.length() <= WORD_MAX_SIZE)
                .collect(Collectors.toList());
        addReviewToTemporaryIndex(noBigWords);
    }

    private void addReviewToTemporaryIndex(List<String> wordsInReview) {
        for(String word: wordsInReview){
            temporaryIndex.add(word, reviewCounter);
        }
    }

    private void incrementTokenCounter() {
        tokenCounter++;
    }

    private void addToTokenCounter(int operand) {
        tokenCounter += operand;
    }


    private void incrementReviewCounter() {
        reviewCounter++;
    }

    TreeMap<Integer, Integer> getReviewsWithToken(String token) {
        InvertedIndex invertedIndex =  temporaryIndex.wordToInvertedIndexMap.get(token);
        TreeMap<Integer, Integer> ridToFrequencies;
        if(invertedIndex == null){ // not found, return empty
            ridToFrequencies = new TreeMap<>();
        } else {
            assert !invertedIndex.isWithFile();
            ridToFrequencies = invertedIndex.getRidToFrequencyMap();
        }
        return ridToFrequencies;
    }

    private class TemporaryIndex {

        private final TreeMap<String, InvertedIndex> wordToInvertedIndexMap = new TreeMap<>();

        private void add(String word, int rid) {
            if (wordToInvertedIndexMap.containsKey(word)) {
                wordToInvertedIndexMap.get(word).put(rid);
            } else {
                InvertedIndex invertedIndex = new InvertedIndex(word, rid, mainIndexDirectory);
                wordToInvertedIndexMap.put(word, invertedIndex);
            }
            // this method increases the size below by zero (same word in the same review) or 1
            int TEMPORARY_INDEX_SIZE = 64;
            if (getQueueSize() == TEMPORARY_INDEX_SIZE) {
                try {
                    writeTemporaryIndex();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private int getQueueSize() {
            int size = 0; // would be the sum of all posting lists lengths, i.e. number of reviews
            // - disregarding frequency - a word is in
            for (InvertedIndex invertedIndex : wordToInvertedIndexMap.values()) {
                size += invertedIndex.getSizeByReviews();
            }
            return size;
        }

        private void writeTemporaryIndex() throws IOException {
            TreeMap<Integer, File> sizeToFile = getSizeToFileMap();
            putTempIndexInMap(sizeToFile);
            int numberOfFirstIndexDirectoriesToMerge = getNumberOfFirstIndexDirectoriesToMerge(sizeToFile);

            if(numberOfFirstIndexDirectoriesToMerge == 1){// just rename Z0 to 0
                Path z0Path = sizeToFile.firstEntry().getValue().toPath();
                Files.move(z0Path, z0Path.resolveSibling("0"));
            } else if(numberOfFirstIndexDirectoriesToMerge > 1){ // at least two indexes to merge
                File mergedDirectory = mergeIndexDirectories(sizeToFile, numberOfFirstIndexDirectoriesToMerge);
                setInvalidationVector(numberOfFirstIndexDirectoriesToMerge == sizeToFile.size());
                renameMergedDirectory(mergedDirectory, numberOfFirstIndexDirectoriesToMerge);
                (new IndexRemover()).removeFiles(sizeToFile);
            }
        }

        private void setInvalidationVector(boolean shouldSetNotDirty) {
            // if we are merge all index files, then we don't need to query through the invalidation vector again.
            setInvalidationVectorIsDirty(!shouldSetNotDirty);
        }

        private void renameMergedDirectory(File mergedDirectory,
                                           int numberOfFirstIndexDirectoriesToMerge)
                throws IOException {
            Path mergedPath = mergedDirectory.toPath();
            String newDirName = Integer.toString(numberOfFirstIndexDirectoriesToMerge -1);
            Files.move(mergedPath,
                    mergedPath.resolveSibling(newDirName));
        }

        private File mergeIndexDirectories(TreeMap<Integer, File> sizeToFile, int numberOfFirstIndexDirectoriesToMerge) {
            IndexReader indexReader = new IndexReader(mainIndexDirectory.getAbsolutePath(),
                    sizeToFile.values(),
                    numberOfFirstIndexDirectoriesToMerge);
            IndexMergingModerator indexMergingModerator = indexReader.getIndexMergingModeratorLogMerge();
            IndexMergeWriter indexMergeWriter = new IndexMergeWriter(mainIndexDirectory.getAbsolutePath());
            return indexMergeWriter.merge(indexMergingModerator);
        }

        private void putTempIndexInMap(TreeMap<Integer, File> sizeToFile) {
            File tempIndexDirectory = createDirectory(mainIndexDirectory + File.separator + "Z0");
            SimpleIndexWriter simpleIndexWriter = new SimpleIndexWriter(tempIndexDirectory);
            simpleIndexWriter.write(wordToInvertedIndexMap);
            sizeToFile.put(0, tempIndexDirectory);
        }

        private TreeMap<Integer, File> getSizeToFileMap() {
            TreeMap<Integer, File> map = new TreeMap<>();
            File[] directories = mainIndexDirectory.listFiles(File::isDirectory);
            assert directories != null;
            for(File directory: directories){
                int dirSize = getDirectorySizeFromName(directory);
                map.put(dirSize + 1, directory); // why + 1? because whatever else I tried something still doesn't make
                // sense entirely.
            }
            return map;
        }

        private int getNumberOfFirstIndexDirectoriesToMerge(TreeMap<Integer, File> sizeToFile) {
            int numberToMerge = 0;
            // at this point we are guaranteed to not break in the first iteration because Z0
            // is in the map with value 0
            for(Map.Entry<Integer, File> entry: sizeToFile.entrySet()){
                int currentSize = entry.getKey();
                if(currentSize > numberToMerge){
                    break;
                } else if (currentSize == numberToMerge){
                    numberToMerge++;
                } else {
                    assert true;
                }
            }
            return  numberToMerge;
        }


        private int getDirectorySizeFromName(File directory) {
            String dirName = directory.getName();
            return Integer.parseInt(dirName);
        }

    }
}
