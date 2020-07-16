package dynamic_index;

import dynamic_index.global_tools.IndexInvalidationTool;
import dynamic_index.index_reading.IndexMergingModerator;
import dynamic_index.index_structure.InvertedIndex;
import dynamic_index.index_writing.IndexMergeWriter;
import dynamic_index.index_writing.ReviewsMetaDataIndexWriter;
import dynamic_index.index_writing.SimpleIndexWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static dynamic_index.global_tools.MiscTools.*;
import static dynamic_index.global_tools.ParsingTool.extractHelpfulness;
import static dynamic_index.global_tools.ParsingTool.textToNormalizedTokens;

public class LogMergeIndexWriter {

    private final TemporaryIndex temporaryIndex;
    private final ReviewsMetaDataIndexWriter reviewsMetaDataIndexWriter;
    private final File allIndexesDirectory;

    private int reviewCounter = 1; // not necessarily the number of reviews in index in practice because deletion

    public LogMergeIndexWriter(String allIndexesDirectory) {
        this.allIndexesDirectory = createDirectory(allIndexesDirectory);
        this.reviewsMetaDataIndexWriter = new ReviewsMetaDataIndexWriter(allIndexesDirectory);
        this.temporaryIndex = new TemporaryIndex();

    }

    public void construct(String inputFile) {
        try {
            BufferedReader bufferedReaderOfRawInput = new BufferedReader(new FileReader(inputFile));
            StringBuilder reviewConcatFields = new StringBuilder();

            String line = bufferedReaderOfRawInput.readLine();
            while ((line != null)) {
                if (!line.isEmpty()) {
                    String[] splitArray = line.split(":", 2);
                    String field = splitArray[0];
                    String value = splitArray[1];
                    handleLine(field, value, reviewConcatFields);
                }
                line = bufferedReaderOfRawInput.readLine();
            }
            bufferedReaderOfRawInput.close();
            reviewsMetaDataIndexWriter.closeWriter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleLine(String field, String value, StringBuilder reviewConcatFields) {
        switch (field) {
            case PID_FIELD:
                reviewConcatFields.append(reviewCounter);
                reviewConcatFields.append(value);
                break;
            case HELPFULNESS_FIELD:
                reviewConcatFields.append(extractHelpfulness(value));
                break;
            case SCORE_FIELD:
                reviewConcatFields.append(value.split("\\.")[0]);
                break;
            case REVIEW_TEXT_FIELD:
                reviewConcatFields.append(WHITE_SPACE_SEPARATOR).append(feedTextToIndexWriter(value));
                if (reviewCounter % 1000 == 0) {
                    System.out.println(reviewCounter);
                }
                reviewsMetaDataIndexWriter.writeData(reviewConcatFields.toString());
                reviewConcatFields.replace(0, reviewConcatFields.length(), "");
                incrementReviewCounter();
                break;
            default:
                break;
        }
    }

    private int feedTextToIndexWriter(String reviewTextLine) {
        List<String> filteredSortedTokens = textToNormalizedTokens(reviewTextLine);
        List<String> noBigWords = filteredSortedTokens
                .stream()
                .filter(s -> s.length() <= WORD_MAX_SIZE)
                .collect(Collectors.toList());
        addReviewToTemporaryIndex(noBigWords);
        return noBigWords.size();
    }

    private Map<String, Integer> getHistogram(List<String> wordsInReview) {
        Map<String, Integer> histogram = new HashMap<>();
        for (String word : wordsInReview) {
            if (histogram.containsKey(word)) {
                int frequency = histogram.get(word);
                histogram.put(word, frequency + 1);
            } else {
                histogram.put(word, 1);
            }
        }
        return histogram;
    }

    private void addReviewToTemporaryIndex(List<String> wordsInReview) {
        Map<String, Integer> histogram = getHistogram(wordsInReview);
        for (Map.Entry<String, Integer> wordAndFrequency : histogram.entrySet()) {
            temporaryIndex.add(wordAndFrequency.getKey(), wordAndFrequency.getValue(), reviewCounter);
        }
    }

    /**
     * Looks for the corresponding BYTES in the invalidation vector file and "flips" them to 1
     *
     * @param indexDirectory - directory of the main index, aka "indexes"
     * @param ridsToDelete   - review ids to delete. If not in range, will ignore.
     */
    public void removeReviews(String indexDirectory, int[] ridsToDelete) {
        IndexInvalidationTool.addToInvalidationFile(indexDirectory, ridsToDelete);
    }

    TreeMap<Integer, Integer> getReviewsWithToken(String token) {
        InvertedIndex invertedIndex = temporaryIndex.wordToInvertedIndexMap.get(token);
        TreeMap<Integer, Integer> ridToFrequencies;
        if (invertedIndex == null) { // not found, return empty
            ridToFrequencies = new TreeMap<>();
        } else {
            assert !invertedIndex.isWithFile();
            ridToFrequencies = invertedIndex.getRidToFrequencyMap();
            IndexInvalidationTool.filterResults(allIndexesDirectory.getAbsolutePath(), ridToFrequencies);
        }
        return ridToFrequencies;
    }

    private void incrementReviewCounter() {
        reviewCounter++;
    }

    private class TemporaryIndex {

        private final TreeMap<String, InvertedIndex> wordToInvertedIndexMap = new TreeMap<>();
        private final int TEMPORARY_INDEX_SIZE = 4096;

        private void add(String word, int freqForRid, int rid) {
            if (wordToInvertedIndexMap.containsKey(word)) {
                wordToInvertedIndexMap.get(word).put(rid, freqForRid);
            } else {
                InvertedIndex invertedIndex = new InvertedIndex(word, rid, freqForRid, allIndexesDirectory);
                wordToInvertedIndexMap.put(word, invertedIndex);
            }
            // this method increases the size below by 1
            if (getQueueSize() == TEMPORARY_INDEX_SIZE) {
                try {
                    writeTemporaryIndex();
                    wordToInvertedIndexMap.clear();
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
            assert size <= TEMPORARY_INDEX_SIZE;
            return size;
        }

        private void writeTemporaryIndex() throws IOException {
            TreeMap<Integer, File> indexSizeToIndexDirectory = getSizeToIndexDirectoryMap();
            putTempIndexInMap(indexSizeToIndexDirectory);
            int numberOfFirstIndexDirectoriesToMerge = getNumberOfFirstIndexDirectoriesToMerge(indexSizeToIndexDirectory);

            if (numberOfFirstIndexDirectoriesToMerge == 1) {// just rename Z0 to 0
                Path z0Path = indexSizeToIndexDirectory.firstEntry().getValue().toPath();
                Files.move(z0Path, z0Path.resolveSibling("0"));
            } else if (numberOfFirstIndexDirectoriesToMerge > 1) { // at least two indexes to merge including in-memory
                boolean isMergingAllIndexes = numberOfFirstIndexDirectoriesToMerge == indexSizeToIndexDirectory.size();
                SortedMap<Integer, File> onlyFilesToMerge = indexSizeToIndexDirectory.headMap(numberOfFirstIndexDirectoriesToMerge);
                File mergedDirectory = mergeIndexDirectories(onlyFilesToMerge, isMergingAllIndexes);
                emptyInvalidationFileIfNeeded(isMergingAllIndexes);
                renameMergedDirectory(mergedDirectory, numberOfFirstIndexDirectoriesToMerge);
                (new IndexRemover()).removeFiles(onlyFilesToMerge);
            } else {
                // todo
                assert true;
            }
        }

        private void emptyInvalidationFileIfNeeded(boolean shouldSetNotDirty) {
            // if we are merge all index files, then we don't need to query the invalidation vector again.
            if (shouldSetNotDirty) {

                IndexInvalidationTool.emptyInvalidationFile(allIndexesDirectory.getAbsolutePath());
            }
        }

        private void renameMergedDirectory(File mergedDirectory,
                                           int numberOfFirstIndexDirectoriesToMerge)
                throws IOException {
            Path mergedPath = mergedDirectory.toPath();
            String newDirName = Integer.toString(numberOfFirstIndexDirectoriesToMerge - 1);
            Files.move(mergedPath,
                    mergedPath.resolveSibling(newDirName));
        }

        private File mergeIndexDirectories(SortedMap<Integer, File> sizeToFilesToMerge, boolean mergingAllIndexes) {
            IndexReader indexReader = new IndexReader(allIndexesDirectory.getAbsolutePath(),
                    sizeToFilesToMerge.values());
            if (mergingAllIndexes)
                reviewsMetaDataIndexWriter.closeWriter();
            IndexMergingModerator indexMergingModerator = indexReader.getIndexMergingModeratorLogMerge(mergingAllIndexes);
            IndexMergeWriter indexMergeWriter = new IndexMergeWriter(allIndexesDirectory.getAbsolutePath());
            return indexMergeWriter.merge(indexMergingModerator);
        }

        private void putTempIndexInMap(TreeMap<Integer, File> sizeToFile) {
            File tempIndexDirectory = createDirectory(allIndexesDirectory + File.separator + "Z0");
            SimpleIndexWriter simpleIndexWriter = new SimpleIndexWriter(tempIndexDirectory);
            simpleIndexWriter.write(wordToInvertedIndexMap);
            sizeToFile.put(0, tempIndexDirectory);
        }

        private TreeMap<Integer, File> getSizeToIndexDirectoryMap() {
            TreeMap<Integer, File> sizeToIndexDirectory = new TreeMap<>();
            File[] directories = allIndexesDirectory.listFiles(File::isDirectory);
            assert directories != null;
            for (File directory : directories) {
                int dirSize = getDirectorySizeFromName(directory);
                sizeToIndexDirectory.put(dirSize + 1, directory); // why + 1? because whatever else I tried something still doesn't make
                // sense entirely.
            }
            return sizeToIndexDirectory;
        }

        private int getNumberOfFirstIndexDirectoriesToMerge(TreeMap<Integer, File> sizeToFile) {
            int numberToMerge = 0;
            // at this point we are guaranteed to not break in the first iteration because Z0
            // is in the map with value 0
            for (Map.Entry<Integer, File> entry : sizeToFile.entrySet()) {
                int currentSize = entry.getKey();
                if (currentSize > numberToMerge) {
                    break;
                } else if (currentSize == numberToMerge) {
                    numberToMerge++;
                } else {
                    assert true;
                }
            }
            return numberToMerge;
        }


        private int getDirectorySizeFromName(File directory) {
            String dirName = directory.getName();
            return Integer.parseInt(dirName);
        }

    }
}