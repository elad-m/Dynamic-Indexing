package dynamic_index.external_sort;

import dynamic_index.global_tools.MiscTools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class ExternalMergeSort {

    private File filesToMergeDirectory;
    private final File indexDirectory;
    private final int BLOCK_SIZE_IN_INT_PAIRS;


    public ExternalMergeSort(File indexDirectory, File mergeFilesDirectory , int blockSizeInPairs) {
        assert mergeFilesDirectory != null;
        this.indexDirectory = indexDirectory;
        this.filesToMergeDirectory = mergeFilesDirectory;
        BLOCK_SIZE_IN_INT_PAIRS = blockSizeInPairs;
        merge();
    }


    private void merge() {
        int i = 1;
        File[] filesToMerge = this.filesToMergeDirectory.listFiles();
        while (filesToMerge != null && filesToMerge.length > 1) {
            ExternalMergeIteration externalMergeIteration =
                    new ExternalMergeIteration(i, filesToMerge, indexDirectory, BLOCK_SIZE_IN_INT_PAIRS);
            this.filesToMergeDirectory = externalMergeIteration.merge();
            i++;
            filesToMerge = this.filesToMergeDirectory.listFiles();
            System.gc();
        }
        if (filesToMerge != null && filesToMerge.length == 1) {
            renameFinalFile(filesToMerge[0]);

        } else {
            System.err.println("Renaming unsuccessful");
        }

    }

    private void renameFinalFile(File sortedFileToRename) {
        File parentOfSortedFileToRename = sortedFileToRename.getParentFile();
        File renamedFile = new File(indexDirectory + File.separator + MiscTools.WORDS_SORTED_FILE_NAME);
        try {
            Files.move(sortedFileToRename.toPath(), renamedFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.delete(parentOfSortedFileToRename.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}

