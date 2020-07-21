package dynamic_index;

import dynamic_index.global_tools.MiscTools;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.SortedMap;

import static dynamic_index.global_tools.MiscTools.TERM_MAP_FILE_DEBUG;
import static dynamic_index.global_tools.MiscTools.WORDS_MAPPING;


public class IndexRemover {

    private boolean useExceptions = false;

    void removeFilesAfterMerge(String dir) {
        useExceptions = true;
        File file = new File(dir);
        deleteFileOrDirectory(file);
    }

    void removeFiles(SortedMap<Integer, File> sizeToFile){
        useExceptions = true;
        for(File file : sizeToFile.values()){
            deleteDirectory(file);
        }
    }

    public void removeAllIndexFiles(String dir) {
        useExceptions = false;
        File file = new File(dir);
        deleteFileOrDirectory(file);
    }

    private void deleteFileOrDirectory(File toDelete) {
        try {
            if (toDelete.isDirectory()) {
                deleteDirectory(toDelete);
            } else {
                singleDelete(toDelete);
            }
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }

    private void deleteDirectory(File toDelete) {
        try {
            // not deleting the merged index directory and files
            if(useExceptions){
                deleteDirectoryWithExceptions(toDelete);
            } else {
                File[] childFiles = toDelete.listFiles();
                if (childFiles != null) {
                    if (childFiles.length == 0) { //Directory is empty. Proceed for deletion
                        singleDelete(toDelete);
                    } else {
                        for (File childFilePath : childFiles) {
                            deleteFileOrDirectory(childFilePath);
                        }
                        // not deleting the indexes directory itself after deleting its contents
                        deleteDirectory(toDelete); // calling again, now should be empty
                    }
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private void deleteDirectoryWithExceptions(File toDelete) {
        try {
            // not deleting the merged index directory and files
            if (!toDelete.getName().equals(MiscTools.MERGED_INDEX_DIRECTORY)) {
                File[] childFiles = toDelete.listFiles();
                if (childFiles != null) {
                    if (childFiles.length == 0) { //Directory is empty. Proceed for deletion
                        singleDelete(toDelete);
                    } else {
                        for (File childFilePath : childFiles) {
                            deleteFileOrDirectory(childFilePath);
                        }
                        // not deleting the indexes directory itself after deleting its contents
                        if (!toDelete.getName().equals(MiscTools.INDEXES_DIR_NAME)) {
                            deleteDirectory(toDelete); // calling again, now should be empty
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private void singleDelete(File singleFileToDelete) {
        Path dirPathToDelete = singleFileToDelete.toPath();
        if (shouldDeleteFile(singleFileToDelete.getName())) {
            try {
                Files.delete(dirPathToDelete);
            } catch (NoSuchFileException x) {
                System.err.format("%s: no such file or directory%n", dirPathToDelete.toString());
            } catch (DirectoryNotEmptyException x) {
                System.err.format("%s not empty%n", dirPathToDelete.toString());
            } catch (IOException x) {
                x.printStackTrace();
                System.out.println("FILE NAME: " + singleFileToDelete.getName());
                System.err.println(x.getMessage());
                System.exit(3);
            }
        }
    }

    private boolean shouldDeleteFile(String fileName){
        if(useExceptions){
            String wordsMappingFilename = WORDS_MAPPING + TERM_MAP_FILE_DEBUG;
            return !fileName.equals(MiscTools.INVALIDATION_FILENAME) &&
                    !fileName.equals(MiscTools.REVIEW_META_DATA_FILENAME) &&
                    !fileName.equals(wordsMappingFilename);
        } else {
            return true;
        }
    }

}
