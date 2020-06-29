package dynamic_index;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;


public class IndexRemover {

//    private final Set<String> afterMergeExceptionFiles = new HashSet<>();
    private boolean useExceptions = false;

    public IndexRemover(){

    }

    void removeFilesAfterMerge(String dir){
        useExceptions = true;
        File file = new File(dir);
        deleteFileOrDirectory(file);
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
            if(useExceptions && !toDelete.getName().equals(Statics.MERGED_INDEX_DIRECTORY)){
                File[] childFiles = toDelete.listFiles();
                if (childFiles != null) {
                    if (childFiles.length == 0) { //Directory is empty. Proceed for deletion
                        singleDelete(toDelete);
                    } else {
                        for (File childFilePath : childFiles) {
                            deleteFileOrDirectory(childFilePath);
                        }
                        // not deleting the indexes directory itself after deleting its contents
                        if(useExceptions && !toDelete.getName().equals(Statics.INDEXES_DIR_NAME)){
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
        if(!singleFileToDelete.getName().equals(Statics.INVALIDATION_VECTOR_FILENAME)){
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


}
