package dynamic_index;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/**
 * Deletes an index directory (actually any directory, recursively)
 */
public class IndexRemover {

    /**
     * Delete all index files by removing the given directory
     */
    public void removeIndex(String dir) {
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
            File[] childFiles = toDelete.listFiles();
            if (childFiles != null) {
                if (childFiles.length == 0) { //Directory is empty. Proceed for deletion
                    singleDelete(toDelete);
                } else {
                    for (File childFilePath : childFiles) {
                        deleteFileOrDirectory(childFilePath);
                    }
                    deleteDirectory(toDelete); // calling again, now should be empty
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private void singleDelete(File singleFileToDelete) {
        Path dirPathToDelete = singleFileToDelete.toPath();
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
