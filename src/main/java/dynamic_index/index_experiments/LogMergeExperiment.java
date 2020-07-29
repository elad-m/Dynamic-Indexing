package dynamic_index.index_experiments;

import dynamic_index.IndexRemover;
import dynamic_index.IndexWriter;
import dynamic_index.LogMergeIndexWriter;
import dynamic_index.IndexReader;
import dynamic_index.global_tools.PrintingTool;
import dynamic_index.global_tools.MiscTools;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static dynamic_index.global_tools.MiscTools.*;


public class LogMergeExperiment extends Experiment{


    private final int TEMP_INDEX_SIZE;
    public LogMergeExperiment(String localDir, int inputScale, int tempIndexSize) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.LOG_MERGE_INDEXES_DIR_NAME,
                inputScale,
                true);
        TEMP_INDEX_SIZE = tempIndexSize;
    }

    @Override
    public void runExperiment() {
        initiateExperiment();

        IndexWriter logMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory, true);
        queryAfterBuildIndex(indexReader, logMergeIndexWriter);

        indexReader = doInsertions(logMergeIndexWriter);
//        tlog.println("Query in the end");
//        testWordQueriesOnAverage(indexReader,
//                logMergeIndexWriter,
//                scalingCases.getWordQueries(),
//                true);

        removeIndex();
        resultsWriter.writeResults("Log merge results", tlog);
        tlog.close();
    }

    public void initiateExperiment(){
        printDateAndTime();
        createTestLog("Log Merge ");
    }

    private IndexWriter buildIndex(){
        tlog.println("temporary index size: " + TEMP_INDEX_SIZE);

        long startTime = System.currentTimeMillis();
        LogMergeIndexWriter logMergeIndexWriter = new LogMergeIndexWriter(allIndexesDirectory,
                TEMP_INDEX_SIZE, inputScale);
        logMergeIndexWriter.construct(scalingCases.getInputFilename());
        PrintingTool.printElapsedTimeToLog(tlog, startTime, LOG_FIRST_BUILD);
        return logMergeIndexWriter;
    }

    public static long getAllIndexSize(Path path) {

        final AtomicLong size = new AtomicLong(0);

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {

                    System.out.println("skipped: " + file + " (" + exc + ")");
                    // Skip folders that can't be traversed
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {

                    if (exc != null)
                        System.out.println("had trouble traversing: " + dir + " (" + exc + ")");
                    // Ignore errors traversing a folder
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new AssertionError("walkFileTree will not throw IOException if the FileVisitor does not");
        }

        return size.get();
    }

}
