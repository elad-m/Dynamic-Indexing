package dynamic_index.index_experiments;

import dynamic_index.IndexWriter;
import dynamic_index.LogMergeIndexWriter;
import dynamic_index.IndexReader;
import dynamic_index.global_tools.PrintingTool;
import dynamic_index.global_tools.MiscTools;

import java.io.File;

import static dynamic_index.global_tools.MiscTools.*;


public class LogMergeExperiment extends Experiment{


    private final int TEMP_INDEX_SIZE;
    public LogMergeExperiment(String localDir, int inputScale, int tempIndexSize, boolean shouldVerify) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.LOG_MERGE_INDEXES_DIR_NAME,
                inputScale,
                shouldVerify);
        TEMP_INDEX_SIZE = tempIndexSize;
    }

    @Override
    public void runExperiment() {
        initiateExperiment();

        // build from scratch of 40% of the reviews
        IndexWriter logMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory, true);
        queryAfterBuildIndex(indexReader, logMergeIndexWriter);

        // insertions of 240 files
        indexReader = doInsertions(logMergeIndexWriter);

        // removing the index directory and files in it.
        removeIndex();

        // printing the average query time
        resultsWriter.printResults("Log merge results", tlog);

        tlog.close();
    }

    private void initiateExperiment(){
        printDateAndTime();
        createTestLog("Log Merge ");
    }

    private IndexWriter buildIndex(){
        tlog.println("temporary index size: " + TEMP_INDEX_SIZE);

        long startTime = System.currentTimeMillis();
        LogMergeIndexWriter logMergeIndexWriter = new LogMergeIndexWriter(allIndexesDirectory,
                TEMP_INDEX_SIZE);
        logMergeIndexWriter.construct(scalingCases.getInputFilename());
        resultsWriter.addToElapsedConstructionTimeList(startTime);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, LOG_FIRST_BUILD);
        return logMergeIndexWriter;
    }



}
