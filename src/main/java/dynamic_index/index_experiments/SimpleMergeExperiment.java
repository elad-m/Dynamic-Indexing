package dynamic_index.index_experiments;

import dynamic_index.IndexReader;
import dynamic_index.IndexRemover;
import dynamic_index.IndexWriter;
import dynamic_index.SimpleMergeIndexWriter;
import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.PrintingTool;

import java.io.File;

import static dynamic_index.global_tools.MiscTools.*;


public class SimpleMergeExperiment extends Experiment {


    public SimpleMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.INDEXES_DIR_NAME,
                inputScale,
                false);
    }

    @Override
    public void runExperiment() {
        printDateAndTime();
        createTestLog("Simple Merge ");

        IndexWriter simpleMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory);
        queryAfterBuildIndex(indexReader, simpleMergeIndexWriter);

        indexReader = doInsertions(simpleMergeIndexWriter);

//        indexReader = mergeIndex(allIndexesDirectory, simpleMergeIndexWriter, indexReader);
//        queryAfterMerge(indexReader);

        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeAllIndexFiles(allIndexesDirectory);

        tlog.close();
    }

    private IndexWriter buildIndex() {
        long startTime = System.currentTimeMillis();
        SimpleMergeIndexWriter simpleMergeIndexWriter = new SimpleMergeIndexWriter(allIndexesDirectory, inputScale);
        simpleMergeIndexWriter.construct(scalingCases.getInputFilename());
        PrintingTool.printElapsedTimeToLog(tlog, startTime, SIMPLE_FIRST_BUILD, NO_AVERAGE_ARGUMENT);
        return simpleMergeIndexWriter;
    }

    private IndexReader mergeIndex(String indexDirectory, SimpleMergeIndexWriter simpleMergeIndexWriter, IndexReader indexReader) {
        long startTime = System.currentTimeMillis();
        System.out.println("=====\n" + "Merging All Indexes " + "\n=====");
        simpleMergeIndexWriter.merge(indexReader);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, "\tIndex Merging", NO_AVERAGE_ARGUMENT);
        return new IndexReader(indexDirectory);
    }
//    private void queryAfterMerge(IndexReader indexReader) {
//        queryAfterBuildIndex(indexReader);

//    }


}
