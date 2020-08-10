package dynamic_index.index_experiments;

import dynamic_index.IndexReader;
import dynamic_index.IndexWriter;
import dynamic_index.SimpleMergeIndexWriter;
import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.PrintingTool;

import java.io.File;

import static dynamic_index.global_tools.MiscTools.SIMPLE_FIRST_BUILD;


public class SimpleMergeExperiment extends Experiment {


    public SimpleMergeExperiment(String localDir, int inputScale, boolean shouldVerify) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.INDEXES_DIR_NAME,
                inputScale,
                shouldVerify);
    }

    @Override
    public void runExperiment() {
        initiateExperiment();

        // build from scratch of 40% of the reviews
        IndexWriter simpleMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory);
        queryAfterBuildIndex(indexReader, simpleMergeIndexWriter);

        // insertions of 240 files
        indexReader = doInsertions(simpleMergeIndexWriter);

        // testing average query time before merge (i.e. having 240 auxiliary indexes + main index)
        tlog.println("testing average query time before merge: ");
        testWordQueriesOnAverage(indexReader,
                simpleMergeIndexWriter,
                wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY)
        );

        // merging and testing average time again
        tlog.println("testing average query time after merge: ");
        indexReader = mergeIndex(allIndexesDirectory, (SimpleMergeIndexWriter)simpleMergeIndexWriter, indexReader);
        testWordQueriesOnAverage(indexReader,
                simpleMergeIndexWriter,
                wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY)
        );

        // removing the index directory and files in it.
        removeIndex();

        // printing the average query time
        resultsWriter.printResults("Simple merge results", tlog);

        tlog.close();
    }

    private void initiateExperiment() {
        printDateAndTime();
        createTestLog("Simple Merge ");
    }

    private IndexWriter buildIndex() {
        long startTime = System.currentTimeMillis();
        SimpleMergeIndexWriter simpleMergeIndexWriter = new SimpleMergeIndexWriter(allIndexesDirectory);
        simpleMergeIndexWriter.construct(scalingCases.getInputFilename());
        resultsWriter.addToElapsedConstructionTimeList(startTime);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, SIMPLE_FIRST_BUILD);
        return simpleMergeIndexWriter;
    }

    private IndexReader mergeIndex(String indexDirectory, SimpleMergeIndexWriter simpleMergeIndexWriter, IndexReader indexReader) {
        long startTime = System.currentTimeMillis();
        System.out.println("=====\n" + "Merging All Indexes " + "\n=====");
        simpleMergeIndexWriter.merge(indexReader);
        PrintingTool.printElapsedTimeToLog(tlog, startTime, "\tIndex Merging");
        return new IndexReader(indexDirectory);
    }

}
