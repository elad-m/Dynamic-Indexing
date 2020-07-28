package dynamic_index.index_experiments;

import dynamic_index.IndexReader;
import dynamic_index.IndexWriter;
import dynamic_index.SimpleMergeIndexWriter;
import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.PrintingTool;

import java.io.File;

import static dynamic_index.global_tools.MiscTools.SIMPLE_FIRST_BUILD;


public class SimpleMergeExperiment extends Experiment {


    public SimpleMergeExperiment(String localDir, int inputScale) {
        super(localDir,
                localDir + File.separatorChar + MiscTools.INDEXES_DIR_NAME,
                inputScale,
                false);
    }

    @Override
    public void runExperiment() {
        initiateExperiment();

        IndexWriter simpleMergeIndexWriter = buildIndex();
        IndexReader indexReader = new IndexReader(allIndexesDirectory);
//        testWordQueriesOnAverage(indexReader,
//                simpleMergeIndexWriter,
//                wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY));
//        wordsRandomizer.getAllWords());

        indexReader = doInsertions(simpleMergeIndexWriter);

        indexReader = mergeIndex(allIndexesDirectory, (SimpleMergeIndexWriter)simpleMergeIndexWriter, indexReader);
        testWordQueriesOnAverage(indexReader,
                simpleMergeIndexWriter,
                wordsRandomizer.getRandomWords(NUMBER_OF_WORDS_TO_QUERY));

        removeIndex();

        resultsWriter.writeResults("Simple merge results", tlog);

        tlog.close();
    }

    public void initiateExperiment() {
        printDateAndTime();
        createTestLog("Simple Merge ");
    }

    private IndexWriter buildIndex() {
        long startTime = System.currentTimeMillis();
        SimpleMergeIndexWriter simpleMergeIndexWriter = new SimpleMergeIndexWriter(allIndexesDirectory, inputScale);
        simpleMergeIndexWriter.construct(scalingCases.getInputFilename());
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
