import dynamic_index.index_experiments.LogMergeExperiment;
import dynamic_index.index_experiments.SimpleMergeExperiment;

@SuppressWarnings({"SameParameterValue"})
public class Main {


    public static void main(String[] args) {
        final String localDir = System.getProperty("user.dir");

        System.out.println("======================================= Starting Experiment =======================================");

        SimpleMergeExperiment simpleMergeExperiment = new SimpleMergeExperiment(localDir, 4);
        simpleMergeExperiment.runExperiment();
        LogMergeExperiment logMergeExperiment = new LogMergeExperiment(localDir, 4, 131072);
        logMergeExperiment.runExperiment();

        //TODO: continues changing hyper-parameters to get meaningful results.
        // look at notepad++ last results for inspiration.

        ///testing temporary index size
//        int[] tempIndexSizes = {1024, 1024, 8192, 32768, 131072, 1048576, 4194304, 4*4194304};
//        int[] tempIndexSizes = {1048576};
//        for(int tempSize: tempIndexSizes){
//            logMergeExperiment = new LogMergeExperiment(localDir, 5, tempSize);
//            logMergeExperiment.runExperiment();
//        }

    }

}

