import dynamic_index.index_experiments.LogMergeExperiment;
import dynamic_index.index_experiments.SimpleMergeExperiment;

@SuppressWarnings({"SameParameterValue"})
public class Main {


    public static void main(String[] args) {
        final String localDir = System.getProperty("user.dir");

        System.out.println("======================================= Starting Experiment =======================================");
//        SimpleMergeExperiment simpleMergeExperiment = new SimpleMergeExperiment(localDir, 5);
//        simpleMergeExperiment.runExperiment();

        int[] tempIndexSizes = {1024, 1024, 8192, 32768, 131072, 1048576, 4194304, 4*4194304};
        for(int tempSize: tempIndexSizes){
            LogMergeExperiment logMergeExperiment = new LogMergeExperiment(localDir, 5, tempSize);
            logMergeExperiment.runExperiment();
        }

    }

}

