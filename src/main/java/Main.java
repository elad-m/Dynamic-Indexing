import dynamic_index.runExperiment.LogMergeExperiment;
import dynamic_index.runExperiment.SimpleMergeExperiment;

@SuppressWarnings({"SameParameterValue"})
public class Main {


    public static void main(String[] args) {
        final String localDir = System.getProperty("user.dir");

        System.out.println("======================================= Starting Experiment =======================================");
        SimpleMergeExperiment simpleMergeExperiment = new SimpleMergeExperiment(localDir, 4);
        simpleMergeExperiment.runExperiment();
        LogMergeExperiment logMergeExperiment = new LogMergeExperiment(localDir, 4);
        logMergeExperiment.runExperiment();

    }

}

