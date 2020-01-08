package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setBolt("MultiplierBolt", new MultiplierBolt()).shuffleGrouping("IntegerSpout");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = null;
        try {
            cluster = new LocalCluster();
            cluster.submitTopology("HelloTopology", config, builder.createTopology());
            Thread.sleep(10000); // For localMode only
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }
}
