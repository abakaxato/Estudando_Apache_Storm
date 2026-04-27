import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("brapiSpout", new BrapiSpout());
        builder.setBolt("brapiBolt", new BrapiBolt()).shuffleGrouping("brapiSpout");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try{
            cluster.submitTopology("TopologiaBrapi",conf,builder.createTopology());
            Thread.sleep(50000);
        }
        finally {
            cluster.shutdown();
        }
    }
}
