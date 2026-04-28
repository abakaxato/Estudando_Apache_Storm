package contadorDePalavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class AllGroupingMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lerPalavraSpout", new LerLinhaSpout());
        builder.setBolt("contarPalavraBolt", new ContarLinhaBolt(), 2).
                allGrouping("lerPalavra");

        Config conf = new Config();
        conf.put("diretorioDeLeitura", "/home/vboxuser/ExemploEntrada.txt");
        conf.put("diretorioDeResultado", "/home/vboxuser/Storm/Saidas");
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("allGrouping - Topologia Conagem De Palavras", conf, builder.createTopology());
            Thread.sleep(50000);
        } catch (Exception e) {
            System.out.println("\ndeu erro ae\n" + e);
        } finally {
            cluster.shutdown();
        }
    }
}
