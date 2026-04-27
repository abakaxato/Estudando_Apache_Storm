package contadorDePalavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class ShuffleGroupingMain {
    public static void main (String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lerPalavraSpout", new LerPalavraSpout());
        builder.setBolt("contarPalavraBolt", new ContarPalavraBolt(),2).
                shuffleGrouping("lerPalavra");

        Config conf = new Config();
        conf.put("diretorioDeLeitura","/home/vboxuser/ExemploEntrada.txt");
        conf.put("diretorioDeResultado","/home/vboxuser/ExemploSaida.txt");
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("shuffleGrouping - Topologia Conagem De Palavras",conf,builder.createTopology());
            Thread.sleep(10000);
        }catch (Exception e){
            System.out.println("\ndeu erro ae\n"+ e);
        }finally{
            cluster.shutdown();
        }
    }
}
