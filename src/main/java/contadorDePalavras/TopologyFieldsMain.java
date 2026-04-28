package contadorDePalavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyFieldsMain {
    public static void main (String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lerPalavraSpout", new LerPalavraSpout());
        builder.setBolt("contarPalavraBolt", new ContarPalavraBolt(),2).
                fieldsGrouping("lerPalavra", new Fields());

        Config conf = new Config();
        conf.put("diretorioDeLeitura","/home/vboxuser/ExemploEntrada.txt");
        conf.put("diretorioDeResultado", "/home/vboxuser/Storm/Saidas");
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("fieldsGrouping - Topologia Conagem De Palavras",conf,builder.createTopology());
            Thread.sleep(50000);
        }catch (Exception e){
            System.out.println("\ndeu erro ae\n"+ e);
        }finally{
            cluster.shutdown();
        }
    }

}
