package contadorDePalavras;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ContarLinhaBolt extends BaseBasicBolt{

    Map<String, Integer> contagem;
    Integer id;
    String nome;
    String nomeArquivo;

    @Override
    public void prepare(Map stormConf, TopologyContext context){
        this.contagem = new HashMap<String,Integer>();
        this.nome = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.nomeArquivo = (stormConf.get("diretorioDeResultado").toString() + "saida - " + nome + id + ".txt");

    }
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String linha = input.getString(0);

        if(!contagem.containsKey(linha)){
            contagem.put(linha,1);
        }else{
            Integer contador = contagem.get(linha) + 1;
            contagem.put(linha,contador);
        }
    }
    public void cleanup(){
        try{
            PrintWriter writer = new PrintWriter(nomeArquivo,"UTF-8");
            for(Map)
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("linha"));
    }
}
