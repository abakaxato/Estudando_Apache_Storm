package contadorDePalavras;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class LerLinhaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private FileReader fileReader;
    private BufferedReader reader;
    private boolean complete = false;
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        try{
            this.fileReader = new FileReader(conf.get("diretorioDeLeitura"+".").toString());
        }catch (FileNotFoundException fn){
            throw new RuntimeException("Erro ao ler o arquivo " + conf.get("diretorioDeLeitura" + "."));
        }
        this.reader = new BufferedReader(fileReader);
    }

    @Override
    public void nextTuple() {
        if(!complete){
            try {
                String linha = reader.readLine();
                if(linha != null){
                    linha = linha.trim();
                    linha = linha.toLowerCase();
                    collector.emit(new Values(linha));
                }else{
                    complete = true;
                    fileReader.close();
                }
            }catch (Exception e){
                throw new RuntimeException("Erro ao ler tupla", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("linha"));
    }
}
