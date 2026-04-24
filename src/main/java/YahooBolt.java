import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class YahooBolt extends BaseBasicBolt {

    public final void prepare(Map stormConf, TopologyContext context){}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String empresa = input.getValue(0).toString();
        String timestamp = input.getString(1);
        Double preco = (Double) input.getValueByField("preco");
        Double fechamentoAnterior = input.getDoubleByField("fechamentoPrevio");
        boolean ganho = true;

        if (preco <= fechamentoAnterior){
            ganho = false;
            System.out.println("vish, ganhasse nada de diferença");
        }else{
            System.out.println("Ae sim, teve um ganho !");
            System.out.println(empresa);
            System.out.println(timestamp);
            System.out.println(preco);
            System.out.println(ganho);
            collector.emit(new Values(empresa,timestamp,preco,ganho));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    public  void cleanup(){}
}
