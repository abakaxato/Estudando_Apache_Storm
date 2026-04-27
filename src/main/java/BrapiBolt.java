import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class BrapiBolt extends BaseBasicBolt {

    public final void prepare(Map stormConf, TopologyContext context){}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String empresa = input.getValue(0).toString();
        String timestamp = input.getString(1);
        Double valorMedio = (Double) input.getValueByField("valorMedio");
        Double valorOntem = input.getDoubleByField("valorOntem");
        boolean ganho = true;

        if (valorMedio <= valorOntem){
            ganho = false;
            System.out.println("\nvish, ganhasse nada de diferença\n");
        }else{
            System.out.println("\nAe sim, teve um ganho !");
            System.out.println(empresa);
            System.out.println(timestamp);
            System.out.println(valorMedio);
            System.out.println(ganho + "\n");
            collector.emit(new Values(empresa,timestamp,valorMedio,ganho));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("empresa","timestamp","valorMedio","ganho"));
    }
    public  void cleanup(){}
}
