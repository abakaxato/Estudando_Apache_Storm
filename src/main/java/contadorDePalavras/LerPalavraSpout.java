package contadorDePalavras;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

import java.util.Map;

public class LerPalavraSpout extends BaseRichSpout {
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
