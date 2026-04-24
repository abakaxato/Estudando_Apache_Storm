import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

public class YahooSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){ this.collector = collector;}

    public void nextTuple(){
        try{
            StockQuote quote = YahooFinance.get("GOOG").getQuote();
            BigDecimal preco = quote.getPrice();
            BigDecimal fechamentoPrevio = quote.getPreviousClose();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            collector.emit(new Values("GOOG",sdf.format(timestamp),preco.doubleValue(),fechamentoPrevio.doubleValue()));
            Thread.sleep(1000);
        }catch (Exception e){
            System.out.println("Deu Erro Paizao");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("empresa","timestamp","preco","fechamentoPrevio"));
    }
}