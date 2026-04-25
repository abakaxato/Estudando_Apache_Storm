import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class BrapiSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    private String token;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
        try{
            InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties");{
                Properties prop = new Properties();
                prop.load(input);
                this.token = prop.getProperty("brapi.token");
            }
        }catch (IOException ex){
            ex.printStackTrace();
        }
    }

    public void nextTuple(){
        try{
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create("https://brapi.dev/api/quote/PETR4"))
                    .header("Authorization","Bearer "+this.token).build();
            HttpResponse<String> response = client.send(request,HttpResponse.BodyHandlers.ofString());

            if(response.statusCode() == 200){
                JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
                JsonObject stock = json.getAsJsonArray("results").get(0).getAsJsonObject();

                BigDecimal valorMedio = stock.get("regularMarketPrice").getAsBigDecimal();
                BigDecimal valorOntem = stock.get("regularMarketPreviousClose").getAsBigDecimal();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                collector.emit(new Values("PETR4",sdf.format(timestamp),valorMedio.doubleValue(),
                        valorOntem.doubleValue()));

                Thread.sleep(10000);
                }
        }catch (Exception e){
            System.out.println("Deu Erro Paizao"+ e.getMessage());
            try{
                Thread.sleep(30000);
            }catch (InterruptedException ie){
                Thread.currentThread().interrupt();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("empresa","timestamp","valorMedio","valorOntem"));
    }
}