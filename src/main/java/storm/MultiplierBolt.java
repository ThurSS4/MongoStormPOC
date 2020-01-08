package storm;

import com.mongodb.client.MongoCollection;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MultiplierBolt extends BaseBasicBolt {
    private ArrayList<Integer> numbers = new ArrayList<Integer>();

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Integer number = tuple.getInteger(0);
        number *= 2;
        numbers.add(number);
        basicOutputCollector.emit(new Values(number));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }

    @Override
    public void cleanup() {
        MongoConnector connector = new MongoConnector();
        MongoCollection<Document> destinationCol = connector.getCollection("stormtest");

        List<Document> multipliedNumbers = new ArrayList<Document>();

        for (int nr : numbers) {
            Document multipliedNumber = new Document("number", nr);
            multipliedNumbers.add(multipliedNumber);
        }

        destinationCol.insertMany(multipliedNumbers);
    }
}
