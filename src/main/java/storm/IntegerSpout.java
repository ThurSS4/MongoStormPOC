package storm;

import com.mongodb.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Map;

public class IntegerSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private ArrayList<Integer> numbers = new ArrayList<Integer>();
    private boolean isCompleted;

    public void open(Map config, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        if (!isCompleted) {
            getDataFromMongoDB();

            for (int nr : numbers) {
                this.collector.emit(new Values(nr));
                System.out.println("Emitted number " + nr);
            }
            isCompleted = true;
        } else {
            this.close();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }

    private void getDataFromMongoDB() {
        MongoConnector connector = new MongoConnector();
        MongoCollection<Document> sourceCol = connector.getCollection("sparktest");

        FindIterable<Document> documents = sourceCol.find();
        for (Document doc : documents) {
            String number = doc.get("number").toString();
            int nr = Integer.parseInt(number);
            numbers.add(nr);
        }

        System.out.println("The numbers array now contains is: " + numbers.size() + " numbers.");
    }
}
