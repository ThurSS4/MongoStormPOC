package storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;

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

//    @Override
//    public void cleanup() {
//        System.out.println("--> All + " + numbers.size() + " numbers in the list:");
//        for (int nr : numbers) {
//            System.out.println("Numberrrr " + nr);
//        }
//    }
}
