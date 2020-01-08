package storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class IntegerSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Integer index = 0;
    private boolean isCompleted;

    public void open(Map config, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        if (!isCompleted) {
            while (index < 100) {
                System.out.println("--> DONE at index " + index + " <--");
                this.collector.emit(new Values(index));
                index++;
            }
            isCompleted = true;
        } else {
            this.close();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }
}
