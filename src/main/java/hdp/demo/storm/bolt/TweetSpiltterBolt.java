package hdp.demo.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class TweetSpiltterBolt extends BaseRichBolt {

    /**
     * collector
     */
    private OutputCollector collector;
    /**
     * words for filtering
     */
    private String[] subjects;

    public TweetSpiltterBolt(String[] subjects) {
        this.subjects = subjects;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String tweetText = tweet.getText();
        String user = Optional.ofNullable(tweet).map(t -> t.getUser()).map(u -> u.getName()).orElse("");
        String place = Optional.ofNullable(tweet).map(t -> t.getPlace()).map(p -> p.getFullName()).orElse("");
        Date createdAt = Optional.ofNullable(tweet).map(t -> t.getCreatedAt()).orElse(Calendar.getInstance().getTime());

        for (String subject : this.subjects) {
            if (tweetText.toLowerCase().contains(subject)) {
                int length = tweetText.length() - 1;
                if (length > 100) {
                    length = 100;
                }
                System.out.println("Received tweet with [" + subject + "] : " + tweetText.substring(0, length));
                this.collector.emit(new Values(subject, user, place, tweetText, createdAt));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subject", "user", "place", "tweet", "created"));
    }
}
