package hdp.demo.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TweetSentimentBolt extends BaseRichBolt {

    private OutputCollector collector;
    private List<String> positiveWords;
    private List<String> negativeWords;

    public TweetSentimentBolt(List<String> positiveWords, List<String> negativeWords) {
        this.positiveWords = positiveWords;
        this.negativeWords = negativeWords;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("subject", "user", "place", "created", "tweet", "sentiment", "weight"));
    }

    @Override
    public void execute(Tuple input) {
        String tweet = (String) input.getValueByField("tweet");
        List<String> tweetWords = Stream.of(tweet.split(" "))
                .distinct()
                .map(s -> s.toLowerCase())
                .collect(Collectors.toList());

        Integer weight = 0;
        String sentiment = new String("neutral");
        if (tweetWords.parallelStream().anyMatch(v -> positiveWords.contains(v))) {
            sentiment = "positive";
            weight = countMatch(tweetWords, positiveWords);
        } else if (tweetWords.parallelStream().anyMatch(v -> negativeWords.contains(v))) {
            sentiment = "negative";
            weight = countMatch(tweetWords, negativeWords);
        }

        emitValues((String) input.getValueByField("subject"),
                (String) input.getValueByField("user"),
                (String) input.getValueByField("place"),
                (Date) input.getValueByField("created"),
                tweet,
                weight,
                sentiment);
    }

    private void emitValues(String subject, String user, String place, Date createdAt, String tweet, Integer weight, String sentiment) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.collector.emit(new Values(
                subject,
                user,
                place,
                simpleDateFormat.format(createdAt),
                tweet,
                sentiment,
                weight)
        );
    }

    public int countMatch(List<String> tweetWords, List<String> influencingWords) {
        Set<String> set = new HashSet<>(tweetWords);
        set.retainAll(influencingWords);
        System.out.println("The number of matches is " + set.size());
        return set.size();
    }
}
