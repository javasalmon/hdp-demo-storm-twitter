package hdp.demo.storm.topology;

import hdp.demo.storm.bolt.TweetSentimentBolt;
import hdp.demo.storm.bolt.TweetSpiltterBolt;
import hdp.demo.storm.spout.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * SentimentTopology class that sets up the Storm topology.
 */
public class SentimentTopology {

    /**
     * name of the topology
     */
    static final String TOPOLOGY_NAME = "hdp-demo-storm-twitter";
    /**
     * words for filtering
     */
    static final String[] SUBJECT_FILTERS = new String[]{"tsb"};
    static final List<String> positiveWords = Stream.of("good", "great", "positive", "better", "helpful", "satisfied", "improved").collect(toList());
    static final List<String> negativeWords = Stream.of(
            "outage",
            "poor",
            "shocked",
            "tried",
            "disconnected",
            "failed",
            "hopeless",
            "cannot",
            "nightmare",
            "disputes").collect(toList());

    /**
     * metastore URI for Hive
     */
    static final String META_STORE_URI = "thrift://ip-172-31-47-170.eu-west-1.compute.internal:9083";
    /**
     * Hive database
     */
    static final String DATABASE = "default";
    /**
     * Hive table
     */
    static final String TABLE = "tsb_tweets";

    /**
     * Launch method
     *
     * @param args arguments : <type : cluster/local> <Twitter consumer key>
     *             <Twitter consumer secret> <Twitter access token> <Twitter
     *             access token secret>
     */
    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        String type = args[0];
        String consumerKey = args[1];
        String consumerSecret = args[2];
        String accessToken = args[3];
        String accessTokenSecret = args[4];

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("TwitterSpout", new TwitterSpout(SUBJECT_FILTERS, consumerKey, consumerSecret, accessToken, accessTokenSecret), 1);
        topologyBuilder.setBolt("TweetSpiltterBolt", new TweetSpiltterBolt(SUBJECT_FILTERS), 1).shuffleGrouping("TwitterSpout");
        topologyBuilder.setBolt("TweetSentimentBolt", new TweetSentimentBolt(positiveWords, negativeWords), 3).shuffleGrouping("TweetSpiltterBolt");
        topologyBuilder.setBolt("TweetHiveBolt", getHiveBolt(), 1).allGrouping("TweetSentimentBolt");

        switch (type) {
            case "cluster":
                try {
                    StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                break;
            case "local":
                final LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        cluster.killTopology(TOPOLOGY_NAME);
                        cluster.shutdown();
                    }
                });
                break;
            default:
                System.out.println("Launch type incorrect");
                System.exit(1);
                break;

        }
    }

    private static HiveBolt getHiveBolt() {
        Fields fields = new Fields("subject", "user", "place", "created", "tweet", "sentiment", "weight");

        // define bolt to store data in Hive
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(fields);
        HiveOptions hiveOptions = new HiveOptions(META_STORE_URI, DATABASE, TABLE, mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(10)
                .withIdleTimeout(10);
        return new HiveBolt(hiveOptions);
    }

}
