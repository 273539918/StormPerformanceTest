package performance.test;



/**
 * WordCount but teh spout does not stop, and the bolts are implemented in java.
 * This can show how fast the word count can run.
 */
public class FastWordCountTopology {
    private static Logger LOG                             = LoggerFactory.getLogger(FastWordCountTopology.class);
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology.spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "topology.bolt.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "topology.bolt.parallel";

    public static class FastRandomSentenceSpout implements IRichSpout {
        SpoutOutputCollector _collector;
        Random _rand;
        long                 sendingCount;
        long                 startTime;
        boolean              isStatEnable;
        int                  sendNumPerNexttuple;

        private static final String[] CHOICES = { "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers",
                "Storm is a distributed and fault-tolerant realtime computation system.",
                "Inspired by Apache Storm, Storm has been completely rewritten in Java and provides many more enhanced features.",
                "Storm has been widely used in many enterprise environments and proved robust and stable.",
                "Storm provides a distributed programming framework very similar to Hadoop MapReduce.",
                "The developer only needs to compose his/her own pipe-lined computation logic by implementing the JStorm API",
                " which is fully compatible with Apache Storm API",
                "and submit the composed Topology to a working Storm instance.",
                "Similar to Hadoop MapReduce, Storm computes on a DAG (directed acyclic graph)." ,
                "Different from Hadoop MapReduce, a Storm topology runs 24 * 7",
                "the very nature of its continuity abd 100% in-memory architecture ",
                "has been proved a particularly suitable solution for streaming data and real-time computation.",
                "Storm guarantees fault-tolerance.",
                "Whenever a worker process crashes, ",
                "the scheduler embedded in the Storm instance immediately spawns a new worker process to take the place of the failed one.",
                " The Acking framework provided by storm guarantees that every single piece of data will be processed at least once." };

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
            sendingCount = 0;
            startTime = System.currentTimeMillis();
            // sendNumPerNexttuple = Utils.getInt(conf.get("send.num.each.time"), 1);
            sendNumPerNexttuple=1;
            // isStatEnable = Utils.getBoolean(conf.get("is.stat.enable"), true);
            isStatEnable=true;
        }

        public void nextTuple() {
            int n = sendNumPerNexttuple;
            while (--n >= 0) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                _collector.emit(new Values(sentence));
            }
            updateSendTps();
        }


        public void ack(Object id) {
            // Ignored
        }


        public void fail(Object id) {
            _collector.emit(new Values(id), id);
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

        private void updateSendTps() {
            if (!isStatEnable)
                return;

            sendingCount++;
            long now = System.currentTimeMillis(); //
            long interval = now - startTime;

            if (interval > 1000) {
                //LOG.debug("interval = "+interval);
                // 多少条tuple/s
                LOG.info("Sending tps of last one second is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
                startTime = now;
                sendingCount = 0;
            }
        }


        public void close() {
            // TODO Auto-generated method stub

        }


        public void activate() {
            // TODO Auto-generated method stub

        }


        public void deactivate() {
            // TODO Auto-generated method stub

        }


        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    public static class SplitSentence implements IRichBolt {
        OutputCollector collector;


        public void execute(Tuple tuple) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                //collector.emit(new Values(word));
                collector.emit(tuple,new Values(word));

            }
            collector.ack(tuple);
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }


        public void cleanup() {
            // TODO Auto-generated method stub

        }


        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    public static class WordCount implements IRichBolt {
        OutputCollector      collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();


        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            collector.ack(tuple);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }


        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;

        }


        public void cleanup() {
            // TODO Auto-generated method stub

        }


        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    static boolean isLocal = true;
    static Config conf    = null;

    public static void test() throws Exception{

        //int spout_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int spout_Parallelism_hint=1;
        //int split_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint=4;
        // int count_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 2);
        int count_Parallelism_hint=4;

        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("spout", new FastRandomSentenceSpout(), spout_Parallelism_hint);
        builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));


        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];

        LocalCluster cluster = new LocalCluster();
        conf = new Config();
        //conf.setNumAckers(0);
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        //Utils.waitForSeconds(5);
//         Thread.sleep(60 * 1000);
//         cluster.killTopology(topologyName);
//         cluster.shutdown();

        //StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {

        // conf = utils.Utils.getConfig(args);
        test();
    }
}
