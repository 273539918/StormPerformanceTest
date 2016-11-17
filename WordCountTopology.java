package ch.main;

/**
 * Created by chenhong on 16/11/17.
 */
public class WordCountTopology {


    public static class KafkaWordSplitter extends BaseRichBolt{
        // private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
        private static final Logger LOG = LoggerFactory.getLogger(KafkaWordSplitter.class);
        private static final long serialVersionUID = 1L;
        private OutputCollector collector;


        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getString(0);
            LOG.info("RECE[kafka -> splitter] "+line);
            String[] words = line.split("\\s+");
            for(String word : words){
                LOG.info("EMIT[splitter -> counter] "+word);
                collector.emit(input,new Values(word,1));
            }
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
    }

    public static class WordCounter extends BaseRichBolt {
        // private static final Log LOG = LogFactory.getLog(WordCounter.class);
        private static final Logger LOG = LoggerFactory.getLogger(WordCounter.class);
        private static final long serialVersionUID =1L;
        private OutputCollector collector;
        private Map<String,AtomicInteger> counterMap;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
            this.counterMap = new HashMap<String,AtomicInteger>();
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            LOG.info("RECE[splitter -> counter] "+word+" : "+count);
            AtomicInteger ai = this.counterMap.get(word);
            if(ai==null){
                ai= new AtomicInteger();
                this.counterMap.put(word,ai);
            }
            ai.addAndGet(count);
            collector.ack(input);
            LOG.info("CHECK statistics map: "+this.counterMap);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }

        @Override
        public void cleanup() {
            LOG.info("The final result:");
            Iterator<Map.Entry<String,AtomicInteger>> iter = this.counterMap.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry<String,AtomicInteger> entry =iter.next();
                LOG.info(entry.getKey()+"\t:\t"+entry.getValue().get());
            }
        }
    }

    public static void main(String[] args) throws AlreadyAliveException,InvalidTopologyException,InterruptedException{
        String zks =Config.clusterAddress  ; //"10.101.227.20:2181,10.101.226.226:2181,10.101.226.96:2181";
        String topic =Config.kafkaTopic;//"test-topic5";
        String zkRoot =Config.zkRoot;//"/kafka" ;
        String id ="word"; // 读取的status会被存在，/zkRoot/id下面，所以id类似consumer group

        BrokerHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts,topic,zkRoot,id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;
        // spoutConf.zkServers= Arrays.asList(new String[]{"10.101.227.20","10.101.226.226","10.101.226.96"});
        spoutConf.zkServers= Arrays.asList(new String[]{Config.clusterIP});
        spoutConf.zkPort=2181;

        TopologyBuilder  builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); //// Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setBolt("word-splitter",new KafkaWordSplitter(),2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter",new WordCounter() ).fieldsGrouping("word-splitter",new Fields("word"));
        builder.setBolt("persistence-bolt",new PersistenceBolt()).globalGrouping("word-count");


        Config config = new Config();
        String name = MyKafkaTopology.class.getSimpleName();
        if(args !=null && args.length>0 ){
            //config.put(Config.NIMBUS_HOST,args[0]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(name,config,builder.createTopology());
        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name,config,builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

}
