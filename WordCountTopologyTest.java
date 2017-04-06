package DStorm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.sun.javafx.tools.packager.Log;

import java.util.*;

/**
 * Created by chenhong on 17/4/3.
 */

class SentenceSpout extends BaseRichSpout{


    private SpoutOutputCollector collector;
    private String[] sentences={ "marry had a little lamb whos fleese was white as snow",
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
    private int index=0;
    private long sendingCount=0;
    private long startTime=0;

    public void declareOutputFields(OutputFieldsDeclarer declarer){

        declarer.declare(new Fields("sentence","timestamp"));

    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector){

        this.collector=collector;
        this.startTime=System.currentTimeMillis();
    }

    public void nextTuple(){

        if(index < sentences.length){
            this.collector.emit(new Values(sentences[index],System.currentTimeMillis()));
            index++;
        }
        else{
            index=0; //如果不让index归0，sentences只会发送一次
        }



    }
}

class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector collector;
    public void prepare(Map config ,TopologyContext context,OutputCollector collector){
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word","timestamp"));
    }


    public void execute(Tuple tuple){
        String sentence = tuple.getStringByField("sentence");
        long timestamp = tuple.getLongByField("timestamp");
        String[] words = sentence.split(" ");
        for(String word : words){
            this.collector.emit(new Values(word,timestamp));
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class WordCountBolt extends BaseRichBolt{
    private OutputCollector collector;

    private HashMap<String,Long> counts = null;

    public void prepare(Map config , TopologyContext context,OutputCollector collector){
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }



    public void execute(Tuple tuple){
        String word = tuple.getStringByField("word");
        long timestamp = tuple.getLongByField("timestamp");
        Long count = this.counts.get(word);
        if(count ==null){
            count =0L;
        }
        count++;
        this.counts.put(word,count);
        this.collector.emit(new Values(word,count,timestamp));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count","timestamp"));
    }
}

class ReportBolt extends BaseRichBolt {
    private HashMap<String,Long> counts =null;

    private static  long sendingCount =0;
    private static  long sumTime =0;

    public void prepare(Map config, TopologyContext context, OutputCollector collector){
        this.counts = new HashMap<String, Long>();


    }

    public void execute(Tuple tuple){
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        Long timestamp = tuple.getLongByField("timestamp");

        sendingCount++;

        long interval = System.currentTimeMillis()-timestamp;
        System.out.println("the complete latency is:"+ interval);
//        sumTime =sumTime +interval;
//        if (sendingCount > 100) {
//            System.out.println("the complete latency is : " + (sumTime/sendingCount)  +" sumTime="+sumTime+" sendingCount="+sendingCount);
//
//            sendingCount = 0;
//            sumTime=0;
//        }


        this.counts.put(word,count);

    }

    /*
     该bolt位于末端,所以declareOutputFields为空
    **/
    public void declareOutputFields(OutputFieldsDeclarer declarer){

    }


    /*
     cleanup方法用来释放bolt占用的资源
     */
    public void cleanup(){
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for(String key: keys){
            //System.out.println(key+" : "+this.counts.get(key));
        }
    }
}


public class WordCountTopologyDemo {

    private static final String SENTENCE_SPOUT_ID="sentence-spout";
    private static final String SPILL_BOLT_ID ="split-bolt";
    private static final String COUNT_BOLT_ID ="count-bolt";
    private static final String REPORT_BOLT_ID="report-bolt";
    private static final String TOPOLOGY_NAME="word-count-topology";
    //int spout_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
    private static final int spout_Parallelism_hint=1;
    //int split_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
    private static final int split_Parallelism_hint=2;
    // int count_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 2);
    private static final int count_Parallelism_hint=4;

    private static final int reportBolt_Parallelism_hint=1;

    public static void main(String[] args){

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();

        //注册一个sentence spout并且赋值给其唯一的ID
        builder.setSpout(SENTENCE_SPOUT_ID, spout,spout_Parallelism_hint);
        //注册一个splitsentencebolt ，这个bolt订阅sentencespout发射出来的数据流,shuffleGrouping方法告诉
        //storm要将类sentenceSpout发射的tuple随机均匀地分发给SplitSentenceBolt实例
        builder.setBolt(SPILL_BOLT_ID,splitBolt,split_Parallelism_hint).shuffleGrouping(SENTENCE_SPOUT_ID);
        //fieldsGrouping()方法来保证所有 word字段值相同的tuple会被路由到同一个wordcountbolt实例中
        builder.setBolt(COUNT_BOLT_ID,countBolt,count_Parallelism_hint).fieldsGrouping(SPILL_BOLT_ID, new Fields("word"));
        //globalGrouping方法将WordCountBolt发射的所有tuple路由到唯一的ReportBolt任务中
        builder.setBolt(REPORT_BOLT_ID,reportBolt,reportBolt_Parallelism_hint).globalGrouping(COUNT_BOLT_ID);

        //config对象代表了对topology所有组件全局生效的配置参数集合，会分发给各个spout和bolt的open(),prepare()方法
        Config config = new Config();
        //LocalCluster类在本地开发环境来模拟一个完整的storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        try {
            Thread.sleep(1000*60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
