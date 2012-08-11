package com.sjoerdmulder.trident.mongodb;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;

public class MongoStateTest {

    public static StormTopology buildTopology(LocalDRPC drpc, StateFactory stateFactory) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.build();
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .parallelismHint(16)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(stateFactory, new Fields("word"), new WordCount(), new Fields("count"))
                        .parallelismHint(16);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new SumWord(), new Fields("sum"))
        ;

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
//        StateFactory stateFactory = MongoState.nonTransactional("mongodb://127.0.0.1/test.words", Word.class);
//        StateFactory stateFactory = MongoState.opaque("mongodb://127.0.0.1/test.words", Word.class);
        StateFactory stateFactory = MongoState.transactional("mongodb://127.0.0.1/test.words", Word.class);


        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc, stateFactory));
            for (int i = 0; i < 100; i++) {
                long startDate = System.nanoTime();
                String result = drpc.execute("words", "cat the dog jumped");
                long endDate = System.nanoTime() - startDate;
                System.out.println("DRPC RESULT: " + result + " took: " + endDate / 1000000);
                Thread.sleep(100);
            }
            cluster.shutdown();
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null, stateFactory));
        }
    }

    private static class WordCount implements CombinerAggregator<Word> {
        @Override
        public Word init(TridentTuple tuple) {
            return new Word(tuple.getString(0), 1L);
        }

        @Override
        public Word combine(Word word1, Word word2) {
            return new Word(word1, word2);
        }

        @Override
        public Word zero() {
            return new Word(null, 0L);
        }
    }

    public static class SumWord extends Sum {
        @Override
        public Number init(TridentTuple tuple) {
            return ((Word) tuple.getValue(0)).count;
        }
    }

}
