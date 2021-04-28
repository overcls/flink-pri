package com.youzhu.pre4;

import com.youzhu.bean.MarketingUserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class Flink06_Demo_MarketByChannel {

    //获取执行环境
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从自定义Source加载数据
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDataStreamSource = env.addSource(new AppMarketingDataSource());

        //按照渠道以及行为分组
        KeyedStream<MarketingUserBehavior, Tuple2<String, String>> marketingUserBehaviorTuple2KeyedStream = marketingUserBehaviorDataStreamSource.keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                return new Tuple2<>(value.getChannel(), value.getBehavior());
            }
        });

        //计算总和
        SingleOutputStreamOperator<Tuple2<Tuple2<String, String>, Integer>> result = marketingUserBehaviorTuple2KeyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, MarketingUserBehavior, Tuple2<Tuple2<String, String>, Integer>>() {

            private HashMap<String, Integer> hashMap = new HashMap<>();

            @Override
            public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<Tuple2<String, String>, Integer>> out) throws Exception {
                //拼接HashKey
                String hashKey = value.getChannel() + "-" + value.getBehavior();

                //取出HashMap中的数据,如果该数据是第一次过来,则给定默认值为0
                Integer count = hashMap.getOrDefault(hashKey, 0);

                count++;

                //输出结果
                out.collect(new Tuple2<>(ctx.getCurrentKey(), count));

                //更新hashMap中的数据
                hashMap.put(hashKey, count);


            }
        });

        result.print();

        env.execute();
    }


    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}