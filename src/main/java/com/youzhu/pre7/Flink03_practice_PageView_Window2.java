package com.youzhu.pre7;

import com.youzhu.bean.PageViewCount;
import com.youzhu.bean.UserBehavior;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

public class Flink03_practice_PageView_Window2 {
    /*
    pageview开窗 处理数据倾斜   二次聚合
     */
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //转换为JavaBean根据行为过滤数据并提取时间戳生成Watermark
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = stringDataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        //将数据转换为元组
        KeyedStream<Tuple2<String, Integer>, String> KeyedStream = userBehaviorSingleOutputStreamOperator.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("PV_" + new Random().nextInt(8), 1);
            }
        }).keyBy(x -> x.f0);

        //开窗计算
        SingleOutputStreamOperator<PageViewCount> aggregate = KeyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PageViewAggFunc(), new PageViewWindowFunc());

        //按照窗口信息重新分组做第二次聚合
        org.apache.flink.streaming.api.datastream.KeyedStream<PageViewCount, String> pageViewCountStringKeyedStream = aggregate.keyBy(PageViewCount::getTime);

        //累加结果
        SingleOutputStreamOperator<PageViewCount> result = pageViewCountStringKeyedStream.process(new PageViewProcessFunc());

        result.print();

        env.execute();

    }

    public static class PageViewAggFunc implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }
    public static class PageViewWindowFunc implements WindowFunction<Integer, PageViewCount,String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {
            //提取窗口时间
            String timeStamp = new TimeStamp(window.getEnd()).toString();
            //获取累计结果
            Integer next = input.iterator().next();

            out.collect(new PageViewCount("PV",timeStamp,next));
        }
    }

    public static class PageViewProcessFunc extends KeyedProcessFunction<String,PageViewCount,PageViewCount>{

       //定义状态
        private ListState<PageViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("list-state",PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {

            //将数据放入状态
            listState.add(value);

            //注册定时器
            String time = value.getTime();
            long ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();

            //1ms延时
            ctx.timerService().registerEventTimeTimer(ts + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            Iterable<PageViewCount> pageViewCounts = listState.get();

            //遍历累加数据
            Integer count = 0;
            Iterator<PageViewCount> iterator = pageViewCounts.iterator();
            while(iterator.hasNext()){
                PageViewCount next = iterator.next();
                count += next.getCount();
            }

            //输出数据
            out.collect(new PageViewCount("PV",new Timestamp(timestamp -1).toString(),count));

            //清空状态
            listState.clear();
        }
    }
}
