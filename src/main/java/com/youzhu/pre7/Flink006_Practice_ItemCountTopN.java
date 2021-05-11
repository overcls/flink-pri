package com.youzhu.pre7;


import com.youzhu.bean.ItemCount;
import com.youzhu.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class Flink006_Practice_ItemCountTopN {

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

        //按照商品ID分组
        KeyedStream<Tuple2<Long, Integer>, Long> keyedStream = userBehaviorSingleOutputStreamOperator.map(new MapFunction<UserBehavior, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>(value.getItemId(), 1);
            }
        }).keyBy(x -> x.f0);

        //开窗计算结果
        SingleOutputStreamOperator<ItemCount> aggregate = keyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        //按照窗口信息重新分组,使用状态编程的方式,实现窗口内TopN
        SingleOutputStreamOperator<String> result = aggregate.keyBy(ItemCount::getTime)
                .process(new ItemCountProcessFunc(5));

        //打印
        result.print();
        env.execute();



    }
    public static class ItemCountAggFunc implements AggregateFunction<Tuple2<Long,Integer>,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<Long, Integer> value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }

    public static class ItemCountWindowFunc implements WindowFunction<Integer, ItemCount,Long, TimeWindow>{
        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Integer> input, Collector<ItemCount> out) throws Exception {

            //取出数据
            Integer count = input.iterator().next();

            //输出数据
            out.collect(new ItemCount(aLong ,
                    new TimeStamp(window.getEnd()).toString() , count));

        }
    }
    public static class ItemCountProcessFunc extends KeyedProcessFunction<String,ItemCount,String>{

        //定义状态
        private ListState<ItemCount> listState;

        private Integer topSize;

        public ItemCountProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list-state",ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {

            //将数据存入状态
            listState.add(value);

            //定义定时器
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //注册定时器
            ctx.timerService().registerEventTimeTimer(sdf.parse(value.getTime()).getTime()+1000L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态中的所有数据
            Iterable<ItemCount> itemCounts = listState.get();

            //将迭代器转换为集合
            ArrayList<ItemCount> itemCounts1 = Lists.newArrayList(itemCounts.iterator());

            //按照点击次数排序
            itemCounts1.sort(((o1, o2) -> o2.getCount() - o1.getCount()));

            //输出TopSize条数据
            StringBuilder sb = new StringBuilder();
            sb.append("============")
                    .append(new TimeStamp(timestamp - 1000L))
                    .append("============")
                    .append("\n");
            for (int i = 0; i < Math.min(topSize,itemCounts1.size()); i++) {
                ItemCount itemCount = itemCounts1.get(i);
                sb.append("Top").append(i+1);
                sb.append("ItemId").append(itemCount.getItem());
                sb.append("Count:").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("============")
                    .append(new TimeStamp(timestamp - 1000L))
                    .append("============")
                    .append("\n")
                    .append("\n");

            //清空状态并输出数据
            listState.clear();
            out.collect(sb.toString());

            //线程休息
            Thread.sleep(2000);
        }
    }
}

