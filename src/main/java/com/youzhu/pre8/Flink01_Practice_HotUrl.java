package com.youzhu.pre8;

import com.youzhu.bean.ApacheLog;
import com.youzhu.bean.UrlCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class Flink01_Practice_HotUrl {

    public static void main(String[] args) {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文本数据创建流并转换为JavaBean对象提取时间戳生成WaterMark
        WatermarkStrategy<ApacheLog> apacheLogWatermarkStrategy = WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.readTextFile("input/apache.log").map(data -> {
            String[] split = data.split(" ");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            return new ApacheLog(split[0],
                    split[1],
                    sdf.parse(split[3]).getTime(),
                    split[5],
                    split[6]);
        }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(apacheLogWatermarkStrategy);

        //转换为元组类型,(url,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> urlToOneDS = apacheLogDS.map(new MapFunction<ApacheLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(ApacheLog value) throws Exception {
                return new Tuple2<>(value.getUrl(), 1);
            }
        });

        //按照url分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = urlToOneDS.keyBy(data -> data.f0);

        //开窗聚合,增量+窗口函数(补充窗口信息)
        SingleOutputStreamOperator<UrlCount> urlCountByWindowDS = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new HotUrlAggFunc(), new HotUrlWindowFunc());

        //按照窗口信息重新分组

        //使用状态编程的方式+定时器的方式实现同一个窗口中所有数据的排序输出

        //打印数据

        //执行任务



    }

    public static class HotUrlAggFunc implements AggregateFunction<Tuple2<String, Integer>,Integer,Integer>{
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
            return a+b;
        }
    }

    public static class HotUrlWindowFunc implements WindowFunction<Integer, UrlCount,String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<UrlCount> out) throws Exception {
            Integer count = input.iterator().next();
            out.collect(new UrlCount(s,window.getEnd(),count));
        }
    }
}
