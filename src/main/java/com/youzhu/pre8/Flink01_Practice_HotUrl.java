package com.youzhu.pre8;

import com.google.common.collect.Lists;
import com.youzhu.bean.ApacheLog;
import com.youzhu.bean.UrlCount;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

public class Flink01_Practice_HotUrl {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文本数据创建流并转换为JavaBean对象提取时间戳生成WaterMark
        WatermarkStrategy<ApacheLog> apacheLogWatermarkStrategy = WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
        //SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.readTextFile("input/apache.log")
        //从端口获取数据
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.socketTextStream("pre1",9999)
                .map(data -> {
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
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutput"){
                })
                .aggregate(new HotUrlAggFunc(), new HotUrlWindowFunc());

        //按照窗口信息重新分组
        KeyedStream<UrlCount, Long> windowKeyedStream = urlCountByWindowDS.keyBy(UrlCount::getWindowEnd);

        //使用状态编程的方式(rich&&process可进行状态编程)+定时器(process可以用process)的方式实现同一个窗口中所有数据的排序输出
        SingleOutputStreamOperator<String> result = windowKeyedStream.process(new HotUrlProcessFunc(5));


        //打印数据
        apacheLogDS.print("apacheLogDS");
        urlCountByWindowDS.print("urlCountByWindowDS");
        result.print("Result");

        //执行任务
        env.execute();



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

    public static class  HotUrlProcessFunc extends KeyedProcessFunction<Long,UrlCount,String>{

        private Integer topSize;

        public HotUrlProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        //声明状态
        private ListState<UrlCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlCount>("list-state",UrlCount.class));

        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {

            //将当前数据放入状态
            listState.add(value);

            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);

            //注册定时器 ,在窗口真正关闭之后,专门用于清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+61001L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //ctx.getCurrentKey()就是获取当前定时器 使用这个定时器是为了解决 迟到数据到来时 清空了状态 没有之前的状态 定时器这样做会解决无状态的问题
            //出现的新问题 watermark更新时
            if (timestamp == ctx.getCurrentKey()+61001L){
               listState.clear();
               return;
            }

            //提取状态当中的数据
            Iterator<UrlCount> urlCountIterator = listState.get().iterator();

            ArrayList<UrlCount> urlCounts = Lists.newArrayList(urlCountIterator);

            //排序
            urlCounts.sort(((o1, o2) -> o2.getCount()-o1.getCount()));

            //取TopN
            StringBuilder sb = new StringBuilder();
            sb.append("==================")
                    .append(new TimeStamp(timestamp-1000L))
                    .append("==================")
                    .append("\n");
            for (int i = 0; i < Math.min(topSize, urlCounts.size()); i++) {
                UrlCount urlCount = urlCounts.get(i);

                sb.append("Top").append(i+1);
                sb.append("Url:").append(urlCount.getUrl());
                sb.append("Count").append(urlCount.getCount());
                sb.append("\n");
            }

            sb.append("==================")
                    .append(new TimeStamp(timestamp-1L))
                    .append("==================")
                    .append("\n")
                    .append("\n");

            //输出数据
            //listState.clear();
            out.collect(sb.toString());

            //线程睡眠
            Thread.sleep(2000);

        }
    }
}
