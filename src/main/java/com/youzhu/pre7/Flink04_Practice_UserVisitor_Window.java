package com.youzhu.pre7;


import com.youzhu.bean.UserBehavior;
import com.youzhu.bean.UserVisitorCount;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;

public class Flink04_Practice_UserVisitor_Window {

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

        //按照行为分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorSingleOutputStreamOperator.keyBy(x -> x.getBehavior());

        //开窗
        WindowedStream<UserBehavior, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        //使用HashSet方式
        SingleOutputStreamOperator<UserVisitorCount> result = windowedStream.process(new UserVisitorProcessWindowFunc());

        //打印
        result.print();
        env.execute();

    }
    public  static class UserVisitorProcessWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount,String,TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {

            //创建HashSet用于去重
            HashSet<Long> uids = new HashSet<>();

            //取出窗口中所有数据
            Iterator<UserBehavior> iterator = elements.iterator();

            //遍历迭代器,将数据中的uid放入HashSet,去重
            while (iterator.hasNext()){
                uids.add(iterator.next().getUserId());
            }

            //输出数据
            out.collect(new UserVisitorCount("UV",new TimeStamp(context.window().getEnd()).toString(),uids.size()));
        }
    }
}
