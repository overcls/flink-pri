package com.youzhu.pre8;

import com.youzhu.bean.AdCount;
import com.youzhu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink03_Practice_Adcount {


    public static void main(String[] args) throws Exception {
        /*
        广告点击量   这个时候要注意 恶意刷单  关于黑名单
        （如某个用户一天内点击超过100次  这时候应该把该用户 加入黑名单并报警 报警信息输出到侧输出流，对加入黑名单的用户，此后不再对其点击行为进行统计）
         */

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取文本数据创建流 转化为JavaBean，提取时间戳生成watermark
        WatermarkStrategy<AdsClickLog> adsClickLogWatermarkStrategy = WatermarkStrategy.<AdsClickLog>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                    @Override
                    public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<AdsClickLog> adsClickLogDS = env.readTextFile("input/AdClickLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new AdsClickLog(Long.parseLong(split[0]),
                            Long.parseLong(split[1]),
                            split[2],
                            split[3],
                            Long.parseLong(split[4]));
                }).assignTimestampsAndWatermarks(adsClickLogWatermarkStrategy);

        /*
        根据黑名单进行过滤 保留某个人点击某个广告的次数应该保留下来   用状态编程 状态到凌晨清空状态 即使用定时器出发天状态清空 应该选用 process方法
        */
        SingleOutputStreamOperator<AdsClickLog> filterDs = adsClickLogDS.keyBy(data -> data.getUserId()+"_"+data.getAdId())
                .process(new BlackListProcessFunc());


        //按照省份分组
        KeyedStream<AdsClickLog, String> provinceKeyedStream = filterDs.keyBy(AdsClickLog::getProvince);

        //开窗聚合，增量聚合+窗口函数 补充窗口函数
        SingleOutputStreamOperator<AdCount> result = provinceKeyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdCountAggFunc(), new AdCountWindowFunc());

        //打印数据
        result.print();

        //执行任务
        env.execute();


    }
    public static class AdCountAggFunc implements AggregateFunction<AdsClickLog,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(AdsClickLog value, Integer accumulator) {
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

    public static class AdCountWindowFunc implements WindowFunction<Integer, AdCount,String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<AdCount> out) throws Exception {
            Integer count = input.iterator().next();
            out.collect(new AdCount(s,window.getEnd(),count));
        }
    }

}


