package com.youzhu.pre4;

import com.youzhu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_Demo_PV_WordCount {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //转换为JavaBean并过滤PV的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = stringDataStreamSource.
                flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        //分隔
                        String[] split = value.split(",");
                        //封装Bean
                        UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4]));
                        //选择需要输出的数据
                        if ("pv".equals(userBehavior.getBehavior())) {
                            out.collect(new Tuple2<>("PV", 1));
                        }
                    }
                });


        //指定KEY分组
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = pv.keyBy(data -> data.f0);


        //计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);


        //数据倾斜严重   解决方案 -》 二次聚合
        //打印
        sum.print();

        //执行任务
        env.execute();
    }

}
