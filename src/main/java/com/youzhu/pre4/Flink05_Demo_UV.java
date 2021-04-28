package com.youzhu.pre4;

import com.youzhu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink05_Demo_UV {

    public static void main(String[] args) throws Exception {



        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //转换为JavaBean并过滤PV的数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = stringDataStreamSource.
                flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
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
                            out.collect(userBehavior);
                        }
                    }
                });

        //指定key分组   不能加随机数 可能进入不同并行度进行分别去重  就会出现问题
        KeyedStream<UserBehavior, String> userBehaviorStringKeyedStream = userBehaviorSingleOutputStreamOperator.keyBy(data -> "UV");

        //使用Process方式计算总和（注意UserId的去重）
        SingleOutputStreamOperator<Integer> process = userBehaviorStringKeyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {

            private HashSet<Long> uids = new HashSet<>();
            private Integer count = 0;


            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                if (!uids.contains(value.getUserId())) {
                    uids.add(value.getUserId());
                    count++;
                    out.collect(count);
                }
            }
        });

        process.print();

        env.execute();


    }
}
