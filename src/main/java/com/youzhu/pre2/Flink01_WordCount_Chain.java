package com.youzhu.pre2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Chain {

    public static void main(String[] args) throws Exception {

        //创建获取执行环境getExecutionEnvironment自身判断是local还是remot
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度  默认是key的hash值%核数
        env.setParallelism(1);
        //全局禁用任务链->不允许合并
        env.disableOperatorChaining();

        /*
        state划分方式
         */

        //读取端口数据   nc.exe -l -p 9999
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        //将每行数据压平并转换为元组
        SingleOutputStreamOperator<String> wordDS = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
            /*
            开启新的任务链
             */
        }).startNewChain();
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
            /*
            单独禁用任务链
             */
        }).slotSharingGroup("group1").disableChaining();

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //聚合   sum是子类自己的方法  这里的返回值类型不能转换为datastream  故scala
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1).slotSharingGroup("group2");

        //打印结果   slot共享组
        /*
        任务链  可理解为stage    slot同一任务不可公用 , 不同任务可以共用
                在内存上slot上不会公用
                cpu上slot是可以共用的
            默认条件下需要slot与最大并行组相同(默认情况是指同一个贡献组) -每个共享组中最大并行度的和
            --------共享组
                -----------会不会继承?
        one-to-one
        并行度设置
            全局级别
            算子级别
        slot共享组
            可规定任务放到一个共享组
            共享组不同也不可合并 即使满足了2个和平条件也不能合并
            共享组不同一定不能共享一个slot
         */
        result.print().slotSharingGroup("group2");

        //启动程序
        env.execute(" ");

    }
}
