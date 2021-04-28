package com.youzhu.pre1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_batch {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取文件数据
        DataSource<String> input = env.readTextFile("input");

        //压平
        FlatMapOperator<String, String> wordDS = input.flatMap(new MyFlatMapFuction());

        //将单词转换为元组
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
                //return Tuple 2.of(value,1);

            }
        });
        /*
        转变为java lamad表达式要写返回.return(Types.TUPLE.STRING,Types.INT)
         */

        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);

        //聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //打印
        result.print();

    }
    //自定义实现压平操作的类
    public static class MyFlatMapFuction implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            
            //按照空格切割
            String[] words = value.split(" ");

            //遍历word,写出一个个单词
            for (String word : words) {
                out.collect(word);
            }
            
        }
    }
}
