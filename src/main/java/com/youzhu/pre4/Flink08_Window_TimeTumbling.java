package com.youzhu.pre4;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

public class Flink08_Window_TimeTumbling {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        //压平为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {

                    out.collect(new Tuple2<>(s1, 1));
                }
            }
        });

        //按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(data -> data.f0);

        //开窗
        /*
         * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes
         * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
         * time windows start at 0:15:00,1:15:00,2:15:00,etc.
         *
         * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
         * such as China which is using UTC+08:00,and you want a time window with size of one day,
         * and window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}
         *
         * if (Math.abs(offset) >= size) {
			throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
		}
		*
		* long start = TimeWindow.getWindowStartWithOffset(now, (globalOffset + staggerOffset) % size, size);
		*
		* 	public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		return timestamp - (timestamp - offset + windowSize) % windowSize;
		*
		return Collections.singletonList(new TimeWindow(start, start + size));
		* offset !!!!!!!!!!!!!!!!!!!!!!!
		* + windowSize 保证为整数  保证数据一定落在对应的窗口  不会有数据丢失的情况
		*
		* 	/**
	 * Gets the largest timestamp that still belongs to this window.
	 *
	 * <p>This timestamp is identical to {@code getEnd() - 1}.
	 *
	 * @return The largest timestamp that still belongs to this window.
	 *
	 * @see #getEnd()
	左闭又开的原因  点 of
         */

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowWindowedStream = tuple2StringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //增量聚合计算
      //  SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowWindowedStream.sum(1);

        /*SingleOutputStreamOperator<Tuple2<String, Integer>> result
                = windowWindowedStream.aggregate(new MyAggFunc());*/

        //既实现了  增量聚合的来一条处理一条 又能获得窗口信息
        SingleOutputStreamOperator<Tuple2<String, Integer>> result1 = windowWindowedStream.aggregate(new MyAggFunc(), new MyWindowFunc());

        //全量窗口apply  在计算平均数  排序 的时候应用   必须收集到所有数据的时候才会用到     全量窗口  的优点  能拿到窗口的相关信息  增量聚合拿不到窗口的相关信息
/*        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowWindowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String s,
                              TimeWindow window,
                              Iterable<Tuple2<String, Integer>> input,
                              Collector<Tuple2<String, Integer>> out) throws Exception {

                //取出迭代器的长度
                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(input.iterator());
                //输出数据
                out.collect(new Tuple2<>(new Timestamp(window.getStart())+":"+s, arrayList.size()));


            }
        });*/

        //process全量窗口函数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowWindowedStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

                //取出迭代器的长度
                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(elements.iterator());
                //输出数据
                out.collect(new Tuple2<>(new Timestamp(context.window().getStart()) + ":" + s, arrayList.size()));
            }
        });

     result.print();
        result1.print();

        env.execute();


    }
   /* public static class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>,Integer,Tuple2<String, Integer>>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator+ 1;
        }

        @Override
        public Tuple2<String, Integer> getResult(Integer accumulator) {
            return new Tuple2<>(,accumulator);
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }*/
   public static class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>,Integer, Integer> {
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
   public static class MyWindowFunc implements  WindowFunction<Integer,Tuple2<String,Integer>,String,TimeWindow>{
       @Override
       public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {

           //取出迭代器中的数据
           Integer next = input.iterator().next();

           out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + s ,next));

       }
   }
}
