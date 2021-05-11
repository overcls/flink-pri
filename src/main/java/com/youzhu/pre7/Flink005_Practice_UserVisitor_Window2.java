package com.youzhu.pre7;


import com.youzhu.bean.UserBehavior;
import com.youzhu.bean.UserVisitorCount;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class Flink005_Practice_UserVisitor_Window2 {

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

        //使用Bloomfilter方式  自定义触发器:来一条计算一脚(访问redis一次)
        SingleOutputStreamOperator<UserVisitorCount> result = windowedStream
                .trigger(new Mytrigger())
                .process(new UserVisitorWindowfunc());


        //打印
       result.print();
        env.execute();

    }
   public static class Mytrigger extends Trigger<UserBehavior,TimeWindow>{
        /*
        基于不同条件触发触发器   什么时候做计算  什么时候是输出结果  做什么样的计算是后续跟的函数进行的计算
         */
        //元素来了
       @Override
       public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
           return TriggerResult.FIRE_AND_PURGE;
       }

       //处理时间到了
       @Override
       public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
           return TriggerResult.CONTINUE ;
       }

       //事件时间到了
       @Override
       public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
           return TriggerResult.CONTINUE;
       }

       @Override
       public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

       }
   }

   public static class UserVisitorWindowfunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount,String, TimeWindow>{

        //声明redis连接
       private Jedis jedis;

       //声明布隆过滤器
       private MyBloomFilter myBloomFilter;

       //声明每个窗口总人数的key
       private String hourUvCountKey;

       @Override
       public void open(Configuration parameters) throws Exception {
           jedis = new Jedis("ip",6379);

           hourUvCountKey = "HourUv";
           //2的30次方
           myBloomFilter = new MyBloomFilter(1 << 30);
       }

       @Override
       public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {

           //取出数据
           UserBehavior userBehavior = elements.iterator().next();

           //提取窗口信息
           String windowEnd = new TimeStamp(context.window().getEnd()).toString();


           //定义当前窗口的BitMap Key
           String bitMapKey = "BitMap_" + windowEnd;

           //查询当前uid是否已经存在于当前的bitmap中
           long offset = myBloomFilter.getOffset(userBehavior.getUserId().toString());
           Boolean exist = jedis.getbit(bitMapKey, offset);

           //判断数据是否存在决定下一步操作
           if (!exist){
               //将对应offset位置改为1
               jedis.setbit(bitMapKey,offset,true);

               //累加当前窗口的总和
               jedis.hincrBy(hourUvCountKey,windowEnd,1);
           }

           //输出数据
           String hget = jedis.hget(hourUvCountKey, windowEnd);
           out.collect(new UserVisitorCount("UV",windowEnd,Integer.parseInt(hget)));

       }
   }

   //自定义布隆过滤器
    public static class MyBloomFilter{

        //定义布隆过滤器容量,最好传入2的整次幂数据
        private long cap;

       public MyBloomFilter(long cap) {
           this.cap = cap;
       }

       //传入一个字符串,获取在bitmap中的位置信息
       public long getOffset(String value){

           long result = 0L;

           for (char c : value.toCharArray()) {

               result += result * 31 +c ;
           }

           //取模
           //使用位运算 效率更高
           return result & (cap - 1);


       }
   }
}

