package com.youzhu.CDC;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

public class flinkCDC_WithCustomerSchema {


    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        //scan.startup.mode
        //initial 从头开始读(default)
        //latest-offset 从最新位置开始读
        //timestamp 从指定时间戳开始读
        //specific-offset 从指定offset开始读
        //创建Mysql CDC Source
        properties.setProperty("scan.startup.mode","initial");
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("ip")
                .port(3306)
                .username("root")
                .password("xxxxx")
                .databaseList("database")
                .tableList("database.table_name")
                .deserializer(new MySchema() )
                .debeziumProperties(properties)
                .build();

        //读取mysql数据
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);

        //打印
        stringDataStreamSource.print();

        //执行任务

        env.execute();

    }

    public static class MySchema implements DebeziumDeserializationSchema<String>{

        //反序列化方法
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //库名与表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String db = split[1];
            String table = split[2];

            //获取数据

            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject data = new JSONObject();
            //判断delete情况
            if (after !=null){

                Schema schema = after.schema();

                for (Field field : schema.fields()) {
                    data.put(field.name(),after.get(field.name()));
                }
            }

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //创建json存放最终结果
            JSONObject result = new JSONObject();
            result.put("database",db);
            result.put("table",table);
            result.put("type",operation.toString().toLowerCase());
            result.put("data",data);

            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class) ;
        }
    }
}
