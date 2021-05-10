package com.youzhu.CDC;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class flinkCDC {

    /*
    local模式是不支持 从checkpoint savepoint 恢复任务  必须在集群上提交
     */
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

       //CK
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs地址"));

        //重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2000L));

        //设置任务关闭的时候保留最后一次CK数据  默认是delet
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //访问hdfs
        System.setProperty("HADOOP_USER_NAME","ywt");

        //创建mysqlcdcsource

        //开启binlog vim /etc/my.conf
        Properties properties = new Properties();
        //scan.startup.mode
        //initial 从头开始读(default)
        //latest-offset 从最新位置开始读
        //timestamp 从指定时间戳开始读
        //specific-offset 从指定offset开始读
        properties.setProperty("scan.startup.mode","initial");

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("ip")
                .port(3306)
                .username("root")
                .password("xxxxx")
                .databaseList("database")
                .tableList("database.table_name")
                .deserializer(new StringDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();

        //读取mysql数据
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);

        //打印
        stringDataStreamSource.print();

        //执行任务

        env.execute();

    }
}
