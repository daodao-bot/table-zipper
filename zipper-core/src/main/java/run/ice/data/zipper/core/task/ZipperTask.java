package run.ice.data.zipper.core.task;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import run.ice.data.zipper.core.process.InitProcessFunction;
import run.ice.data.zipper.core.process.SplitProcessFunction;
import run.ice.data.zipper.core.sink.DdlMapFunction;
import run.ice.data.zipper.core.sink.DdlSinkFunction;
import run.ice.data.zipper.core.sink.DmlMapFunction;
import run.ice.data.zipper.core.sink.DmlSinkFunction;
import run.ice.data.zipper.core.util.FlinkUtil;

/**
 * Zipper 任务
 *
 * @author DaoDao
 */
@Slf4j
public class ZipperTask {

    public static void run(Class<?> clazz, String[] args) throws Exception {

        String name = clazz.getSimpleName();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkUtil.env(clazz, env, args);
        ParameterTool parameter = FlinkUtil.parameter(clazz, args);

        MySqlSource<String> mySqlSource = FlinkUtil.mysqlSource(parameter);

        /*
         * 转为 string 格式的 DataStream
         */
        DataStream<String> mySqlStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource")
                .name(name + ":" + "mySqlStream")
                .uid(name + ":" + "mySqlStream")
                .setParallelism(1)
                .startNewChain();

        if (FlinkUtil.localEnv(parameter)) {
            DataStreamSink<String> mySqlStreamPrint = mySqlStream.addSink(new PrintSinkFunction<>())
                    .name(name + ":" + "mySqlStreamPrint")
                    .uid(name + ":" + "mySqlStreamPrint")
                    .disableChaining();
        }

        DataStream<String> mySqlStreamInit = mySqlStream.process(new InitProcessFunction())
                .name(name + ":" + "mySqlStreamInit")
                .uid(name + ":" + "mySqlStreamInit")
                .setParallelism(1)
                .startNewChain();

        SingleOutputStreamOperator<String> mySqlStreamSplit = mySqlStreamInit.process(new SplitProcessFunction())
                .name(name + ":" + "mySqlStreamSplit")
                .uid(name + ":" + "mySqlStreamSplit")
                .startNewChain();

        SideOutputDataStream<String> ddlSideOutput = mySqlStreamSplit.getSideOutput(SplitProcessFunction.ddlTag);
        SideOutputDataStream<String> dmlSideOutput = mySqlStreamSplit.getSideOutput(SplitProcessFunction.dmlTag);
        SideOutputDataStream<String> ___SideOutput = mySqlStreamSplit.getSideOutput(SplitProcessFunction.___Tag);

        DataStream<String> ddlMapStream = ddlSideOutput.map(new DdlMapFunction())
                .name(name + ":" + "ddlMapStream")
                .uid(name + ":" + "ddlMapStream")
                .startNewChain();
        DataStream<String> dmlMapStream = dmlSideOutput.map(new DmlMapFunction())
                .name(name + ":" + "dmlMapStream")
                .uid(name + ":" + "dmlMapStream")
                .startNewChain();
        DataStream<String> ___MapStream = ___SideOutput.map(new DmlMapFunction())
                .name(name + ":" + "___MapStream")
                .uid(name + ":" + "___MapStream")
                .startNewChain();

        DataStreamSink<String> ddlStreamSink = ddlMapStream.addSink(new DdlSinkFunction())
                .name(name + ":" + "ddlStreamSink")
                .uid(name + ":" + "ddlStreamSink");
        DataStreamSink<String> dmlStreamSink = dmlMapStream.addSink(new DmlSinkFunction())
                .name(name + ":" + "dmlStreamSink")
                .uid(name + ":" + "dmlStreamSink");
        DataStreamSink<String> ___StreamSink = ___MapStream.addSink(new PrintSinkFunction<>())
                .name(name + ":" + "___StreamSink")
                .uid(name + ":" + "___StreamSink");

        /*
         * 执行计划
         */
        log.info(env.getExecutionPlan());

        /*
         * 执行任务
         */
        env.execute();

    }

}
