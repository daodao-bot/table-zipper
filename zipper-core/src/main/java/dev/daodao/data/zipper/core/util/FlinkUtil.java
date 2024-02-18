package dev.daodao.data.zipper.core.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import dev.daodao.data.zipper.core.constant.ZipperConstant;
import dev.daodao.data.zipper.core.udf.DateTimeFormatFunction;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Flink 工具类
 *
 * @author DaoDao
 */
@Slf4j
public class FlinkUtil {

    public static ParameterTool parameter(Class<?> clazz, String[] args) throws IOException {
        ParameterTool tool = ParameterTool.fromArgs(args);
        ParameterTool parameter;
        if (FlinkUtil.localEnv(tool)) {
            parameter = ParameterTool.fromPropertiesFile(clazz.getResourceAsStream("/task-local.properties"));
        } else {
            parameter = ParameterTool.fromPropertiesFile(clazz.getResourceAsStream("/task.properties"));
        }
        parameter = parameter.mergeWith(tool);
        return parameter;
    }

    /**
     * --env local
     *
     * @param parameter 参数
     * @return 是否为本地环境
     */
    public static Boolean localEnv(ParameterTool parameter) {
        return "local".equals(parameter.get("env"));
    }

    public static StreamExecutionEnvironment env(Class<?> clazz, StreamExecutionEnvironment env, String[] args) throws IOException {
        ParameterTool parameter = FlinkUtil.parameter(clazz, args);
        env.getConfig().setGlobalJobParameters(parameter);

        // 全局并行度
        // env.setParallelism(1);
        // 重启策略-时间间隔配置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(parameter.getInt("restart-strategy.fixed-delay.attempts", 2147483647), Time.of(parameter.getLong("restart-strategy.fixed-delay.delay", 60 * 1000L), TimeUnit.MILLISECONDS)));

        String checkpointUri = parameter.get("state.checkpoints.dir", "file:///data/checkpoint");
        String checkpointDataUri = checkpointUri + File.separator + clazz.getName();
        env.getCheckpointConfig().setCheckpointStorage(checkpointDataUri);

        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(parameter.getLong("execution.checkpointing.interval", 60 * 1000L));
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameter.getLong("execution.checkpointing.min-pause", 60 * 1000L));
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(parameter.getLong("execution.checkpointing.timeout", 60 * 60 * 1000L));
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 开启实验性的 unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(parameter.getInt("execution.checkpointing.tolerable-failed-checkpoints", 100));

        // 状态后端
        //env.setStateBackend(new RocksDBStateBackend(checkpointDataUri));
        return env;
    }

    public static TableEnvironment tEnv(Class<?> clazz, StreamExecutionEnvironment env, String[] args) throws IOException {
        FlinkUtil.env(clazz, env, args);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        ParameterTool parameter = FlinkUtil.parameter(clazz, args);
        env.getConfig().setGlobalJobParameters(parameter);

        String name = clazz.getSimpleName();
        TableConfig tableConfig = tEnv.getConfig();
        tableConfig.setIdleStateRetention(Duration.ofDays(7L));
        Configuration configuration = tableConfig.getConfiguration();
        // set job name
        configuration.setString("pipeline.name", name);
        configuration.setString("table.exec.sink.not-null-enforcer", "drop");
        // 注册自定义函数
        tEnv.createTemporarySystemFunction("DATE_TIME_FORMAT", DateTimeFormatFunction.class);

        return tEnv;
    }

    private static String type(Class<?> clazz) {
        String type = "";
        if (String.class.equals(clazz)) {
            type = "STRING";
        } else if (Boolean.class.equals(clazz)) {
            type = "BOOLEAN";
        } else if (Byte.class.equals(clazz)) {
            type = "TINYINT";
        } else if (Short.class.equals(clazz)) {
            type = "SMALLINT";
        } else if (Integer.class.equals(clazz)) {
            type = "INTEGER";
        } else if (Long.class.equals(clazz)) {
            type = "BIGINT";
        } else if (Float.class.equals(clazz)) {
            type = "FLOAT";
        } else if (Double.class.equals(clazz)) {
            type = "DOUBLE";
        } else if (LocalDate.class.equals(clazz) || Date.class.equals(clazz)) {
            type = "DATE";
        } else if (LocalTime.class.equals(clazz) || java.sql.Time.class.equals(clazz)) {
            type = "TIME(3)";
        } else if (LocalDateTime.class.equals(clazz) || Timestamp.class.equals(clazz)) {
            type = "TIMESTAMP(3)";
        } else if (BigDecimal.class.equals(clazz)) {
            type = "DECIMAL(38,8)";
        } else {
            type = "STRING";
        }
        return type;
    }

    public static void mysqlDriver() {
        try {
            Class.forName(ZipperConstant.MYSQL_DRIVER);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static Connection mysqlConnection(ParameterTool parameter) throws Exception {
        String host = ZipperUtil.mysqlHost(parameter);
        Integer port = ZipperUtil.mysqlPort(parameter);
        String username = ZipperUtil.mysqlUsername(parameter);
        String password = ZipperUtil.mysqlPassword(parameter);
        String url = "jdbc:mysql://" + host + ":" + port + "/";
        return DriverManager.getConnection(url, username, password);
    }

    public static MySqlSource<String> mysqlSource(ParameterTool parameter) {
        String host = ZipperUtil.mysqlHost(parameter);
        Integer port = ZipperUtil.mysqlPort(parameter);
        String username = ZipperUtil.mysqlUsername(parameter);
        String password = ZipperUtil.mysqlPassword(parameter);
        String databases = parameter.get("mysql.databases", "");
        String[] databaseList = databases.split(",");
        for (int i = 0; i < databaseList.length; i++) {
            databaseList[i] = databaseList[i].trim();
        }
        List<String> databaseTables = new ArrayList<>();
        for (String database : databaseList) {
            String tables = parameter.get("mysql.tables." + database, "");
            String[] tableArray = tables.split(",");
            for (int i = 0; i < tableArray.length; i++) {
                tableArray[i] = tableArray[i].trim();
                databaseTables.add(database + "." + tableArray[i]);
            }
        }
        String[] tableList = databaseTables.toArray(new String[0]);

        Map<String, Object> configs = new HashMap<>();
        configs.put("decimal.format", "NUMERIC");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(host)
                .port(port)
                .databaseList(databaseList)
                .tableList(tableList)
                .username(username)
                .password(password)
                .scanNewlyAddedTableEnabled(true)
                .includeSchemaChanges(true)
                .deserializer(new JsonDebeziumDeserializationSchema(false, configs))
                .build();

        return mySqlSource;
    }

}
