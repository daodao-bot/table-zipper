package dev.daodao.data.zipper.core.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import dev.daodao.data.zipper.core.util.FlinkUtil;
import dev.daodao.data.zipper.core.util.ZipperUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author DaoDao
 * 初始化处理函数
 * 订阅 mysql cdc 数据流，遇到新的数据库表时，自动创建对应的 zipper 表
 */
@Slf4j
public class InitProcessFunction extends ProcessFunction<String, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Connection connection;

    static {
        FlinkUtil.mysqlDriver();
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = FlinkUtil.mysqlConnection(parameter);
    }

    @Override
    public void processElement(String json, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        JsonNode jsonNode = objectMapper.readTree(json);
        JsonNode source = jsonNode.get("source");
        if (null != source && source.isObject()) {
            JsonNode db = source.get("db");
            JsonNode tableNode = source.get("table");
            if (null != db && db.isTextual() && !db.asText().isEmpty() && null != tableNode && tableNode.isTextual() && !tableNode.asText().isEmpty()) {
                String database = db.asText();
                String use = "USE " + "`" + database + "`";
                connection.createStatement().execute(use);
                String table = tableNode.asText();
                String zipper = ZipperUtil.zipperTableName(parameter, database, table);
                String count = "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema = '" + database + "' AND table_name = '" + zipper + "'";
                ResultSet rs = connection.createStatement().executeQuery(count);
                long c = 0L;
                if (rs.next()) {
                    c = rs.getLong("c");
                }
                if (c == 0L) {
                    String show = "SHOW CREATE TABLE " + database + "." + table;
                    rs = connection.createStatement().executeQuery(show);
                    String create = "";
                    if (rs.next()) {
                        create = rs.getString(2);
                    }
                    log.info("{} : {}", show, create);
                    String regex = "(?i)^CREATE\\s+TABLE\\s+(?<database>`?[a-zA-Z0-9_]+`?\\s*\\.\\s*)?(?<table>`?[a-zA-Z0-9_]+`?\\s*\\()[\\s\\S]*(?<engine>\\)\\s*ENGINE\\s*=\\s*InnoDB)[\\s\\S]*$";
                    Matcher matcher = Pattern.compile(regex).matcher(create);
                    if (matcher.matches()) {
                        String k = ZipperUtil.zipperPrimaryKey(parameter, database, table);
                        String s = ZipperUtil.zipperStartTime(parameter, database, table);
                        String e = ZipperUtil.zipperEndTime(parameter, database, table);

                        String sql = create.replaceAll("(?i)\\s+AUTO_INCREMENT\\s*=\\s*\\d+\\s+", " ");
                        sql = sql.replaceAll("(?i)\\s+AUTO_INCREMENT", " ");
                        sql = sql.replaceAll("(?i)\\s+PRIMARY\\s+KEY\\s+", " KEY ");
                        sql = sql.replaceAll("(?i)\\s+UNIQUE\\s+KEY\\s+", " KEY ");
                        sql = sql.replaceAll("(?i)\\s+UNIQUE\\s+INDEX\\s+", " INDEX ");
                        sql = sql.replace(matcher.group("table"), "`" + zipper + "`" + " (\n" +
                                " `" + k + "` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',\n" +
                                " `" + s + "` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',\n" +
                                " `" + e + "` datetime NOT NULL DEFAULT '9999-12-31 23:59:59' COMMENT '结束时间',\n"
                        );
                        sql = sql.replace(matcher.group("engine"), ",\n" +
                                " PRIMARY KEY (`" + k + "`),\n" +
                                " KEY (`" + s + "`),\n" +
                                " KEY (`" + e + "`)\n" +
                                ") ENGINE = InnoDB\n"
                        );
                        log.info("zipper : {}", sql);
                        connection.createStatement().execute(sql);
                    } else {
                        log.error("CREATE TABLE not match : {} : {}", regex, create);
                        throw new RuntimeException("CREATE TABLE not match : " + regex + " : " + create);
                    }
                } else {
                    log.debug("Table exists : {}.{}", database, zipper);
                    // todo 校对表结构，比如字段的个数，类型，顺序，约束 等
                }
            } else {
                log.error("Unknown Database or Table : {}.{}", db, tableNode);
                throw new RuntimeException("Unknown Database or Table : " + db + "." + tableNode);
            }
        } else {
            log.error("Unknown Binlog Json : {}", json);
            throw new RuntimeException("Unknown Binlog Json : " + json);
        }

        collector.collect(json);
    }

    @Override
    public void close() throws Exception {
        if (null != connection) {
            connection.close();
        }
    }

}
