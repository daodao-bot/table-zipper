package run.ice.data.zipper.core.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import run.ice.data.zipper.core.constant.ZipperConstant;
import run.ice.data.zipper.core.util.FlinkUtil;
import run.ice.data.zipper.core.util.ZipperUtil;

import java.sql.Connection;

/**
 * @author DaoDao
 */
@Slf4j
public class DmlSinkFunction extends RichSinkFunction<String> {

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
    public void invoke(String json, Context context) throws Exception {
        if (null == json || json.isEmpty()) {
            return;
        }
        ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        JsonNode jsonNode = objectMapper.readTree(json);
        String database = jsonNode.get("database").asText();
        String table = jsonNode.get("table").asText();
        String k = ZipperUtil.zipperPrimaryKey(parameter, database, table);
        String s = ZipperUtil.zipperStartTime(parameter, database, table);
        String e = ZipperUtil.zipperEndTime(parameter, database, table);
        String kv = "NULL";
        String sv = "CURRENT_TIMESTAMP";
        String ev = "'" + ZipperConstant.ZIPPER_END_OF_TIME + "'";
        String zipper = jsonNode.get("zipper").asText();
        String key = jsonNode.get("key").asText();
        String value = jsonNode.get("value").asText();
        String op = jsonNode.get("op").asText();

        String use = "USE " + "`" + database + "`";
        connection.createStatement().execute(use);

        String update = "UPDATE " + "`" + database + "`" + "." + "`" + zipper + "`" +
                " SET " + "`" + e + "`" + " = " + sv +
                " WHERE " + "`" + key + "`" + " = " + "'" + value + "'" +
                " AND " + "`" + e + "`" + " = " + ev +
                " ORDER BY " + "`" + k + "`" + " DESC" +
                " LIMIT 1";
        log.info("DML UPDATE : {}", update);
        connection.createStatement().execute(update);

        if (!"d".equals(op)) {
            String insert = "INSERT INTO " + "`" + database + "`" + "." + "`" + zipper + "`" +
                    " SELECT " + kv + ", " + sv + ", " + ev + ", " + "`" + database + "`" + "." + "`" + table + "`" + ".* " +
                    " FROM " + "`" + database + "`" + "." + "`" + table + "`" +
                    " WHERE " + "`" + key + "`" + " = " + "'" + value + "'";
            log.info("DML INSERT : {}", insert);
            connection.createStatement().execute(insert);
        }

    }

    @Override
    public void close() throws Exception {
        if (null != connection) {
            connection.close();
        }
    }

}
