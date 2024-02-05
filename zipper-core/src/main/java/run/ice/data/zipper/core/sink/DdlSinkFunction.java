package run.ice.data.zipper.core.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import run.ice.data.zipper.core.util.FlinkUtil;

import java.sql.Connection;

/**
 * @author DaoDao
 */
@Slf4j
public class DdlSinkFunction extends RichSinkFunction<String> {

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
    public void invoke(String value, Context context) throws Exception {
        if (null == value || value.isEmpty()) {
            return;
        }
        JsonNode jsonNode = objectMapper.readTree(value);
        String database = jsonNode.get("database").asText();
        String use = "USE " + "`" + database + "`";
        connection.createStatement().execute(use);
        String alter = jsonNode.get("alter").asText();
        log.info("DDL ALTER : {}", alter);
        connection.createStatement().execute(alter);
    }

    @Override
    public void close() throws Exception {
        if (null != connection) {
            connection.close();
        }
    }

}
