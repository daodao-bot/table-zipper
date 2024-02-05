package run.ice.data.zipper.core.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import run.ice.data.zipper.core.util.ZipperUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author DaoDao
 */
@Slf4j
public class DmlMapFunction extends RichMapFunction<String, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String map(String json) throws Exception {
        ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        JsonNode jsonNode = objectMapper.readTree(json);
        Map<String, String> map = new HashMap<>();
        String op = jsonNode.get("op").asText();
        JsonNode source = jsonNode.get("source");
        String database = source.get("db").asText();
        String table = source.get("table").asText();
        // database
        map.put("database", database);
        // table
        map.put("table", table);
        String zipper = ZipperUtil.zipperTableName(parameter, database, table);
        // zipper
        map.put("zipper", zipper);
        JsonNode data;
        if ("c".equals(op) || "r".equals(op)) {
            data = jsonNode.get("after");
        } else {
            data = jsonNode.get("before");
        }
        String key = ZipperUtil.zipperTableKey(parameter, database, table);
        String value = data.get(key).asText();
        // key
        map.put("key", key);
        // value
        map.put("value", value);
        // operation
        map.put("op", op);
        return objectMapper.writeValueAsString(map);
    }

}
