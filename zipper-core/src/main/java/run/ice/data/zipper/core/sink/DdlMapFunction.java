package run.ice.data.zipper.core.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import run.ice.data.zipper.core.util.ZipperUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author DaoDao
 */
@Slf4j
public class DdlMapFunction extends RichMapFunction<String, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String map(String json) throws Exception {
        ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        JsonNode jsonNode = objectMapper.readTree(json);
        Map<String, String> map = new HashMap<>();
        JsonNode ddlNode = jsonNode.get("ddl");
        JsonNode databaseNode = jsonNode.get("databaseName");
        String ddl = ddlNode.asText();
        String database = databaseNode.asText();
        String regex = "(?i)^ALTER\\s+TABLE\\s+(`?(?<database>[a-zA-Z0-9_]+)`?\\s*\\.\\s*)?`?(?<table>[a-zA-Z0-9_]+)`?\\s+.*$";
        Matcher matcher = Pattern.compile(regex).matcher(ddl);
        if (matcher.matches()) {
            String table = matcher.group("table");
            String zipper = ZipperUtil.zipperTableName(parameter, database, table);
            String alter = ddl.replace(matcher.group("table"), zipper);
            alter = alter.replaceAll("(?i)\\s+PRIMARY\\s+KEY", " KEY");
            alter = alter.replaceAll("(?i)\\s+UNIQUE\\s+KEY", " KEY");
            alter = alter.replaceAll("(?i)\\s+UNIQUE\\s+INDEX\\s+", " INDEX ");
            alter = alter.replaceAll("(?i)\\s+AUTO_INCREMENT", " ");
            map.put("database", database);
            map.put("table", table);
            map.put("zipper", zipper);
            map.put("alter", alter);
            return objectMapper.writeValueAsString(map);
        } else {
            // todo 支持 TRUNCATE & COLUMN & INDEX ...
            log.error("DDL not match : {} : {}", regex, ddl);
            // throw new RuntimeException("DDL not match: " + ddl);
            return null;
        }
    }

}
