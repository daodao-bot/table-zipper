package run.ice.data.zipper.core.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流处理函数
 * 将 binlog 数据流分流为 DDL 和 DML 两个流
 * DDL 流用于创建 zipper 表
 * DML 流用于同步数据
 * ___ 流用于未知数据
 *
 * @author DaoDao
 */
@Slf4j
public class SplitProcessFunction extends ProcessFunction<String, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public static final OutputTag<String> ddlTag = new OutputTag<>("DDL") {
    };

    public static final OutputTag<String> dmlTag = new OutputTag<>("DML") {
    };

    public static final OutputTag<String> ___Tag = new OutputTag<>("___") {
    };

    @Override
    public void processElement(String json, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {

        // 发送到主输出流
        collector.collect(json);

        // 发送到侧输出流
        try {
            JsonNode jsonNode = objectMapper.readTree(json);
            JsonNode op = jsonNode.get("op");
            JsonNode historyRecord = jsonNode.get("historyRecord");
            if (null != op && op.isTextual() && ("c".equals(op.asText()) || "r".equals(op.asText()) || "u".equals(op.asText()) || "d".equals(op.asText()))) {
                context.output(dmlTag, json);
            } else if (null != historyRecord && historyRecord.isTextual() && !historyRecord.asText().isEmpty()) {
                String historyRecordString = historyRecord.asText();
                JsonNode historyRecordJsonNode = objectMapper.readTree(historyRecordString);
                JsonNode ddl = historyRecordJsonNode.get("ddl");
                if (null != ddl && ddl.isTextual() && !ddl.asText().isEmpty()) {
                    context.output(ddlTag, historyRecordString);
                } else {
                    log.error("Unknown DDL value : {}", historyRecordString);
                    context.output(___Tag, json);
                }
            } else {
                log.error("Unknown Binlog Json : {}", json);
                context.output(___Tag, json);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            context.output(___Tag, json);
            throw e;
        }
    }

}
