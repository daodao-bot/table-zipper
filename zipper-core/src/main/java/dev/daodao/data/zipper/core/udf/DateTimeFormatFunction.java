package dev.daodao.data.zipper.core.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

/**
 * @author DaoDao
 */
public class DateTimeFormatFunction extends ScalarFunction {

    public static final String DEFAULT_DATE_TIME_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME.toString();

    private final String pattern;

    public DateTimeFormatFunction() {
        this.pattern = DEFAULT_DATE_TIME_FORMAT;
    }

    public DateTimeFormatFunction(String pattern) {
        this.pattern = pattern;
    }

    public String eval(Timestamp timestamp) {
        if (null == timestamp) {
            return null;
        }
        return timestamp.toLocalDateTime().format(DateTimeFormatter.ofPattern(pattern));
    }

    public String eval(Timestamp timestamp, String pattern) {
        if (null == timestamp) {
            return null;
        }
        if (null == pattern) {
            pattern = DEFAULT_DATE_TIME_FORMAT;
        }
        return timestamp.toLocalDateTime().format(DateTimeFormatter.ofPattern(pattern));
    }

}
