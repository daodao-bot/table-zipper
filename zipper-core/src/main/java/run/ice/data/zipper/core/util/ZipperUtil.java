package run.ice.data.zipper.core.util;

import org.apache.flink.api.java.utils.ParameterTool;
import run.ice.data.zipper.core.constant.ZipperConstant;

/**
 * Zipper 工具类
 *
 * @author DaoDao
 */
public class ZipperUtil {

    public static String mysqlHost(ParameterTool parameter) {
        return parameter.get("mysql.host", ZipperConstant.MYSQL_HOST);
    }

    public static Integer mysqlPort(ParameterTool parameter) {
        return parameter.getInt("mysql.port", ZipperConstant.MYSQL_PORT);
    }

    public static String mysqlUsername(ParameterTool parameter) {
        return parameter.get("mysql.username", ZipperConstant.MYSQL_USERNAME);
    }

    public static String mysqlPassword(ParameterTool parameter) {
        return parameter.get("mysql.password", ZipperConstant.MYSQL_PASSWORD);
    }

    public static String zipperTableSuffix(ParameterTool parameter, String database, String table) {
        return parameter.get("zipper.table-suffix." + database + "." + table, parameter.get("zipper.table-suffix", ZipperConstant.ZIPPER_TABLE_SUFFIX));
    }

    public static String zipperTableKey(ParameterTool parameter, String database, String table) {
        return parameter.get("zipper.table-key." + database + "." + table, parameter.get("zipper.table-key", ZipperConstant.ZIPPER_TABLE_KEY));
    }

    public static String zipperTableName(ParameterTool parameter, String database, String table) {
        return table + zipperTableSuffix(parameter, database, table);
    }

    public static String zipperPrimaryKey(ParameterTool parameter, String database, String table) {
        return parameter.get("zipper.primary-key." + database + "." + table, parameter.get("zipper.primary-key", ZipperConstant.ZIPPER_PRIMARY_KEY));
    }

    public static String zipperStartTime(ParameterTool parameter, String database, String table) {
        return parameter.get("zipper.start-time." + database + "." + table, parameter.get("zipper.start-time", ZipperConstant.ZIPPER_START_TIME));
    }

    public static String zipperEndTime(ParameterTool parameter, String database, String table) {
        return parameter.get("zipper.end-time." + database + "." + table, parameter.get("zipper.end-time", ZipperConstant.ZIPPER_END_TIME));
    }

}
