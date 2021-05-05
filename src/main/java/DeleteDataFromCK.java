import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class DeleteDataFromCK {
    public static void main(String[] args) throws Exception {

        String table = args[0];
        Class.forName(ru.yandex.clickhouse.ClickHouseDriver.class.getName());
        Properties prop = new Properties();
        prop.put("user", "default");
        prop.put("password", "Lb5NcL09");
        prop.put("driver", ru.yandex.clickhouse.ClickHouseDriver.class.getName());
        String url = "jdbc:clickhouse://192.168.1.31:8123/ODS_LOCAL";
        String sql = "truncate table ODS_LOCAL." + table + " on cluster ch_cluster ";
        Connection conn = null;
        PreparedStatement ptst = null;

        try {
            conn = DriverManager.getConnection(url, prop);
            ptst = conn.prepareStatement(sql);

            boolean execute = ptst.execute();
            System.out.println(execute);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            if (null != ptst) {
                ptst.close();
            }
            if (null != conn) {
                conn.close();
            }
        }


    }
}
