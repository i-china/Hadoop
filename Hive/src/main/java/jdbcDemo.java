import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @author Hale Lv
 * @created 2021-02-27 08:00
 * @project Github
 */
public class jdbcDemo {
    public static void main(String[] args) throws Exception  {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://bigdata003/jdbc","root","halelv");
        String sql = "update user set id = 3 where name = 'hale' ";
        Statement stmt = conn.createStatement();
        int count = stmt.executeUpdate(sql);

        System.out.println(count);

        stmt.close();
        conn.close();
    }
}
