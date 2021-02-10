import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PubAPI {


    //Create a new public class PubAPI and add a static procedure clearTable(String tblName) that deletes all the entries in the named table
    // but does not drop the table itself. The function throws a SQLException if an error occurs.
    private static String jdbc = "jdbc:derby:";
    private static String name = "publication;";
    private static String derbyStr = jdbc + name;
    private static Connection conn = null;
    private static Statement stmt = null;

    private static void createConnection(){
        try{
            Properties properties = new Properties();
            properties.put("create", "true");
            properties.put("user", "user1");
            properties.put("password", "user1");
            conn = DriverManager.getConnection(derbyStr, properties);
            if(conn ==null)throw new Exception();
            stmt = conn.createStatement();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //Assignment 6, Answer1: clearTable Method
    public static void clearTable(String tblName) throws SQLException {
        //Or "delete from xxx"
        createConnection();
        stmt.executeUpdate("delete from " + tblName);
        //status[0] = true;
    }

}
