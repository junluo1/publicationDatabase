import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Demonstrates creating Publisher, Journal, Article, Author, WrittenBy tables
 * in database for publication example
 *
 * @author Jun Luo Haozhao Zeng
 * CCIS ID: junluo01 hauser
 */
public class Assignment6 {

    private static String jdbc = "jdbc:derby:";
    private static String name = "publication;";
    private static String derbyStr = jdbc + name;
    private static Connection conn = null;
    private static Statement stmt = null;


    public static void main(String[] args) {
        createConnection();
        clearTables();
        createTables();
        shutdown();
    }
    private static void createConnection(){
        try{
            Properties properties = new Properties();
            properties.put("create", "true");
            properties.put("user", "user1");
            properties.put("password", "user1");
            Connection conn = DriverManager.getConnection(derbyStr, properties);
            if(conn !=null)System.out.println("connect to the database");
            stmt = conn.createStatement();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void createTables() {
        try{
            /**
             * Delete possible old triggers
             */
            String oldTrigger = "t2";
            try {
                stmt.executeUpdate("drop TRIGGER " + oldTrigger);
                System.out.println("Success: Drop TRIGGER " + oldTrigger);
            } catch (SQLException ex) {
                System.out.println("Did Not Drop TRIGGER " + oldTrigger +" as it does not exist.");
            }
            /**
             * Delete possible old tables.
             */
            String oldTables[] = {"WrittenBy","Author","Article","Journal","Publisher"};
            for (String old : oldTables) {
                try {
                    stmt.executeUpdate("drop table " + old);
                    System.out.println("Success: Drop table " + old );
                } catch (SQLException ex) {
                    System.out.println("Did Not Drop table " + old +" as it does not exist.");
                }
            }
            /**
             * delete possible old functiond
             */
            String functions[] = {"parseIssn", "isIssn", "parseOrcid", "isOrcid", "isDoi"};
            for(String func: functions){
                try{
                    stmt.executeUpdate("drop Function " + func);
                    System.out.println("Success: Drop Function " + func);
                }catch(SQLException e){
                    System.out.println("Did not drop function "+func+" because it does not exist.");
                }
            }

            //Assignment 6, Answer 2: create stored functions for all of the functions in Biblo:
            //parseIssn, isIssn, parseOrcid, isOrcid, isDoi.
            /**
             * create stored functions
             */
            //parseIssn
            String parseIssn = "CREATE FUNCTION parseIssn (ISSN VARCHAR(64)) RETURNS INT" +
                " PARAMETER STYLE JAVA LANGUAGE JAVA DETERMINISTIC " +
                //" NO_SQL  " +
                "EXTERNAL NAME 'Biblio.parseIssn'  ";
            stmt.executeUpdate(parseIssn);

            //isIssn
            String isIssn = "CREATE FUNCTION isIssn( ISSN INT  ) RETURNS BOOLEAN" +
                " PARAMETER STYLE JAVA LANGUAGE JAVA DETERMINISTIC " +
                //"NO_SQL " +
                "EXTERNAL NAME'Biblio.isIssn' ";
            stmt.executeUpdate(isIssn);

            //parseOrcid
            String parseOrcid = "CREATE FUNCTION parseOrcid( ORCID VARCHAR(64)) RETURNS BIGINT" +
                " PARAMETER STYLE JAVA LANGUAGE JAVA DETERMINISTIC " +
                //"NO_SQL " +
                "EXTERNAL NAME 'Biblio.parseOrcid'  ";
            stmt.executeUpdate(parseOrcid);

            //isOrcid
            String isOrcid = "CREATE FUNCTION isOrcid( ORCID BIGINT    ) RETURNS BOOLEAN" +
                " PARAMETER STYLE JAVA LANGUAGE JAVA DETERMINISTIC " +
                //"NO_SQL " +
                "EXTERNAL NAME 'Biblio.isOrcid' ";
            stmt.executeUpdate(isOrcid);

            //isDoi
            String isDoi = "CREATE FUNCTION isDoi( DOI VARCHAR(64)) RETURNS BOOLEAN" +
                " PARAMETER STYLE JAVA LANGUAGE JAVA DETERMINISTIC " +
                //"NO_SQL " +
                "EXTERNAL NAME 'Biblio.isDoi' ";
            stmt.executeUpdate(isDoi);

            /**
             * create the Publisher, the entity table
             */
            String publisher = "create table Publisher( Name varchar(32) NOT NULL, City varchar(16) NOT NULL, PRIMARY KEY (NAME))";// PRIMARY KEY (NAME)
            stmt.executeUpdate(publisher);
            System.out.println("Success: Create entity table Publisher");


            //Assignment 6, Answer 3: replace the validation checks with calls to Biblio validation stored functions.
            // create the Journal, the entity table
            String journal = "create table Journal( Title varchar(32) NOT NULL, ISSN int NOT NULL, Name varchar(32) NOT NULL," +
                            "PRIMARY KEY(ISSN)," +
                            "CONSTRAINT FK_Journal FOREIGN KEY (Name) REFERENCES Publisher (Name) ON delete cascade," +
                            "CHECK(isIssn(ISSN)), "+
                            "CHECK ((ISSN >=" +  0x00000001  +" AND ISSN <=" +  0x7999999A  +") OR (issn >= "+0x80000000+" AND ISSN <="+0x9999999A+")  ))";//references Publisher(Name),PRIMARY KEY(ISSN)
            stmt.executeUpdate(journal);
            System.out.println("Success: Create entity table Journal");

            // create the Article table
            String article ="create table Article ( Title varchar(32) NOT NULL,  DOI varchar(64) NOT NULL, ISSN int NOT NULL, " +
                    "PRIMARY KEY (DOI), CONSTRAINT FK_Article FOREIGN KEY(ISSN) REFERENCES Journal(ISSN) ON delete cascade,"
                +"CHECK( isIssn(ISSN)), CHECK ( isDoi(DOI)),"
            +"CHECK (  (ISSN >=" +  0x00000001  +" AND ISSN <=" +  0x7999999A  +") OR (issn >= "+0x80000000+" AND ISSN <="+0x9999999A+") ))";//references Publisher(Name),PRIMARY KEY(ISSN);
            stmt.executeUpdate(article);
            System.out.println("Success: Create entity table Article");

            // create the Author entity table
            String author ="create table Author( FamilyName varchar(16) NOT NULL, GivenName varchar(16) NOT NULL, ORCID bigint NOT NULL, " +
                    "PRIMARY KEY (ORCID), check( isOrcid(ORCID)),CHECK(ORCID >= 1 and ORCID <= 9999999999999999))";
            stmt.executeUpdate(author);
            System.out.println("Success: Create entity table Author");

            // create the WrittenBy relation table
            String writtenBy = "create table WrittenBy( " +
                    "DOI varchar(64) NOT NULL, ORCID bigint NOT NULL, " +
                    "CONSTRAINT FK_Writtenby FOREIGN kEY(DOI) REFERENCES Article(DOI) ON delete cascade" +
                    ",CONSTRAINT FK_Author FOREIGN kEY(ORCID) REFERENCES Author(ORCID) ON delete cascade, " +
                    "PRIMARY KEY (DOI, ORCID) ,check(isDoi(DOI)),check(isOrcid(ORCID)),check( isOrcid(ORCID)),CHECK(ORCID >= 1 and ORCID <= 9999999999999999))";
            stmt.executeUpdate(writtenBy);
            System.out.println("Success: Create relation table WrittenBy");

            //If the last article by an Author is deleted, ensure that the Author entry is also deleted.
            String check2 = "CREATE TRIGGER t2 " +
                "after delete on article " +
                "REFERENCING old_table as deleted_Article " +
                "FOR EACH STATEMENT MODE DB2SQL " +
                 "DELETE FROM Author WHERE ORCID IN ((select ORCID from Author) except " +
                "(select Orcid from writtenBy join deleted_Article on writtenBy.DOI != deleted_Article.DOI))";
            stmt.executeUpdate(check2);

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void shutdown(){
        try
        {
            if (stmt != null) stmt.close();
            if (conn != null) {
                DriverManager.getConnection(derbyStr + ";shutdown=true");
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * CLEAR TABLES STORED PROCEDURE
     */
    private static void clearTables(){
        try{
            //drop stored procedure of clearTable
            stmt.executeUpdate("drop Procedure clearTable" );
            System.out.println("Success: Dropped Procedure clearTable");

        }catch(SQLException E){
            System.out.println("Did not drop Procedure clearTable because it does not exist.");
        }

        try{
            //Assignment 6, Answer 2: create stored procedure of clearTable from PubAPI.
            String cleartbl = "CREATE PROCEDURE clearTable(" +
                " tblName varchar(64))" +
                " PARAMETER STYLE JAVA LANGUAGE JAVA " +
                "DETERMINISTIC " +
//                "NO_SQL " +
                "EXTERNAL NAME 'PubAPI.clearTable'";
            stmt.executeUpdate(cleartbl);
        }catch(SQLException ex){
            ex.printStackTrace();
        }

    }
}
