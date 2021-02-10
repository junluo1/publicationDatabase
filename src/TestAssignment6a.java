import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;


/**
 * This program tests the version of the publication database tables for Assignment 5 
 * that uses attributes for the PublishedBy and PublishedIn relations. The sample
 * data is stored in a tab-separated data file The columns of the data file are:
 * pubName, pubCity, jnlName, jnlISSN, artTitle, artDOI, auFamiily, auGiven, auORCID
 * 
 * @author philip gust
 */
public class TestAssignment6a {

	public static void main(String[] args) {
	    // the default framework is embedded
	    String protocol = "jdbc:derby:";
	    String dbName = "publication";
		String connStr = protocol + dbName+ ";create=true";
		int I=0, j=0;
	    // tables tested by this program
		String dbTables[] = {
			"WrittenBy",		// relations
			"Author", "Article", "Journal", "Publisher"		// entities
	    };

		// name of data file
		String fileName = "pubdata.txt";

		Properties props = new Properties(); // connection properties
        // providing a user name and password is optional in the embedded
        // and derbyclient frameworks
        props.put("user", "user1");
        props.put("password", "user1");

        // result set for queries
        //ResultSet resultSet = null;

		try (
			// open data file
			BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
			
			// connect to database
			Connection  conn = DriverManager.getConnection(connStr, props);
			Statement stmt = conn.createStatement();
			/**
			 * Assignment 6, Answer 6 and Answer 7: Modify the INSERT prepared statements.
			 */
			// insert prepared statements
			PreparedStatement insertRow_Publisher = conn.prepareStatement(
					"insert into Publisher values(?, ?)");
			PreparedStatement insertRow_Journal = conn.prepareStatement(
					"insert into Journal values(?, parseIssn(?), ?)");
			PreparedStatement insertRow_Article = conn.prepareStatement(
					"insert into Article values(?, ?, parseIssn(?))");
			PreparedStatement insertRow_Author = conn.prepareStatement(
					"insert into Author values(?, ?, parseOrcid(?))");
			PreparedStatement insertRow_WrittenBy = conn.prepareStatement(
					"insert into WrittenBy values(?, parseOrcid(?))");
		) {
			// connect to the database using URL
            System.out.println("Connected to database " + dbName);
            
            // clear data from tables
            for (String tbl : dbTables) {
            	//Assignment 6, Answer 4: modify the code that clears the contents of the database tables to call the new clearTable()
				// database stored function for each of the publication tables.
				CallableStatement cstmt = conn.prepareCall("call clearTable(?,?)");//clearTable
	            try {
					cstmt.setString(1,tbl);
					cstmt.registerOutParameter(2, Types.BOOLEAN);
					cstmt.execute();
					boolean result = cstmt.getBoolean(2);
					if(!result)throw new SQLException();
	            	System.out.println("delete from  table " + tbl);
					//cstmt.close();

	            } catch (SQLException ex) {
	            	System.out.println("Did not delete from table " + tbl);
	            }

            }

			String line;
			while ((line = br.readLine()) != null) {

				// split input line into fields at tab delimiter
				String[] data = line.split("\t");
				if (data.length != 9) continue;
			
				// get fields from input data
				String publisherName = data[0];
				String publisherCity = data[1];
				
				// add Publisher if does not exist
				try {
					insertRow_Publisher.setString(1, publisherName);
					insertRow_Publisher.setString(2, publisherCity);
					insertRow_Publisher.execute();
				} catch (SQLException ex) {
					// already exists
					// System.err.printf("Already inserted Publisher %s City %s\n", publisherName, publisherCity);
				}
				
				// get fields from input data
				String journalTitle = data[2];
				String journalIssn = ""; // no ISSN

				// add Journal if does not exist
				try {

					journalIssn = data[3];
					/**
					 * Assignment 6, Answer 5: create tests that invoke each of the stored functions using a values query.
					 */
					PreparedStatement invoke_isIssn = conn.prepareStatement("values ( isISSN(parseIssn(?)) )");
					invoke_isIssn.setString(1, journalIssn);
					ResultSet resultSet = invoke_isIssn.executeQuery();
					if (resultSet.next()) {
						boolean status = resultSet.getBoolean(1);
						if(status) ;//System.out.printf(journalIssn + "is valid");
						else System.out.printf(journalIssn + "is not valid");
					}
					resultSet.close();
					insertRow_Journal.setString(1, journalTitle);
					insertRow_Journal.setString(2, journalIssn); //Assignment 6, Answer 6 and Answer 7
					insertRow_Journal.setString(3, publisherName);
					insertRow_Journal.execute();
				} catch (SQLException ex) {
					if(ex.getSQLState().equals( "23505" )){
						System.out.println("ISSN "+ data[3]+" is invalid");
					};
				} catch (NumberFormatException ex) {
					System.out.printf("Unable to insert Journal %s invalid Issn %s\n", journalTitle, data[3]);
				}

				// add Article if does not exist
				String articleTitle = data[4];
				String articleDOI = data[5];

				try {
					/**
					 * Assignment 6, Answer 5: create tests that invoke each of the stored functions using a values query.
					 */
					PreparedStatement invoke_isDOI = conn.prepareStatement("values ( isDoi(?) )");
					invoke_isDOI.setString(1, articleDOI);
					ResultSet resultSet = invoke_isDOI.executeQuery();
					if (resultSet.next()) {
						boolean status = resultSet.getBoolean(1);
						if(status) ;//System.out.printf(articleDOI + "is valid");
						else System.out.printf(articleDOI + "is not valid"); }
					resultSet.close();
					insertRow_Article.setString(1, articleTitle);
					insertRow_Article.setString(2, articleDOI); //Assignment 6, Answer 6 and Answer 7
					insertRow_Article.setString(3, journalIssn); //Assignment 6, Answer 6 and Answer 7
					insertRow_Article.execute();
				} catch (SQLException ex) {
					//Assignment 6, Answer 8: Modify the SQLException exception handlers for the INSERT statements.
					//String LANG_CHECK_CONSTRAINT_VIOLATED  = "23513";
					if(ex.getSQLState().equals( "23505" )){
						System.out.println("DOI "+ data[5]+" is invalid");
					};
				}

				// add Author if does not exist
				String authorFamilyName = data[6];
				String authorGivenName = data[7];
				String authorORCID = "";
				try {
					authorORCID = data[8];
					/**
					 * Assignment 6, Answer 5: create tests that invoke each of the stored functions using a values query.
					 */
					PreparedStatement invoke_isOrcid = conn.prepareStatement("values ( isOrcid(parseOrcid(?)) )");
					invoke_isOrcid.setString(1, authorORCID);
					ResultSet resultSet = invoke_isOrcid.executeQuery();
					if (resultSet.next()) {
						boolean status = resultSet.getBoolean(1);
						if(status) ;//System.out.printf(authorORCID + "is valid");
						else System.out.println(authorORCID + "is not valid");}
					resultSet.close();
					insertRow_Author.setString(1, authorFamilyName);
					insertRow_Author.setString(2, authorGivenName);
					insertRow_Author.setString(3, authorORCID); //Assignment 6, Answer 6 and Answer 7

					insertRow_Author.execute();

				} catch (SQLException ex) {
					//Assignment 6, Answer 8: Modify the SQLException exception handlers for the INSERT statements.
					//String LANG_CHECK_CONSTRAINT_VIOLATED  = "23513";
					if(ex.getSQLState().equals( "23505" )||ex.getSQLState().equals("LANG_CHECK_CONSTRAINT_VIOLATED") ){
						System.out.println("ORCID "+ data[8]+" is invalid");
					};
					//System.out.println("authorORCID is wrong:"+ex.getSQLState()); //23505
					//for(String s: data)System.out.print(s);

				} catch (NumberFormatException ex) {
					 System.out.printf("Unable to insert Author %s, %s invalid ORCID %s\n",
							 authorFamilyName, authorGivenName, data[8]);
				}

				// add WrittenBy if does not exist
				try {
					insertRow_WrittenBy.setString(1, articleDOI);
					insertRow_WrittenBy.setString(2, authorORCID);
					insertRow_WrittenBy.execute();
				} catch (SQLException ex) {
					// already exists
					//System.out.printf("Already inserted WrittenBy %64s ORCID %d\n", articleDOI, authorORCID);
				}

			}

			//invalid case
			/**
			 * Assignment 6, Answer 5: create tests that invoke each of the stored functions using a values query.
			 * Assignment 6, Answer 8: SQLException.getSQLState() method.
			 */
			System.out.println("Start to test invalid that's not in pubdata.txt. ");
			String journalIssn = "0000-000b";
			try {
				PreparedStatement invoke_isIssn = conn.prepareStatement("values ( isISSN(parseIssn(?)) )");
				invoke_isIssn.setString(1, journalIssn);
				ResultSet resultSet = invoke_isIssn.executeQuery();
				if (resultSet.next()) {
					boolean status = resultSet.getBoolean(1);
					if (status) ; //System.out.printf(journalIssn + " is valid");
					else System.err.printf("Test data: " + journalIssn + " is not valid"); //not in
				}
				resultSet.close();
			}catch(SQLException e){
				//System.out.println("e.getSQLState():"+e.getSQLState());
				if(e.getSQLState().equals("38000"))
				System.err.println("Test data: journalIssn " + journalIssn + " is not valid.");
			}

			String articleDOI = "10.11092.889091";
			try{
				PreparedStatement invoke_isDOI = conn.prepareStatement("values ( isDoi(?) )");
				invoke_isDOI.setString(1, articleDOI);
				ResultSet resultSet = invoke_isDOI.executeQuery();
				if (resultSet.next()) {
					boolean status = resultSet.getBoolean(1);
					if (status) ;//System.out.printf(articleDOI + "is valid");
					else System.err.println("Test data: articleDOI "+articleDOI + " is not valid.");
				}
				resultSet.close();
			}catch(SQLException e){
				//System.err.println("Test data: articleDOI "+articleDOI + "is not valid.");
			}
			String authorORCID = "0000-0000-0000-00dd";
			try{
				PreparedStatement invoke_isOrcid = conn.prepareStatement("values ( isOrcid(parseOrcid(?)) )");
				invoke_isOrcid.setString(1, authorORCID);
				ResultSet resultSet = invoke_isOrcid.executeQuery();
				if (resultSet.next()) {
					boolean status = resultSet.getBoolean(1);
					if (status) System.err.printf(authorORCID + "is valid");
					else System.err.println("Test data: "+authorORCID + " is not valid.");
				}
				resultSet.close();
			} catch (SQLException e) {
				//Assignment 6, Answer 8: Modify the SQLException exception handlers for the INSERT statements.
				//String LANG_CHECK_CONSTRAINT_VIOLATED  = "23513";
				//System.out.println("e.getSQLState():"+e.getSQLState());
				if(e.getSQLState().equals("38000"))
				System.err.println("Test data: authorORCID "+ authorORCID + " is not valid.");
			} catch (NumberFormatException ex) {
				System.err.printf("Unable to insert, invalid data");
			}

			// print number of rows in tables
			for (String tbl : dbTables) {
				ResultSet resultSet = stmt.executeQuery("select count(*) from " + tbl);
				if (resultSet.next()) {
					int count = resultSet.getInt(1);
					System.out.printf("Table %s : count: %d\n", tbl, count);
				}
				resultSet.close();
			}


			// delete article
			System.out.println("\nDeleting article 10.1145/2838730 from CACM with 3 authors");
			stmt.execute("delete from Article where doi = '10.1145/2838730'");
			PubUtil.printArticles(conn);
			PubUtil.printAuthors(conn);

			// delete publisher ACM
			System.out.println("\nDeleting publisher ACM");
			stmt.executeUpdate("delete from Publisher where name = 'ACM'");
			PubUtil.printPublishers(conn);
			PubUtil.printJournals(conn);
			PubUtil.printArticles(conn);
			PubUtil.printAuthors(conn);

			// delete journal Spectrum (0018-9235)
			System.out.println("\nDeleting journal Spectrum from IEEE");
			stmt.executeUpdate("delete from Journal where issn = " + Biblio.parseIssn("0018-9235"));
			PubUtil.printJournals(conn);
			PubUtil.printArticles(conn);
			PubUtil.printAuthors(conn);

			// delete journal Computer
			System.out.println("\nDeleting journal Computer from IEEE");
			stmt.executeUpdate("delete from Journal where title = 'Computer'");
			PubUtil.printPublishers(conn);
			PubUtil.printJournals(conn);
			PubUtil.printArticles(conn);
			PubUtil.printAuthors(conn);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
}
