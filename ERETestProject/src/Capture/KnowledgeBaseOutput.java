package Capture;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class KnowledgeBaseOutput 
{
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public void outputKB(String RefID, String Outputpath, String filename, String colLine, String flag) throws SQLException, IOException
	{
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
		Statement stmt = con.createStatement();
        String Query = "select rid, Gender, Remdata from knowledgebase where RefID like '"+RefID+"%'";
        ResultSet res=stmt.executeQuery(Query);
        BufferedWriter br =new BufferedWriter(new FileWriter(Outputpath+"/"+filename, true));
        
        colLine=colLine.substring(("Firstname|middlename|lastname|ssn|dob|").length(), colLine.length());
        if(flag.equals("capture"))
        {
            br.write("ResearchId|"+colLine);
            br.newLine();
        }
        while(res.next())
        {
        	br.write(res.getString(1)+"|"+res.getString(2)+"|"+res.getString(3).replace(",", "|"));
        	br.newLine();
        }
        br.close();
        
        System.out.println("Knowledgebase exported...");
        con.close();
	}

}
