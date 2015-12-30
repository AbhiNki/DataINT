package Capture;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang3.RandomStringUtils;


public class TransitiveClosure 
{
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public void transitiveClosure(Connection con, String workingDir) throws SQLException, IOException
    {
    	Calendar cal1=Calendar.getInstance();
        SimpleDateFormat sdf1 =new SimpleDateFormat("HH:mm:ss");
		String startTime =sdf1.format(cal1.getTime());
    	System.out.println("Inside Transitive Closure Program.....");
    	List<HashSet> listA=new CopyOnWriteArrayList<HashSet>();
		Set<Set> finalset=new HashSet();
		Set<String> hs3;
		
		 Statement stmt = con.createStatement();
		 String sql="SELECT * FROM TCCANDIDATES";
		 ResultSet res = stmt.executeQuery(sql);
		 
		 while(res.next())
		 {
			 String[] str=res.getString(3).split(",");
			 HashSet<String> hs=new HashSet<>();
			 for(int i=0;i<str.length;i++)
			 {
				 hs.add(str[i]);
		     }
			 listA.add(hs);
		 }
		 /*for(Set s:listA)
		 {
			 System.out.println(s.toString());
		 }*/

		 for(HashSet hs1:listA)
		 {
			 
			 
			 hs3 =new HashSet<String>(hs1);
			 for(HashSet hs2:listA)
			 {
	     		 if(!Collections.disjoint(hs3, hs2))
					 {
	     			     Set<String> hs4 =new HashSet<String>(hs3);
	     			     
						 hs3.addAll(hs2);
						 listA.remove(hs2);
						 listA.remove(hs4);
					 }
	     		 
	          }
			 listA.add((HashSet) hs3);
		 }
			 
		 
		 int counter=0;
		 BufferedWriter bw=new BufferedWriter(new FileWriter(workingDir+"/IndexFiles/TCOUTPUT"));
		 for(Set s: listA)
		 {
			 bw.write("LINK_"+randGenerator()+"|"+s.toString().replace("[","").replace("]", "").trim());
			 bw.newLine();
		 }
		 
		 bw.close();
		 
		 Calendar cal2=Calendar.getInstance();
		 SimpleDateFormat sdf2 =new SimpleDateFormat("HH:mm:ss");
		 String endTime =sdf2.format(cal2.getTime());
		 		
		 System.out.println("Transitive Closure Completed! Start time: "+startTime+"  End Time: "+endTime);
    }
    public static String randGenerator()
	{
		int length=10;
		boolean useLetters=true;
		boolean useNumbers=true;
		String generatedString= RandomStringUtils.random(length, useLetters, useNumbers);
		
		return generatedString.toUpperCase();
		
	}
    
}
