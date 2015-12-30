package Update;

import Capture.MainProgram;
import Capture.XmlReaderClass;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.xml.sax.SAXException;

public class MatchProgram 
{
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public static  void processUpdate(String workingDir, String outputpath, String colLine, String filename) throws SQLException, ParserConfigurationException, SAXException, IOException, InterruptedException 
	
	{
		 try 
		 {
		      Class.forName(driverName);
		 }
		 catch (ClassNotFoundException e) 
		 {
		      e.printStackTrace();
		      System.exit(1);
		 }
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
		Statement stmt = con.createStatement();
		
		
		/*Clean all the required tables except Knowledgebase*/
		
		String sql ="TRUNCATE TABLE IDENTITYRESOLUTIONTEMP";
		stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
		sql="TRUNCATE TABLE MATCHEDREFID";
		stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
		sql="TRUNCATE TABLE NEWCAPTURE";
		stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
			
		sql="LOAD DATA LOCAL INPATH '"+workingDir+"/ProcessFiles/IDENTITYRESOLUTIONTEMP.csv' INTO TABLE IDENTITYRESOLUTIONTEMP";
		stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
		ResultSet res;
		HashMap<String, String> matchOP=new HashMap<String, String>();
		BufferedWriter bw =new BufferedWriter(new FileWriter(workingDir+"/ProcessFiles/MatchOP"));
		
		XmlReaderClass xr=new XmlReaderClass();
		HashMap<String,String> updaterules=xr.xmlRead(workingDir+"/XmlFiles/UpdateRuleDescriptor.xml");
		System.out.println("Update Rules:");
		for(Entry<String,String> ur: updaterules.entrySet())
		{
			System.out.println(ur.getKey()+"  "+ur.getValue());
		}
        String Query ="";
        String[] col =null;
        int noOfreqMatch=(int) Math.ceil((float)updaterules.size()/2);
        System.out.println("No of required matches: "+noOfreqMatch);
        for(Entry<String, String> hs: updaterules.entrySet())
        {
        	if(hs.getValue().contains("|"))
        	{
        		if(StringUtils.countMatches(hs.getValue(), "|")==2)
        		{
        			System.out.println(hs.getValue());
        			col =hs.getValue().split("\\|");
        			Query="select kb.RID, ident.RefID from knowledgebase kb, Identityresolutiontemp ident where kb.'"+col[0].split(",")[0].toLowerCase()+"'=ident.'"+col[0].split(",")[0].toLowerCase()+"'";
        			Query+=" and kb.'"+col[1].split(",")[0].toLowerCase()+"'=ident.'"+col[1].split(",")[0].toLowerCase()+"'";
        			Query+=" and kb.'"+col[2].split(",")[0].toLowerCase()+"'=ident.'"+col[2].split(",")[0].toLowerCase()+"'";
        		}
        		if(StringUtils.countMatches(hs.getValue(), "|")==1)
        		{
        			System.out.println(hs.getValue());
        			col =hs.getValue().split("\\|");
        			Query="select kb.RID, ident.RefID from knowledgebase kb, Identityresolutiontemp ident where kb."+col[0].split(",")[0].toLowerCase()+"=ident."+col[0].split(",")[0].toLowerCase();
        			Query+=" and kb."+col[1].split(",")[0].toLowerCase()+"=ident."+col[1].split(",")[0].toLowerCase()+"";
        		}
        	}
        	if(!hs.getValue().contains("|"))
        	{
        		String singleCol=hs.getValue().split(",")[0];
        		Query="select kb.RID, ident.RefID from knowledgebase kb, Identityresolutiontemp ident where kb.'"+singleCol.toLowerCase()+"'=ident.'"+singleCol.toLowerCase()+"'";
        	}
        	System.out.println(Query);
        	res=stmt.executeQuery(Query);
        	String key;
        	String value;
        	int matchCount;
        	while(res.next())
        	{
        		if(matchOP.containsKey(res.getString(1)))
        		{
        			key=res.getString(1);
        			matchCount=Integer.parseInt(matchOP.get(key).split(",")[1])+1;
        			value=matchOP.get(key).substring(0, matchOP.get(key).length()-1)+matchCount;
        			matchOP.put(key, value);
        			System.out.println(key+"  "+value);
        		}
        		else if(!matchOP.containsKey(res.getString(1)))
        		{
        			matchOP.put(res.getString(1), res.getString(2)+","+1);
        			System.out.println(res.getString(1)+"  "+ res.getString(2)+","+1);
        		}
        	}
        	matchCount=0;
        	
        	Query="";
        }
        int counter=0;
        for(Entry<String,String> hs: matchOP.entrySet())
        {
        	//System.out.println(counter++ +" : "+hs.getKey()+"   "+hs.getValue());
        	bw.write(hs.getKey()+"|"+hs.getValue().split(",")[0]+"|"+hs.getValue().split(",")[1]);
        	bw.newLine();
        }
        bw.close();
        
        sql="LOAD DATA LOCAL INPATH '"+workingDir+"/ProcessFiles/MatchOP' INTO TABLE MATCHEDREFID";
        stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
		sql="INSERT INTO KNOWLEDGEBASE SELECT a.RID, a.REFID, b.FIRSTNAME, b.MIDDLENAME, b.LASTNAME, b.SSN, b.DOB, b.GENDER, b.REMDATA from MATCHEDREFID a, IDENTITYRESOLUTIONTEMP b WHERE a.REFID= b.REFID and a.COUNT >="+noOfreqMatch;
		stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
		BufferedWriter out= new BufferedWriter(new FileWriter(outputpath+"/"+filename));
		String colLinenew=colLine.substring(("firstname|middlename|lastname|ssn|dob|").length(), colLine.length());
		out.write("ResearchID|"+colLinenew);
		out.newLine();
		sql="SELECT a.RID, a.REFID, b.FIRSTNAME, b.MIDDLENAME, b.LASTNAME, b.SSN, b.DOB, b.GENDER, b.REMDATA from MATCHEDREFID a, IDENTITYRESOLUTIONTEMP b WHERE a.REFID= b.REFID and a.COUNT >="+noOfreqMatch;
		res=stmt.executeQuery(sql);
		while(res.next())
		{
			out.write(res.getString(1)+"|"+res.getString(8)+"|"+res.getString(9).replace(",", "|"));
			out.newLine();
		}
		out.close();
		
		sql ="INSERT INTO NEWCAPTURE SELECT FIRSTNAME, MIDDLENAME, LASTNAME, SSN, DOB, GENDER, REMDATA from IDENTITYRESOLUTIONTEMP IDENT where IDENT.REFID NOT IN (SELECT MR.REFID FROM MATCHEDREFID MR WHERE MR.count >="+noOfreqMatch+")";
		stmt.execute(sql);
		System.out.println("Executing: "+sql);
		
		sql ="SELECT * FROM NEWCAPTURE";
		res=stmt.executeQuery(sql);
		
		deleteLocalIndexFiles(workingDir+"/ProcessFiles");
		
		BufferedWriter writer=new BufferedWriter(new FileWriter(workingDir+"/ProcessFiles/UpdateCapture.csv"));
		
		writer.write(colLine);
		writer.newLine();
		while(res.next())
		{
			writer.write(res.getString(1)+"|"+res.getString(2)+"|"+res.getString(3)+"|"+res.getString(4)+"|"+res.getString(5)+"|"+res.getString(6)+"|"+res.getString(7).replace(",","|"));
			writer.newLine();
		}
		writer.close();
		System.out.println("Update completed and Capture input created...");
		System.out.println("______________________________________________");
	}
	
	public static void deleteLocalIndexFiles(String dir)
	{
		File path=new File(dir);
        for(File file:path.listFiles())
        {
        	file.delete();
        }
        System.out.println("ProcessFiles directory cleared for new files...");
	}

}
