package Capture;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;


public class RefIDCreation 
{
	int seqRefId=1;
	public static List<String> dataList =new ArrayList(); 
    
	
    public void createRefIDs(String filePath, String workingdir, String RefID)
    {   
    	
    	Calendar cal1=Calendar.getInstance();
        SimpleDateFormat sdf1 =new SimpleDateFormat("HH:mm:ss");
		String startTime =sdf1.format(cal1.getTime());
    	 
    	 RefIDCreation obj=new RefIDCreation();
	     obj.run(filePath,RefID);
	     obj.writeOutput(workingdir);
	     System.out.println("RefIDs are created and the output File is in Project Directory: /ProcessFiles/IDENTITYRESOLUTIONTEMP.csv");
	     String s;
		 Process p;
		 Process q;
	     try
	     {
	    	 p=Runtime.getRuntime().exec("hadoop fs -rm /user/cloudera/HiveDataFolder/IDENTITYRESOLUTIONTEMP.csv");
	    	 p.waitFor();
	    	 q=Runtime.getRuntime().exec("hadoop fs -copyFromLocal "+ workingdir+"/ProcessFiles/IDENTITYRESOLUTIONTEMP.csv /user/cloudera/HiveDataFolder");
	      	 BufferedReader br =new BufferedReader(new InputStreamReader(q.getInputStream()));
	    	 while((s=br.readLine())!=null)
	    	 {
	    	     System.out.println(s);
	    	     q.waitFor();
	    	 }
	    	 System.out.println("IDENTITYRESOLUTIONTEMP.csv file Successfully transferred to HDFS.....");
	     }
		 catch(Exception e)
		 {
		    System.out.println(e.getMessage());
		 }
	     Calendar cal2=Calendar.getInstance();
	     SimpleDateFormat sdf2 =new SimpleDateFormat("HH:mm:ss");
	 	 String endTime =sdf2.format(cal2.getTime());
	 		
	     System.out.println("RefID added to the input File! Start time: "+startTime+"  End Time: "+endTime);
	     System.gc();
    }
    public void run(String filePath, String RefID)
	{
        
        BufferedReader br=null;
        String dataline="";
        String firstname;
        String middlename;
        String lastname;
        String SSN;
        String DOB;
        String Remdata;
        int lengthof5;
        try
        {
            br = new BufferedReader(new FileReader(filePath));
            while((dataline=br.readLine())!=null)
            {
            	firstname=dataline.split("\\|")[0].replaceAll("[-+^,'!@#$%&*()_?/\\+=-]*","");
            	middlename=dataline.split("\\|")[1].replaceAll("[-+^,'!@#$%&*()_?/\\+=-]*","");
            	lastname=dataline.split("\\|")[2].replaceAll("[-+^,'!@#$%&*()_?/\\+=-]*","");
            	SSN=dataline.split("\\|")[3].replaceAll("[-+^,'!@#$%&*()_+=/\\-]*","");
            	DOB=dataline.split("\\|")[4].replaceAll("[-+^,'!@#$%&*()_+=-]*","");

            	if(DOB.contains("/0"))
            	{
            		DOB=DOB.replace("/0", "/");
            	}
            	if( !DOB.equals("") && DOB.split("")[1].equals("0") )
            	{
            		DOB=DOB.substring(1, DOB.length());
            	}
            	
            	lengthof5=(firstname+"|"+middlename+"|"+lastname+"|"+SSN+"|"+DOB+"|").length();
    			Remdata=dataline.substring(lengthof5,dataline.length());
            	dataList.add(RefID+"_"+seqRefId++ +"|"+firstname+"|"+middlename+"|"+lastname+"|"+SSN+"|"+DOB+"|"+Remdata);
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
	
	}
    public void writeOutput(String workingDir)
	{
    	String OutputfilePath=workingDir+"/ProcessFiles/IDENTITYRESOLUTIONTEMP.csv";
		try
		{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(OutputfilePath));
			for (String s : dataList) 
			{
			     bw.write(s);
			     bw.newLine();
			}
			bw.close();
			System.out.println("RefIDs are added and output file is created...");
			dataList.clear();
		} 
		catch (IOException e) {

			e.printStackTrace();
		}
		
	}
    

}
