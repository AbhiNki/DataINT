package Capture;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.RandomStringUtils;
import org.xml.sax.SAXException;

public class MainProgram {

	public static void captureProgram(String inputFilePath, String outputFilePath,String workingDir,String indexFilePath,String captureRuleFilePath, String flag, String filename) throws ParserConfigurationException, SAXException, IOException, InterruptedException, SQLException 
	{
		Calendar cal1=Calendar.getInstance();
        SimpleDateFormat sdf1 =new SimpleDateFormat("HH:mm:ss");
		String startTime =sdf1.format(cal1.getTime());
		
		System.out.println("Identity Capture Program started...");
		
        String dirIndexfiles=workingDir+"/IndexFiles";
        //String dirProcessFiles =workingDir+"/ProcessFiles";
		System.out.println("Deleting all indexFiles from current directory..");
		deleteLocalIndexFiles(dirIndexfiles);
		//deleteLocalIndexFiles(dirProcessFiles);
		System.out.println("Files from indexfile and processfiles directory are deleted...");
		
		
		String RefID=randGenerator();
		

		ProcessInputFile pif=new ProcessInputFile();
		String colLine=pif.processInputFile(inputFilePath, workingDir);
		
		
		System.out.println("Adding RefIDs to all the records in the Processed file......");
		RefIDCreation refIdcreation =new RefIDCreation();
		refIdcreation.createRefIDs(workingDir+"/ProcessFiles/ProcessedInputFile.csv", workingDir, RefID);
		
		System.out.println("Reading Index Descriptor xml.....");
	    XmlReaderClass xmlrdr =new XmlReaderClass();
	    HashMap<String, String> indexMap= xmlrdr.xmlRead(indexFilePath);
	    
	    System.out.println("Indices are as follows.....");
	    for(Entry<String, String> map: indexMap.entrySet())
	    {
	    	System.out.println(map.getKey()+"    "+map.getValue());
	    }
	    
	    String indexCols="";
	    String s;
	    Process p;
	    
	    for(Entry<String, String> hs: indexMap.entrySet())
	    {
	    	if(hs.getValue().toLowerCase().contains(("firstname")))
	    	{
                indexCols+=1+",";	    		
	    	}
	    	if(hs.getValue().toLowerCase().contains("lastname"))
	    	{
	    	    indexCols+=3+",";	
	    	}
	    	if(hs.getValue().toLowerCase().contains("ssn"))
	    	{
	    	    indexCols+=4+",";	
	    	}
	    	if(hs.getValue().toLowerCase().contains("dob"))
	    	{
	    	    indexCols+=5+",";	
	    	}
	    	
	    	System.out.println("Running Indexing Map Reduce with index as :"+indexCols.substring(0, indexCols.length()-1));
	    	
	    	p=Runtime.getRuntime().exec("hadoop jar /home/cloudera/Desktop/executable_jars/IndexTest_v1.jar IndexTest /user/cloudera/HiveDataFolder/IDENTITYRESOLUTIONTEMP.csv /user/cloudera/"+hs.getKey()+" "+indexCols.substring(0, indexCols.length()-1)+" "+hs.getKey());
	    	p.waitFor();
	    	BufferedReader br =new BufferedReader(new InputStreamReader(p.getInputStream()));
	    	
	    	while((s=br.readLine())!=null)
	    	{
	    	    System.out.println(s);
	    	}
	    	indexCols="";
	    	System.out.println("IndexOutputs on HDFS" + "/user/cloudera/"+hs.getKey()+"/part-r-00000"+" created...");
	    }
	    
	    int noOfIndex=indexMap.size();
    	MergeIndexFiles mif=new MergeIndexFiles();
    	mif.mergeFiles(noOfIndex, workingDir);
    	System.out.println("Input for MainIndexTable is Created...");
    	
    	
    	String filePath=workingDir+"/IndexFiles/MainIndexTabInput";
    	AllQueryOperation aqo=new AllQueryOperation();    	
    	aqo.performQueryOp(filePath, workingDir, flag);
    	
    	PairWiseMatching pwm=new PairWiseMatching();
    	pwm.pairWiseMatching(workingDir);
    	
    	Calendar cal2=Calendar.getInstance();
        SimpleDateFormat sdf2 =new SimpleDateFormat("HH:mm:ss");
 		String endTime =sdf2.format(cal2.getTime());
 		
 		KnowledgeBaseOutput kbo =new KnowledgeBaseOutput();
 		kbo.outputKB(RefID, outputFilePath, filename,colLine, flag);
    	System.out.println("Start time: "+startTime);
    	System.out.println("End Time: "+endTime);
	    
    	System.gc();

	}
	public static String randGenerator()
	{
		int length=8;
		boolean useLetters=true;
		boolean useNumbers=true;
		String generatedString= RandomStringUtils.random(length, useLetters, useNumbers);
		return generatedString.toUpperCase();
	}
	public static void deleteLocalIndexFiles(String dir)
	{
		File path=new File(dir);
        for(File file:path.listFiles())
        {
        	file.delete();
        }
        System.out.println("Directory cleared for new files...");
	}

}
///home/cloudera/Desktop/datasets/Input/InputFile.csv
//home/cloudera/Desktop/datasets/Input/updateInput.csv
///home/cloudera/Desktop/datasets/Output