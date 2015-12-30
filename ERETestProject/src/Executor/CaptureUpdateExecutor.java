package Executor;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import Update.UpdateMain;
import Capture.MainProgram;

public class CaptureUpdateExecutor 
{
	static String workingDir=System.getProperty("user.dir");
	static String indexFilePath=workingDir+"/XmlFiles/IndexDescriptor.xml";
	static String captureRuleFilePath =workingDir+"/XmlFiles/CaptureruleDescriptor.xml";
	static String updateRuleFilePath =workingDir+"/XmlFiles/UpdateRuleDescriptor.xml";
	static String flag;

	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, InterruptedException, SQLException 
	{
		
		Scanner sc=new Scanner(System.in);
		MainProgram mp =new MainProgram();
		UpdateMain up =new UpdateMain();
		System.out.println("For capture enter 1");
		System.out.println("For update enter 2");
		String option=sc.next();

		if(option.equals("1"))
		{
			System.out.println("Enter input file path for Identity Capture:");
			String inputFilePath=sc.next();
			System.out.println("Enter output file path for Identity Capture:");
			String outputpath=sc.next();
			flag="capture";
			String filename =inputFilePath.substring(inputFilePath.lastIndexOf('/')+1, inputFilePath.length());
			System.out.println(filename);
			mp.captureProgram(inputFilePath,outputpath, workingDir, indexFilePath, captureRuleFilePath, flag, filename);
		}
		else if(option.equals("2"))
		{
			System.out.println("Enter input file path for Identity Update:");
			String inputFilePath=sc.next();
			System.out.println("Enter output file path for Identity Update:");
			String outputpath=sc.next();
			String colLine=up.updateProgram(inputFilePath,outputpath,workingDir,updateRuleFilePath);
			
			String filename =inputFilePath.substring(inputFilePath.lastIndexOf('/')+1, inputFilePath.length());
			System.out.println(filename);
			flag="update";
			String IUCaptureFilePath =workingDir+"/ProcessFiles/UpdateCapture.csv";
			System.out.println("Executing capture on non-matched data...");
			mp.captureProgram(IUCaptureFilePath,outputpath, workingDir,indexFilePath,captureRuleFilePath, flag, filename);
		}
		else
		{
			System.out.println("Wrong option...");
			System.exit(0);
		}
		sc.close();

	}

}
