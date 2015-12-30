package Update;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.RandomStringUtils;
import org.xml.sax.SAXException;

import Capture.ProcessInputFile;
import Capture.RefIDCreation;

public class UpdateMain 
{

	public String updateProgram(String inputFilePath, String outputFilePath, String workingDir, String updateRuleFilePath) throws IOException, SQLException, ParserConfigurationException, SAXException, InterruptedException
	{
	
		System.out.println("Idenity Update Program Started...");
		String dirProcessFiles =workingDir+"/ProcessFiles";
		System.out.println("Deleting all indexFiles and processfiles from current directory..");
		deleteLocalIndexFiles(dirProcessFiles);
		
		String filename =inputFilePath.substring(inputFilePath.lastIndexOf('/')+1, inputFilePath.length());
		System.out.println(filename);
		String RefID=randGenerator();
		
		ProcessInputFile pif=new ProcessInputFile();
		String colLine=pif.processInputFile(inputFilePath, workingDir);
		
		System.out.println("Adding RefIDs to all the records in the Processed file......");
		RefIDCreation refIdcreation =new RefIDCreation();
		refIdcreation.createRefIDs(workingDir+"/ProcessFiles/ProcessedInputFile.csv", workingDir, RefID);

		MatchProgram mp=new MatchProgram();
		mp.processUpdate(workingDir, outputFilePath, colLine, filename);
		
		return colLine;
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
