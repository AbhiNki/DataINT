package Capture;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class ProcessInputFile 
{
	public String processInputFile(String path, String workingDir) throws IOException
	{
		Calendar cal1=Calendar.getInstance();
        SimpleDateFormat sdf1 =new SimpleDateFormat("HH:mm:ss");
		String startTime =sdf1.format(cal1.getTime());
		
		System.out.println("Processing file for creating RemData(columns apart from first five as one column separated by ',')");	
		BufferedReader br =new BufferedReader(new FileReader(path));
		BufferedWriter bw =new BufferedWriter(new FileWriter(workingDir+"/ProcessFiles/ProcessedInputFile.csv"));
		String columnline=br.readLine();
		String[] colArray=columnline.split("\\|");
		String reqColumn=colArray[0]+"|"+colArray[1]+"|"+colArray[2]+"|"+colArray[3]+"|"+colArray[4]; //columnline.split("|")[0]+"|"+columnline.split("|")[1]+"|"+columnline.split("|")[2]+"|"+columnline.split("|")[3]+"|"+columnline.split("|")[4];

		if(!(reqColumn.toLowerCase().equals("firstname|middlename|lastname|ssn|dob")))
			{
					System.out.println("Check the columns...");
					System.exit(0);
			}
		
		String Remdata, line;
		int lengthof5;

		String newline;
		while((line=br.readLine())!=null)
		{
			lengthof5=(line.split("\\|")[0]+"|"+line.split("\\|")[1]+"|"+line.split("\\|")[2]+"|"+line.split("\\|")[3]+"|"+line.split("\\|")[4]+"|"+line.split("\\|")[5]).length();
			Remdata=line.substring(lengthof5+1,line.length()).replace('|', ',');
			newline =line.split("\\|")[0].trim()+"|"+line.split("\\|")[1].trim()+"|"+line.split("\\|")[2].trim()+"|"+line.split("\\|")[3].trim()+"|"+line.split("\\|")[4].trim()+"|"+line.split("\\|")[4]+"|"+Remdata;
			bw.write(newline);
			bw.newLine();
		}
		br.close();
		bw.close();
		
		System.out.println("File created with RemData column! Outputfile is in the project Directory:/ProcessFiles/ProcessedInputFile.csv");
		
		Calendar cal2=Calendar.getInstance();
        SimpleDateFormat sdf2 =new SimpleDateFormat("HH:mm:ss");
 		String endTime =sdf2.format(cal2.getTime());
 		
    	System.out.println("Processing the input File! Start time: "+startTime+"  End Time: "+endTime);
    	
		System.gc();
		return columnline;
	}

}
///