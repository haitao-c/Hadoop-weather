
import java.io.*;

public class AverageTMAXTest{
    static final String TMAX="TMAX";
    static final String ID = "ITE00100554";
    public static void main(String[] args){
      File file = new File("f:"+File.separator+"1789.csv");
      BufferedReader reader = null;
      int sum = 0;
      int count=0;
      try {
        reader = new BufferedReader(new FileReader(file));
        String tempString = null;
        int line = 1;
       
            // 
        while ((tempString = reader.readLine()) != null) {
            String[] array = tempString.split(",");
                //System.out.println(array[2]);
            if(ID.equals(array[0]))
                if(TMAX.equals(array[2])){
                    System.out.println("line " + line + ": " + array[0]+" "+array[1]+" "+array[3]);
                    sum +=Integer.parseInt(array[3]);
                    count++;
                }
                
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        System.out.println(count);
        System.out.println(sum/count);
    }
}