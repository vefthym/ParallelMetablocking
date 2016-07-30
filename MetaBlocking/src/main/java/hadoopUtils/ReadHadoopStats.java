/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoopUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;

/**
 *
 * @author VASILIS
 */
public class ReadHadoopStats {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws MalformedURLException, UnsupportedEncodingException, IOException {
        final int PARTITIONS = 728; //<--------------CHANGE THIS VALUE EACH TIME
        
        final String TASKID = "201607021850";
        for (int i = 0; i < PARTITIONS; i++) {                        
            String reduceId = String.format("%06d", i);
            System.out.print(i+":"); //i is the partition number
                       
            String urlStr = "http://83.212.123.10:50030/taskstats.jsp?tipid=task_"+TASKID+"_0001_r_"+reduceId;
            
            URL url = new URL(URLDecoder.decode(urlStr, "UTF-8"));
            URLConnection con = url.openConnection();
            
            try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"))) {
                String input;
                while ((input = in.readLine()) != null) {
                    if (input.contains("CLEAN_BLOCKS")) {
                        String nextLine = in.readLine();
                        System.out.print(nextLine.substring(nextLine.indexOf(">")+1, nextLine.lastIndexOf("<"))+":");
                    }
                    if (input.contains("COMPARISONS")) {
                        String nextLine = in.readLine();
                        System.out.println(nextLine.substring(nextLine.indexOf(">")+1, nextLine.lastIndexOf("<")));
                        continue;
                    }
                }
            }
        }
        
        
        
    }
    
}
