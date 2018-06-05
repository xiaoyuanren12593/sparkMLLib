package Gbk_UTF;

import java.io.*;

/**
 * Created by MK on 2018/4/18.
 *
 */
public  class FileUtils {
   public static void writeByBufferedReader(String path, String content) {
        try {
            File file = new File(path);
            file.delete();
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file, false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.flush();
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

   public static String readFileByEncode(String path) throws Exception {
        InputStream input = new FileInputStream(path);
        InputStreamReader in = new InputStreamReader(input, "GBK");
        BufferedReader reader = new BufferedReader(in);
        StringBuilder sb = new StringBuilder();
        String line = reader.readLine();
        while (line != null) {
            sb.append(line);
            sb.append("\r\n");
            line = reader.readLine();
        }
        return sb.toString();
    }
}