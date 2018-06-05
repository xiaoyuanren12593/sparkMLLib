package Gbk_UTF;

import java.io.File;
import java.io.IOException;

/**
 * Created by MK on 2018/4/18.
 *
 */


public class TransferProject {
    private static void transferFile(String pathName, int depth) throws Exception {
        File dirFile = new File(pathName);

        if (!isValidFile(dirFile)) return;
        //获取此目录下的所有文件名与目录名
        String[] fileList = dirFile.list();
        int currentDepth = depth + 1;
        assert fileList != null;
        for (String string : fileList) {
            File file = new File(dirFile.getPath(), string);
            String name = file.getName();
//            如果是一个目录，搜索深度depth++，输出目录名后，进行递归
            if (file.isDirectory()) {
                //递归
                transferFile(file.getCanonicalPath(), currentDepth);
            } else {
                if (name.contains(".java") || name.contains(".properties") || name.contains(".xml") || name.contains(".txt") || name.contains(".csv")) {
                    readAndWrite(file);
                    System.out.println(name + " has converted to utf8 ");
                }
            }
        }
    }


    private static boolean isValidFile(File dirFile) throws IOException {
        if (dirFile.exists()) {
            System.out.println("file exist");
            return true;
        }
        if (dirFile.isDirectory()) {
            if (dirFile.isFile()) {
                System.out.println(dirFile.getCanonicalFile());
            }
            return true;
        }
        return false;
    }

    private static void readAndWrite(File file) throws Exception {
        String content = FileUtils.readFileByEncode(file.getPath());
        FileUtils.writeByBufferedReader(file.getPath(), new String(content.getBytes("UTF-8"), "UTF-8"));
    }




    public static void main(String[] args) throws Exception {
        //程序入口，制定src的path
//        String path = "/Users/mac/Downloads/unit06_jdbc/src";
//        String path = "F:\\tmp\\company\\enter";
        String path = "C:\\Users\\a2589\\Desktop\\需求one\\是否是雇主（小表）";
        transferFile(path, 1);
    }
}



