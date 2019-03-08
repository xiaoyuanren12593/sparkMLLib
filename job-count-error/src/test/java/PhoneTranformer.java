
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;


public class PhoneTranformer {
    //得到归属地
    public static String getCity(String tel) {
        try{
            //获取返回结果
            String json = httpRequest(tel).toString();
            //拆分xml页面代码
            String[] a = json.split("att");
            String[] b = a[1].split(",");
            //归属地
            String city = b[2].replace(">", "").replace("</", "");
            return city;
        }catch(Exception e){
            return "暂无相关归属地信息！";
        }
    }

    //得到运营商
    public static String getCarrier(String tel) {
        try{
            //获取返回结果
            String json = httpRequest(tel).toString();
            //拆分xml页面代码
            String[] a = json.split("att");
            String[] c = a[2].split("operators");
            //运营商
            String carrier = c[1].replace(">", "").replace("</", "");
            return carrier;
        }catch(Exception e){
            return "暂无相关运营商信息！";
        }
    }

    //得归属地，运营商。如：西双版纳,中国电信
    public static String getResult(String tel) {
        try{
            //获取返回结果
            String json = httpRequest(tel).toString();
            //拆分xml页面代码
            String[] a = json.split("att");
            String[] b = a[1].split(",");
            //归属地
            String city = b[2].replace(">", "").replace("</", "");
            String[] c = a[2].split("operators");
            //运营商
            String carrier = c[1].replace(">", "").replace("</", "");
            String cityAndCarrier = city+","+carrier;
            return cityAndCarrier;
        }catch(Exception e){
            return "暂无相关归属地、运营商信息！";
        }
    }

    /**
     * 发起http请求获取返回结果
     * @param tel 待查询手机号
     * @return String 结果字符串
     */
    public static String httpRequest(String tel) {

        //组装查询地址(requestUrl 请求地址)
//        String requestUrl = "http://api.k780.com:88/?app=phone.get&phone="+tel+"&appkey=10003&sign=b59bc3ef6191eb9f747dd4e83c99f2a4&format=xml";
        String requestUrl = "http://mobsec-dianhua.baidu.com/dianhua_api/open/location?tel="+tel;

        StringBuffer buffer = new StringBuffer();
        try {
            URL url = new URL(requestUrl);
            HttpURLConnection httpUrlConn = (HttpURLConnection) url.openConnection();
            httpUrlConn.setDoOutput(false);
            httpUrlConn.setDoInput(true);
            httpUrlConn.setUseCaches(false);
            httpUrlConn.setRequestMethod("GET");
            httpUrlConn.connect();
            //将返回的输入流转换成字符串
            InputStream inputStream = httpUrlConn.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            System.out.println(bufferedReader.readLine());
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                buffer.append(str);
            }
            bufferedReader.close();
            inputStreamReader.close();
            //释放资源
            inputStream.close();
            inputStream = null;
            httpUrlConn.disconnect();
        }
        catch (Exception e) {
            return "发起http请求后，获取返回结果失败！";
        }
        return buffer.toString();
    }


    /**
     * 测试
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
//      System.out.println(getMobileFrom("13266197806"));
    	System.out.println(PhoneTranformer.httpRequest("0455-5351552"));
//		System.out.println(PhoneTranformer.getCity("13936939565"));
//		System.out.println(PhoneTranformer.getResult("13936939565"));

    }
}

