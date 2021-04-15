import java.io.*;

public class test {

    public static void main(String[] args) throws IOException {

        InputStream in=new FileInputStream("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\hello.txt");

        //缓冲流
        BufferedInputStream reader=new BufferedInputStream(in);

        //字符流
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in,"UTF-8"));

        String[] split=null;

        while (bufferedReader.readLine() != null){
             split = bufferedReader.readLine().split("\t");

          //  bufferedReader.readLine();
        }

        for (String s : split) {

            System.out.print(s+"\n");
        }



    }
}
