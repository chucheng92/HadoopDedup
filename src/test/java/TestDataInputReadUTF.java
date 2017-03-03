/**
 * Created by Ryan Tao on 2017/3/3.
 */

import java.io.*;

public class TestDataInputReadUTF {
    public static void main(String[] args) throws Exception {
        DataInputStream in = new DataInputStream(new FileInputStream(new File("src/test/data.txt")));
        String s;
        while ((s = in.readLine()) != null) {
            System.out.println(s);
        }
        in.close();
    }
}
