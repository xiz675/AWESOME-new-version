package test;

import edu.sdsc.utils.Pair;
import edu.sdsc.utils.RDBMSUtils;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static edu.sdsc.utils.LoadConfig.getConfig;
import static edu.sdsc.utils.TinkerpopUtil.getRandomString;

public class Store2PostgresTest {
    private List<Pair<Integer, String>> data2Store;

    private static List<Pair<Integer, String>> createDataset(int size) {
        List<Pair<Integer, String>> data = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        text.append("abc ".repeat(800));
        for (int i=0; i<size; i++) {
            data.add(new Pair<>(i, text.toString()));
        }
        return data;
    }


    public static void main(String[] args) throws InterruptedException {
        JsonObject config = getConfig("newsDB") ;
        RDBMSUtils db_util = new RDBMSUtils(config, "News");
        Connection con = db_util.getConnection();
        StringBuilder sb = new StringBuilder(1024);
        String tableName = "toyset_600_words";
        sb.append("Create table ").append(tableName).append("(id integer , news text)");
        try {
            Statement st = con.createStatement();
            st.execute(sb.toString());
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        List<Pair<Integer, String>> data = createDataset(100000);
//        CountDownLatch latch = new CountDownLatch(5);
//        // have 5 threads and each insert 20000 documents
//        List<List<Pair<Integer, String>>> listOfData = partitionData(data, 5);
//        long start = System.currentTimeMillis();
//        System.out.println(start);
//        for (List<Pair<Integer, String>> i : listOfData) {
//            System.out.println(i.size());
//            Store2PostgresThread p = new Store2PostgresThread(i, tableName, latch);
//            Thread t = new Thread(p);
//            t.start();
//        }
//        latch.await();
//        System.out.println(System.currentTimeMillis() - start);
//        // sequential insert 5 * 2000 documents
//        try {
//            Statement st = con.createStatement();
//            st.execute("Drop table " + tableName);
//            st.close();
//            con.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        long start = System.currentTimeMillis();
        Store2PostgresThread p = new Store2PostgresThread(data, tableName, new CountDownLatch(1));
        p.run();
        System.out.println(System.currentTimeMillis() - start);
        // sequential insert 5 * 2000 documents
//        try {
//            Statement st = con.createStatement();
//            st.execute("Drop table " + tableName);
//            st.close();
//            con.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

    }
}
