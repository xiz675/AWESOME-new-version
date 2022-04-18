package test;

import edu.sdsc.utils.Pair;
import edu.sdsc.utils.RDBMSUtils;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static edu.sdsc.utils.LoadConfig.getConfig;

public class Store2PostgresThread implements Runnable {
    private Iterator<Pair<Integer, String>> dataIter;
    private String tableName;
    private CountDownLatch latch;

    public Store2PostgresThread(List<Pair<Integer, String>> data, String tableName, CountDownLatch latch) {
        this.dataIter = data.iterator();
        this.tableName = tableName;
        this.latch = latch;
    }


    @Override
    public void run() {
        JsonObject config = getConfig("newsDB") ;
        RDBMSUtils db_util = new RDBMSUtils(config, "News");
        Connection con = db_util.getConnection();
        try {
            while (dataIter.hasNext()) {
                int count = 500;
                try (PreparedStatement s2 = con.prepareStatement(
                        "INSERT INTO " + tableName + " (id, news) VALUES (?, ?)"
                )) {
                    long start = System.currentTimeMillis();
                    while (count>0) {
                        Pair<Integer, String> crtDoc = dataIter.next();
                        s2.setObject(1, crtDoc.first);
                        s2.setObject(2, crtDoc.second);
                        s2.addBatch();
                        count -= 1;
                    }
                    s2.executeBatch();
                }
                System.out.println(Thread.currentThread() + " processed  500 docs");
            }
            con.close();
            latch.countDown();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
