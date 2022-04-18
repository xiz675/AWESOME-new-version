package edu.sdsc.createdataset;

import edu.sdsc.utils.Pair;
import edu.sdsc.utils.RDBMSUtils;

import javax.json.JsonObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.*;

import static edu.sdsc.utils.LoadConfig.getConfig;
import static edu.sdsc.utils.TinkerpopUtil.getRandomString;

public class Case1Dataset {
    // read keywords list
    public static List<String> readFile(String path, Integer... count) {
        List<String> keywords = new ArrayList<>();
        int num = 0;
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                keywords.add(data);
                num ++ ;
                if (count.length == 1 && num >= count[0]) {
                    break;
                }
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return keywords;
    }

    // create documents from random words and keywords
    public static List<Pair<Integer, String>> create5000Docs(List<String> keywords, int numOfKeywordsPerDoc, int numOfWords, int startIndex) {
        List<Pair<Integer, String>> docs = new ArrayList<>();
        for (int i = startIndex; i < startIndex + 1000; i++) {
            Random rand = new Random();
            StringBuilder text = new StringBuilder();
            for (int j=0; j<numOfWords-numOfKeywordsPerDoc; j++) {
                text.append(getRandomString(3)).append(" ");
            }
            for (int k=0; k<numOfKeywordsPerDoc; k++) {
                int randomIndex = rand.nextInt(keywords.size());
                text.append(keywords.get(randomIndex)).append(" ");
            }
            docs.add(new Pair<>(i, text.toString()));
        }
        return docs;
    }


    // store to Postgres
    public static void store2Postgres(Connection con, Integer docLen, Integer keywordsSize, Integer keywordPerDoc, List<String> keywords) throws SQLException {
        StringBuilder sb = new StringBuilder(1024);
        assert keywords.size() == keywordsSize;
        String tableName = String.format("toyset_%d_%d_%d", docLen, keywordsSize, keywordPerDoc);
        sb.append("Create table ").append(tableName).append("(id integer , news text)");
        try {
            Statement st = con.createStatement();
            st.execute(sb.toString());
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 200; i++) {
            Iterator<Pair<Integer, String>> docsIter = create5000Docs(keywords, keywordPerDoc, docLen, 5000*i).iterator();
            try (PreparedStatement s2 = con.prepareStatement(
                    "INSERT INTO " + tableName + " (id, news) VALUES (?, ?)"
            )) {
                while (docsIter.hasNext()) {
                    Pair<Integer, String> crtDoc = docsIter.next();
                    s2.setObject(1, crtDoc.first);
                    s2.setObject(2, crtDoc.second);
                    s2.addBatch();
                }
                s2.executeBatch();
            }
            if (i%10 == 0) {
                System.out.println("processed " + i*5000 + " docs");
            }
        }
        con.close();
    }

    public static void main(String[] args) throws SQLException {
        // database infomation
        JsonObject config = getConfig("newsDB") ;
        RDBMSUtils db_util = new RDBMSUtils(config, "News");
        Connection con = db_util.getConnection();

        // documents dataset parameters
        int numOfWords = 600;
        // number of keywords in each document
        int numOfKeywords = 5;
        int numOfDocs = 50_000_000;

        // keywords list parameters
        int keywordsSize = 300;

        List<String> keywords = readFile("C:/Users/xw/Documents/lastName.txt", keywordsSize);
        store2Postgres(con, numOfWords, keywordsSize, numOfKeywords, keywords);
    }

}
