package edu.sdsc.utils;
import java.io.*;
import java.sql.*;
import java.util.*;

import net.sf.jsqlparser.JSQLParserException;

import javax.json.JsonObject;

public class AutoPhraseUtil {
    private Connection connection;

    private String dataPath = "data/EN/patent.txt";
    private String modelPath = "d";
    private String workspace;
    private String vocubulary;
    private String sql;
    private String resultTable;
    private Integer num;
    private Integer min_sup;
    // config of database connection



    public AutoPhraseUtil(Connection con, String resultTable, String col, Integer min_sup, Integer num) {
        this.connection = con;
        this.resultTable = resultTable;
        try {
            InputStream in = new FileInputStream("adil-parser/config.properties");
            Properties p = new Properties();
            p.load(in);
            this.workspace = p.getProperty("autoPhrase_workpath");
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        this.vocubulary = workspace + "data/EN/wiki_quality.txt";
        String[] tabCol = col.split("\\.");
        this.sql = "select \"" + tabCol[1] + "\" from " + tabCol[0];
        this.min_sup = min_sup;
        this.num = num;
    }

    public void callScript() {
        try {
            System.out.println("load data from database");
            storeText(this.sql, this.workspace+this.dataPath, false);
            String scriptPath = "auto_phrase.sh";
            String pos = "1";
            String cmd = "sh  " + scriptPath +" "+ this.modelPath + " " + this.dataPath + " " + pos + " " + this.min_sup;
//        	String[] cmd = {"sh", script, "4"};
            File dir = null;
            dir = new File(this.workspace);
            Process process = Runtime.getRuntime().exec(cmd, null, dir);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
            System.out.println("autophrase finished");
            input.close();
            insertResultToDB(this.num);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void insertResultToDB(Integer num) throws IOException, SQLException, JSQLParserException {
        String resultLoc = this.workspace+this.modelPath + "/autophrase.txt";
        BufferedReader objReader = new BufferedReader(new FileReader(resultLoc));
        String crtLine;
        List<Float> score = new ArrayList<Float>();
        List<String> phrase = new ArrayList<String>();
        Integer count = 0;
        while ((crtLine = objReader.readLine()) != null) {
            String[] data = crtLine.split("\t");
            float s = Float.parseFloat(data[0]);
            if (count >= num) {
                break;
            }
            count += 1;
            score.add(s);
            phrase.add(data[1]);
        }
        Statement create = this.connection.createStatement();
        create.execute("CREATE TABLE " + this.resultTable + " (phrase Text, score FLOAT)");
        create.close();
        String query = " insert into " + this.resultTable + "  (phrase, score)"
                + " values (?, ?)";
        PreparedStatement preparedStmt = this.connection.prepareStatement(query);
        for (int i = 0 ; i < score.size(); i++) {
            preparedStmt.setString(1, phrase.get(i));
            preparedStmt.setFloat(2, score.get(i));
            preparedStmt.addBatch();
        }
        preparedStmt.executeBatch();
        this.connection.close();



//        final Float[] score_arr = new Float[score.size()];
//        int index = 0;
//        for (final Float value: score) {
//            score_arr[index++] = value;
//        }
        // DB_UTILS.storeResult(this.resultTable, true, this.sql, score_arr, phrase.toArray(new String[0]), min_sup, 6);
    }


    private void storeText(String sql, String dataPath, boolean append) throws SQLException, IOException {
        Statement st = this.connection.createStatement();
        ResultSet rs = st.executeQuery(sql);
        Writer output;
        if (append) {
            output = new BufferedWriter(new FileWriter(dataPath, true));
        }
        else {
            output = new BufferedWriter(new FileWriter(dataPath));
        }
        while (rs.next()) {
            String word = rs.getString(1);
            output.write(word + System.getProperty("line.separator"));
        }
        output.close();
    }

    static void storeResult(String table, Connection con, double threshold) throws SQLException, JSQLParserException, IOException {
        String resultLoc = "/Users/zxw/Documents/AutoPhrase/d/autophrase.txt";
        BufferedReader objReader = new BufferedReader(new FileReader(resultLoc));
        String crtLine;
        List<Float> score = new ArrayList<Float>();
        List<String> phrase = new ArrayList<String>();
        while ((crtLine = objReader.readLine()) != null) {
            String[] data = crtLine.split("\t");
            float s = Float.parseFloat(data[0]);
            if (s <= threshold) {
                break;
            }
            score.add(s);
            phrase.add(data[1]);
        }
        Statement create = con.createStatement();
        create.execute("CREATE TABLE " + table + " (phrase Text, score FLOAT)");
        create.close();
        String query = " insert into " + table + "  (phrase, score)"
                + " values (?, ?)";
        PreparedStatement preparedStmt = con.prepareStatement(query);
        for (int i = 0 ; i < score.size(); i++) {
            preparedStmt.setString(1, phrase.get(i));
            preparedStmt.setFloat(2, score.get(i));
            preparedStmt.addBatch();
        }
        preparedStmt.executeBatch();
        con.close();
    }
    public static void main(String[] args) throws JSQLParserException, IOException, SQLException {
        JsonObject config = LoadConfig.getConfig("newsDB");
        Connection conn =  new RDBMSUtils(config, "Patents").getConnection();
        storeResult("xw_cyber_10", conn, 0.6);
        // AutoPhrase call = new AutoPhrase("/Users/zxw/Documents/AutoPhrase/","d", "data/EN/patent.txt", "autophrase", "EN", col);
        // call.callScript(true, min_sup);
    }
}
