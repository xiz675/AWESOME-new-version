package edu.sdsc.utils;
import edu.sdsc.datatype.execution.AwesomeRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;

import javax.json.JsonObject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SolrUtil {
    private String solrUrl;
    private String instance;

    public SolrUtil(JsonObject config, String solrName) {
        JsonObject solrConfig = config.getJsonObject(solrName);
        instance = solrConfig.getString("instance");
        solrUrl = solrConfig.getString("URL");
    }

    private HttpSolrClient getClient() {
        // here uses an http client
        return new HttpSolrClient.Builder()
                .withBaseSolrUrl(solrUrl)
                .build();
    }

    private SolrParams parseUserSolrQuery(String rawSolrQuery) {
        Map<String, String> paramMap = new HashMap<>();
        try {
            String[] nameValues = rawSolrQuery.split("&");
            for (String nameValue : nameValues) {
                String[] split = nameValue.split("=");
                // todo : should add row here
                paramMap.put(split[0], split[1]);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Parse Error for query: %s.\n", rawSolrQuery));
        }
        return new MapSolrParams(paramMap);
    }

//    public List<AwesomeRecord> executeResult(String solrQuery, List<String> cols) throws IOException, SolrServerException {
//        HttpSolrClient client = getClient();
//        SolrParams params = parseUserSolrQuery(solrQuery);
//        QueryResponse response = client.query(databaseName, params);
//        SolrDocumentList docs = response.getResults();
//        return docs.stream()
//    }


    public List<AwesomeRecord> execute(String solrQuery, List<String> cols) throws SolrServerException, IOException {
        HttpSolrClient client = getClient();
        SolrParams params = parseUserSolrQuery(solrQuery);
        QueryResponse response = client.query(instance, params);
        SolrDocumentList docs = response.getResults();
        List<AwesomeRecord> result = new ArrayList<>();
        for (SolrDocument doc : docs) {
            Map<String, Object> temp = new HashMap<>();
            for (String col : cols) {
                temp.put(col, doc.getFirstValue(col));
            }
            result.add(new AwesomeRecord(temp));
        }
        return result;
    }

    public SolrDocumentList executeRawResult(String solrQuery, List<String> cols) throws SolrServerException, IOException {
        HttpSolrClient client = getClient();
        SolrParams params = parseUserSolrQuery(solrQuery);
        QueryResponse response = client.query(instance, params);
        return response.getResults();
    }

    public void executeStore(String solrQuery, List<String> cols, String table, Connection conn) throws IOException, SolrServerException, SQLException {
        HttpSolrClient client = getClient();
        SolrParams params = parseUserSolrQuery(solrQuery);
        QueryResponse response = client.query(instance, params);
        SolrDocumentList docs = response.getResults();
        StringBuilder sb = new StringBuilder( 1024 );
        sb.append( "Create table " ).append( table ).append( " ( " );

        for ( int i = 0; i < cols.size(); i ++ ) {
            if ( i > 0 ) sb.append( ", " );
            sb.append("\"").append( cols.get(i) ).append( "\" " ).append( "text" );
        } // for columns
        sb.append( " ) " );
        Statement st = conn.createStatement();
        st.execute(sb.toString());
        st.close();

        try (PreparedStatement s2 = conn.prepareStatement(
                "INSERT INTO " + table + " ("
                        + cols.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", "))
                        + ") VALUES ("
                        + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
                        + ")"
        )) {
            for (SolrDocument doc : docs) {
                for (int i = 1; i <= cols.size(); i++) {
                    s2.setString(i, (String) doc.getFirstValue(cols.get(i-1)));
                }
                s2.addBatch();
            }
            s2.executeBatch();
        }
    }



    public static void main(String[] args) throws IOException, SolrServerException {
        // get an http client
        JsonObject config = LoadConfig.getConfig("newsDB");
        SolrUtil solrCon = new SolrUtil(config, "nrde-project");
        System.out.println(solrCon.solrUrl);
        HttpSolrClient client = solrCon.getClient();
        String query = "q=information-class:\"pulications:scholar\" AND text-field:weapon&rows=10";
        SolrParams params = solrCon.parseUserSolrQuery(query);
        QueryResponse response = client.query("nrde-project", params);
        SolrDocumentList docs = response.getResults();

        for (SolrDocument doc : docs) {
            String id = (String) doc.getFirstValue("text-field");
            String name = (String) doc.getFirstValue("id");
            System.out.format("id: %s, name: %s\n", id, name);
        }
        System.out.format("Found %d docs.\n", docs.getNumFound());
    }
}

