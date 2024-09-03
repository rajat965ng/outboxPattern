package com.outbox.app;

import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class App {
    public static void main(String argsp[]){
        Properties props = new Properties();
        PGProperty.USER.set(props, "postgres");
        PGProperty.PASSWORD.set(props, "postgres");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        String url = "jdbc:postgresql://localhost:5432/employee";

        try (Connection conn = DriverManager.getConnection(url, props)) {
            PGConnection pgConnection = conn.unwrap(PGConnection.class);

            // Create a replication stream
            PGReplicationStream stream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName("test_slot_1")
                    .withSlotOption("include-xids", false)
                    .withSlotOption("skip-empty-xacts", true)
                    .start();

            System.out.println("---------------------START STREAMING--------------------------");
            while (true) {
                // Read changes
                ByteBuffer msg = stream.readPending();

                if (msg == null) {
                    TimeUnit.MILLISECONDS.sleep(10L);
                    continue;
                }

                int offset = msg.arrayOffset();
                byte[] source = msg.array();
                int length = source.length - offset;
                String data = new String(source, offset, length);

                // Convert to JSON
                JSONObject json = convertToJson(data);
                System.out.println(json.toString(2));


                // Acknowledge receipt of the message
                LogSequenceNumber lsn = stream.getLastReceiveLSN();
                stream.setFlushedLSN(lsn);
                stream.setAppliedLSN(lsn);
            }
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("---------------------END STREAMING--------------------------");
        }
    }

    private static JSONObject convertToJson(String data) {
        JSONObject result = new JSONObject();
        JSONArray allChanges = new JSONArray();

        String[] lines = data.split("\n");

        Pattern tablePattern = Pattern.compile("table ([\\w.]+): (INSERT|UPDATE|DELETE):");
        Pattern columnPattern = Pattern.compile("(\\w+)\\[(\\w+)]:(.+?)(?=\\s\\w+\\[|$)");

        for (String line : lines) {
            if (line.equals("BEGIN") || line.equals("COMMIT")) {
                continue;
            }

            JSONObject change = new JSONObject();
            Matcher tableMatcher = tablePattern.matcher(line);
            if (tableMatcher.find()) {
                change.put("table", tableMatcher.group(1));
                change.put("operation", tableMatcher.group(2));
            }

            JSONObject columns = new JSONObject();
            Matcher columnMatcher = columnPattern.matcher(line);
            while (columnMatcher.find()) {
                String columnName = columnMatcher.group(1);
                String dataType = columnMatcher.group(2);
                String value = columnMatcher.group(3).trim();

                // Remove quotes for string values
                if (value.startsWith("'") && value.endsWith("'")) {
                    value = value.substring(1, value.length() - 1);
                }

                JSONObject columnInfo = new JSONObject();
                columnInfo.put("type", dataType);
                columnInfo.put("value", value);

                columns.put(columnName, columnInfo);
            }
            change.put("data", columns);

            allChanges.put(change);
        }

        result.put("changes", allChanges);
        return result;
    }

}
