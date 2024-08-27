package com.outbox.app;

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
/**
 * Hello world!
 *
 */
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

            // Create a replication slot
            pgConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName("test_slot")
                    .withOutputPlugin("pgoutput")
                    .make();

            // Create a replication stream
            PGReplicationStream stream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName("test_slot")
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", "my_publication")
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
                String s = new String(source, offset, length);

                // Process the change
                System.out.println(s);

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
}
