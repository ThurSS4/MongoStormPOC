package storm;

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.connection.ClusterSettings;
import org.bson.Document;

import java.util.Collections;

public class MongoConnector {
    private MongoClient mongoClient;

    public MongoConnector() {
        final String HOST_ADDRESS = "localhost";
        final int HOST_PORT = 27017;

        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                            public void apply(ClusterSettings.Builder builder) {
                                builder.hosts(Collections.singletonList(new ServerAddress(HOST_ADDRESS, HOST_PORT)));
                            }
                        })
                        .build());
    }

    public MongoCollection<Document> getCollection(String collectionName) {
        return mongoClient.getDatabase("newdb").getCollection(collectionName);
    }
}
