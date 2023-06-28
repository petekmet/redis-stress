package com.eurex.r7.notifier.integration;

import io.github.dengliming.redismodule.redisearch.client.RediSearchClient;
import io.github.dengliming.redismodule.redisearch.index.IndexDefinition;
import io.github.dengliming.redismodule.redisearch.index.IndexDefinition.DataType;
import io.github.dengliming.redismodule.redisearch.index.IndexOptions;
import io.github.dengliming.redismodule.redisearch.index.schema.Field;
import io.github.dengliming.redismodule.redisearch.index.schema.FieldType;
import io.github.dengliming.redismodule.redisearch.index.schema.Schema;
import io.github.dengliming.redismodule.redisearch.index.schema.TagField;

import org.junit.jupiter.api.Test;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.BatchOptions.ExecutionMode;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootTest(classes = RefDataRepositoryTest2Configuration.class)
public class RediStressTest {
    
    @Autowired
    private RediSearchClient client;
    // private static String ADO_ID = "myado3";

    public static String APP_KEY_PREFIX = "tk1:";
    public static String REFD_ENTITY_KEY_PREFIX = "rd:";
    public static final String TS_ENTITY_KEY_PREFIX = "ts:";

    @Test
    public void testDropAll() {
        dropIndex("Bond");
        dropIndex("Option");
        dropIndex("Future");
    }

    @Test
    public void testDropBond() {
        dropIndex("Bond");
    }

    @Test
    public void testDropFuture() {
        dropIndex("Future");
    }

    @Test
    public void testDropOption() {
        dropIndex("Option");
    }

    @Test
    public void testCreateIndexAll() {
        try {
            setupIndex("Bond");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            setupIndex("Future");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            setupIndex("Option");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateBondIndex() throws InterruptedException, ExecutionException {
        setupIndex("Bond");
    }

    @Test
    public void testCreateFutureIndex() throws InterruptedException, ExecutionException {
        setupIndex("Future");
    }

    @Test
    public void testCreateOptionIndex() throws InterruptedException, ExecutionException {
        setupIndex("Option");
    }

    @Test
    public void testGenerateBond() throws InterruptedException, ExecutionException {
        createBondInstruments(10000);
    }

    @Test
    public void testGenerateFuture() throws InterruptedException, ExecutionException {
        createFutureInstruments(10000);
    }

    @Test
    public void testGenerateOption() throws InterruptedException, ExecutionException {
        createOptionInstruments(10000);
    }

    private void setupIndex(String indexName) throws InterruptedException, ExecutionException {
        client.getRediSearch("ref"+indexName+"Idx")
            .createIndex(new Schema()
            .addField(new TagField("bdm"))
            .addField(new TagField("id"))
            .addField(new Field("asOf", FieldType.NUMERIC).sortable()),
            new IndexOptions().definition(new IndexDefinition(DataType.HASH).setPrefixes(List.of(APP_KEY_PREFIX+REFD_ENTITY_KEY_PREFIX+indexName.toLowerCase()+":"))));

        client.getRediSearch("ts"+indexName+"Idx")
            .createIndex(new Schema()
            .addField(new TagField("id").sortable())
            .addField(new Field("asOf", FieldType.NUMERIC).sortable()),
            new IndexOptions().definition(new IndexDefinition(DataType.HASH).setPrefixes(List.of(APP_KEY_PREFIX+TS_ENTITY_KEY_PREFIX+indexName.toLowerCase()+":"))));
    }

    private void dropIndex(String indexName) {
        client.getRediSearch("ref"+indexName+"Idx").dropIndex(); // remove index and indexed records
        client.getRediSearch("ts"+indexName+"Idx").dropIndex();
    }

    private void createBondInstruments(int totalInstruments) throws InterruptedException, ExecutionException {
        var columns = createRandomColumnNames(5); // attribute columns
        var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
        for (int i = 0; i <  totalInstruments; i++) {
            var id = String.format("BON.%06d", i);
            
            final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
            batch.getMap("tk1:rd:option:"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, "bond", 0, 8));
            
            createRandomTimeSeries(id, randomBondTimeSeriesRecord())
            .stream()
            .forEach(tsRecord -> {
                batch.getMap("tk1:ts:bond:"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
            });
            
            lst.add(batch.executeAsync().toCompletableFuture());
            System.out.println("saved TS for "+id);
            if(i%2500==2499){
                System.out.println("flushing batches...");
                CompletableFuture.allOf(lst.toArray(new CompletableFuture[lst.size()])).get();
                lst.clear();
            }
            
        }
    }

    private void createFutureInstruments(int totalInstruments) throws InterruptedException, ExecutionException {
        var columns = createRandomColumnNames(5); // attribute columns
        var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
        for (int i = 0; i <  totalInstruments; i++) {
            var id = String.format("FUT.%06d", i);
        
            final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
            batch.getMap("tk1:rd:future:"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, "future", 0, 8));
            
            createRandomTimeSeries(id, randomFutureTimeSeriesRecord())
            .stream()
            .forEach(tsRecord -> {
                batch.getMap("tk1:ts:future:"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
            });
            
            lst.add(batch.executeAsync().toCompletableFuture());
            System.out.println("saved TS for "+id);
            if(i%5000==4999){
                System.out.println("flushing batches...");
                CompletableFuture.allOf(lst.toArray(new CompletableFuture[lst.size()])).get();
                lst.clear();
            }
        }
    }

    private void createOptionInstruments(int totalInstruments) throws InterruptedException, ExecutionException {
        var columns = createRandomColumnNames(5); // attribute columns
        var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
        for (int i = 0; i <  totalInstruments; i++) {
            var id = String.format("OPT.%06d", i);

            final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
            batch.getMap("tk1:rd:option:"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, "future", 0, 8));
            
            createRandomTimeSeries(id, randomOptionTimeSeriesRecord())
            .stream()
            .forEach(tsRecord -> {
                batch.getMap("tk1:ts:option:"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
            });
            
            lst.add(batch.executeAsync().toCompletableFuture());
            System.out.println("saved TS for "+id);
            if(i%5000==4999){
                System.out.println("flushing batches...");
                CompletableFuture.allOf(lst.toArray(new CompletableFuture[lst.size()])).get();
                lst.clear();
            }
        }
    }

    private List<String> createRandomColumnNames(int numOfAdditionalColumns) {
        List<String> baseColumns = List.of("bdm", "asOn", "asOf", "id");
        ArrayList<String> columnList = new ArrayList<>(baseColumns);
        for (int i = 0; i < numOfAdditionalColumns; i++) {
            columnList.add(String.format("column_%03d", i));
        }
        return columnList;
    }

    private Map<String, Object> createRandomColumnValues(List<String> columns, String id, String kind, long asOf, int randomStringLength) {
        return columns.stream().collect(Collectors.toMap(
                a -> a, a -> {
                    if (a.equals("id")) {
                        return id;
                    } else if(a.equals("bdm")) {
                        return kind;
                    } else if (a.equals("asOf")) {
                        return Long.valueOf(asOf);
                    } else if (a.equals("asOn")) {
                        return Long.valueOf(System.nanoTime());
                    } else
                        return randomAlphabeticString(randomStringLength);
                }));
    }

    private List<Map<String,Object>> createRandomTimeSeries(String id, String payload) {
        ArrayList<Map<String,Object>> tsList = new ArrayList<>();
        for(int d=0;d<6;d++) {
            for(int h=0;h<24;h++){
                for(int m=0;m<6;m++){
                    var ts = (d*86400000) + (h*3600000) + (m*10*60*1000);
                    tsList.add(Map.of("id", id, "asOf", ts, "payload", payload));
                }
            }
        }
        return tsList;
    }

    private String randomAlphabeticString(int targetStringLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();
        String generatedString = random.ints(leftLimit, rightLimit + 1)
        .limit(targetStringLength)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
        return generatedString;
    }

    private String randomNumericString(int targetStringLength) {
        return String.format("%d",new Random().nextInt(0, (int)Math.pow(10, targetStringLength)));
    }

    private String randomCurve(int items) {
        return "["+Stream.iterate(0,i->i+1).limit(items).map(i->randomNumericString(8)).collect(Collectors.joining(","))+"]";
    }

    private String randomBondTimeSeriesRecord() {
        return "{\"price\":\""+randomNumericString(8)+"\""+
            ",\"ir_curve\":\""+randomCurve(50)+"\""+
            ":,\"credit_curve\":\""+randomCurve(50)+"\"}";
    }

    private String randomOptionTimeSeriesRecord() {
        return "{\"price\":\""+randomNumericString(8)+"\""+
            ",\"offset\":\""+randomNumericString(8)+"\""+
            ",\"ir_curve\":\""+randomCurve(50)+"\""+
            ":,\"volatility\":\""+randomNumericString(8)+"\"}";
    }

    private String randomFutureTimeSeriesRecord() {
        return "{\"price\":\""+randomNumericString(8)+"\"}";
    }
}

@Configuration
@EnableAutoConfiguration
class RefDataRepositoryTest2Configuration {
    @Bean
    public RediSearchClient getRediSearchClient() {
        Config config = new Config();
        config.useSingleServer()
        .setConnectionPoolSize(150)
        .setTimeout(30000)
        .setRetryAttempts(100)
        .setAddress("redis://localhost:6379");
        return new RediSearchClient(config);
    }
}