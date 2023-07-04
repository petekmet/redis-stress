package com.eurex.r7.notifier.integration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RSearch;
import org.redisson.api.RedissonClient;
import org.redisson.api.SortOrder;
import org.redisson.api.BatchOptions.ExecutionMode;
import org.redisson.api.search.index.FieldIndex;
import org.redisson.api.search.index.IndexType;
import org.redisson.api.search.index.SortMode;
import org.redisson.api.search.query.QueryOptions;
import org.redisson.client.RedisClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootTest(classes = RefDataRepositoryTest2Configuration.class)
public class RediStressTest {
    
    @Autowired
    private RedissonClient client;
    // private static String ADO_ID = "myado3";

    public static String APP_KEY_PREFIX = "tk1:";
    public static String REFD_ENTITY_KEY_PREFIX = "rd:";
    public static final String TS_ENTITY_KEY_PREFIX = "ts:";

    private static final int DAYS_TIMESERIES = 1;
    private static final int HOURS_PER_DAY_TIMESERIES = 24;
    private static final int TICKS_PER_HOUR = 6;
    private static final int TOTAL_TICKS_PER_INSTRUMENT = DAYS_TIMESERIES*HOURS_PER_DAY_TIMESERIES*TICKS_PER_HOUR; 
    private static final int BONDS_INSTRUMENTS = 60000;
    private static final int FUTURES_INSTRUMENTS = 150000;
    private static final int OPTIONS_INSTRUMENTS = 450000;
    
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
            setupIndex2("Bond");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            setupIndex2("Future");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            setupIndex2("Option");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateBondIndex() throws InterruptedException, ExecutionException {
        setupIndex2("Bond");
    }

    @Test
    public void testCreateFutureIndex() throws InterruptedException, ExecutionException {
        setupIndex2("Future");
    }

    @Test
    public void testCreateOptionIndex() throws InterruptedException, ExecutionException {
        setupIndex2("Option");
    }

    @Test
    public void testGenerateBond() throws InterruptedException, ExecutionException {
        createInstruments(BONDS_INSTRUMENTS, "Bond", ()->randomBondTimeSeriesRecord());
    }

    @Test
    public void testGenerateFuture() throws InterruptedException, ExecutionException {
        createInstruments(FUTURES_INSTRUMENTS, "Future", ()->randomFutureTimeSeriesRecord());
    }

    @Test
    public void testGenerateOption() throws InterruptedException, ExecutionException {
        createInstruments(200000, "Option", ()->randomOptionTimeSeriesRecord());
    }

    @Test 
    void testCreateRandomOptionPayload() {
        var payload = randomOptionTimeSeriesRecord();
        System.out.println("Single Option time-series payload bytes: "+payload.length());
        System.out.println(OPTIONS_INSTRUMENTS+"*"+TOTAL_TICKS_PER_INSTRUMENT+"*"+payload.length()+" = "+((long)payload.length())*TOTAL_TICKS_PER_INSTRUMENT*OPTIONS_INSTRUMENTS+" bytes, "+(long)payload.length()*TOTAL_TICKS_PER_INSTRUMENT*OPTIONS_INSTRUMENTS/1024/1024/1024+" GiBytes");
        System.out.println("Payload dump "+payload);
    }

    @Test 
    void testCreateRandomBondPayload() {
        var payload = randomBondTimeSeriesRecord();
        System.out.println("Single Bond time-series payload bytes: "+payload.length());
        System.out.println(BONDS_INSTRUMENTS+"*"+TOTAL_TICKS_PER_INSTRUMENT+"*"+payload.length()+" = "+((long)payload.length())*TOTAL_TICKS_PER_INSTRUMENT*BONDS_INSTRUMENTS+" bytes, "+(long)payload.length()*TOTAL_TICKS_PER_INSTRUMENT*BONDS_INSTRUMENTS/1024/1024/1024+" GiBytes");
        System.out.println("Payload dump "+payload);
    }

    @Test void testCreateRandomFuturePayload() {
        var payload = randomFutureTimeSeriesRecord();
        System.out.println("Single Future time-series payload bytes: "+payload.length());
        System.out.println(FUTURES_INSTRUMENTS+"*"+TOTAL_TICKS_PER_INSTRUMENT+"*"+payload.length()+" = "+((long)payload.length())*TOTAL_TICKS_PER_INSTRUMENT*FUTURES_INSTRUMENTS+" bytes, "+(long)payload.length()*TOTAL_TICKS_PER_INSTRUMENT*FUTURES_INSTRUMENTS/1024/1024+" MiBytes");
        System.out.println("Payload dump "+payload);
    }

    @Test void testLoadBonds() {
        // ft.search refBondIdx "*" sortby id desc limit 0 10000
        // ft.search tsBondIdx "@asOf:[0,0]" sortby id desc limit 0 10000
        search4("refBondIdx", "@bdm:{bond}");
        search4("tsBondIdx", "@asOf:[0,0]");
    }

    @Disabled
    @Test void testLoadBondTsOnly() {
        // ft.search refBondIdx "*" sortby id desc limit 0 10000
        // ft.search tsBondIdx "@asOf:[0,0]" sortby id desc limit 0 10000
        search4("tsBondIdx", "@asOf:[0,0]");
    }

    @Test void testLoadFutures() throws InterruptedException, ExecutionException {
        // ft.search refFutureIdx "*" sortby id desc limit 0 10000
        // ft.search tsFutureIdx "@asOf:[0,0]" sortby id desc limit 0 10000
        search4("refFutureIdx", "@bdm:{future}");
        search4("tsFutureIdx", "@asOf:[0,0]");
    }

    @Test void testLoadFuturesParallel() throws InterruptedException, ExecutionException {
        var cf1 = CompletableFuture.runAsync(()->{
            search4("refFutureIdx", "@bdm:{future}");
        });
        var cf2 = CompletableFuture.runAsync(()->{
            search4("tsFutureIdx", "@asOf:[0,0]");
        });
        CompletableFuture.allOf(cf1, cf2).get();
    }

    @Test void testLoadOptions() throws InterruptedException, ExecutionException {
        // ft.search refOptionIdx "*" sortby id desc limit 0 10000
        // ft.search tsOptionIdx "@asOf:[0,0]" sortby id desc limit 0 10000
        search4("refOptionIdx", "@bdm:{option}");
        search4("tsOptionIdx", "@asOf:[0,0]");
    }

    @Test void testLoadOptionsParallel() throws InterruptedException, ExecutionException {
        // ft.search refOptionIdx "*" sortby id desc limit 0 10000
        // ft.search tsOptionIdx "@asOf:[0,0]" sortby id desc limit 0 10000
        var cf1 = CompletableFuture.runAsync(()->{
            search4("refOptionIdx", "@bdm:{option}");
        });
        var cf2 = CompletableFuture.runAsync(()->{
            search4("tsOptionIdx", "@asOf:[0,0]");
        });
        CompletableFuture.allOf(cf1, cf2).get();
    }

    // private void search(String redisIndex, String redisQuery) {
    //     var then = Instant.now();
    //     SearchOptions searchOptions = new SearchOptions().sort(new SortBy("id", SortOrder.DESC));
    //     var window = 10000;
    //     var count = 
    //     Stream.iterate(0, i->i+window)
    //     .map(pageOffset->{
    //         searchOptions.page(pageOffset, window);
    //         var pageThen = Instant.now();
    //         var result = client.getRediSearch(redisIndex).search(redisQuery, searchOptions);
    //         return result;
    //     })
    //     .takeWhile(searchResult -> searchResult.getDocuments().size()>0)
    //     .flatMap(flattenedResult->{
    //         System.out.println(flattenedResult.getDocuments().size()+"docs");
    //         return flattenedResult.getDocuments().stream();
    //     })
    //     .count();
    //     System.out.println("Total: "+count+", duration "+Duration.between(then, Instant.now()));
    // }

    private void search2(String redisIndex, String redisQuery) {
        QueryOptions options = QueryOptions.defaults()
        .sortBy("id")
        .sortOrder(SortOrder.DESC);
        RSearch search = client.getSearch(StringCodec.INSTANCE);

        var then = Instant.now();
        
        var window = 10000;
        var count = 
        Stream.iterate(0, i->i+window)
        .map(pageOffset->{
            options.limit(pageOffset, window);
            // var pageThen = Instant.now();
            return search.search(redisIndex, redisQuery, options); //getRediSearch(redisIndex).search(redisQuery, searchOptions);
        })
        .takeWhile(searchResult -> searchResult.getDocuments().size()>0)
        .flatMap(flattenedResult->{
            System.out.println(flattenedResult.getDocuments().size()+"docs");
            return flattenedResult.getDocuments().stream();
        })
        .count();
        System.out.println("Total: "+count+", duration "+Duration.between(then, Instant.now()));
    }

    private void search3(String redisIndex, String redisQuery) {
        QueryOptions options = QueryOptions.defaults()
        .sortBy("id")
        .sortOrder(SortOrder.DESC);
        RSearch search = client.getSearch(StringCodec.INSTANCE);

        var then = Instant.now();
        
        var window = 10000;
        var probe = search.search(redisIndex, redisQuery, options.limit(0, 1));
        var total = probe.getTotal();
        var pages = (total+window-1)/window;

        var count = 
        Stream.iterate(0, i->i+window)
        .limit(pages)
        .map(pageOffset->{
            System.out.println("requesting offset "+pageOffset);
            return search.searchAsync(redisIndex, redisQuery, options.limit(pageOffset, window)).toCompletableFuture(); //getRediSearch(redisIndex).search(redisQuery, searchOptions);
        })
        .toList()
        .parallelStream()
        .flatMap(pageFutures -> {
            try {
                return pageFutures.get().getDocuments().stream();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return null;
            } 
        })
        .count();
        System.out.println("Query: "+redisQuery+", results: "+count+", download duration "+Duration.between(then, Instant.now()));
    }

    private void search4(String redisIndex, String redisQuery) {
        QueryOptions options = QueryOptions
        .defaults()
        .sortBy("id")
        .sortOrder(SortOrder.DESC);
        RSearch search = client.getSearch(StringCodec.INSTANCE);

        var then = Instant.now();
        
        var window = 10000;
        var probe = search.search(redisIndex, redisQuery, options.limit(0, 1));
        var total = probe.getTotal();
        var pages = (total+window-1)/window;
        
        var stream = Stream.iterate(0, i -> i+window)
        .limit(pages)
        .parallel()
        .map(pageOffset->{
            System.out.println(Thread.currentThread().getName()+" requesting offset "+pageOffset);
            return search.search(redisIndex, redisQuery, options.limit(pageOffset, window)); //getRediSearch(redisIndex).search(redisQuery, searchOptions);
        })
        .flatMap(result -> result.getDocuments().stream());
        System.out.println("Query: "+redisQuery+", results: "+stream.count()+", download duration "+Duration.between(then, Instant.now()));
    }

    private void setupIndex2(String indexName) throws InterruptedException, ExecutionException {
        client.getSearch().createIndex("ref"+indexName+"Idx", org.redisson.api.search.index.IndexOptions.defaults()
                                 .on(IndexType.HASH)
                                 .prefix(List.of(APP_KEY_PREFIX+REFD_ENTITY_KEY_PREFIX+indexName.toLowerCase()+":")),
                                         FieldIndex.tag("bdm"),
                                         FieldIndex.tag("id"),
                                         FieldIndex.numeric("asOf").sortMode(SortMode.NORMALIZED));
        client.getSearch().createIndex("ts"+indexName+"Idx", org.redisson.api.search.index.IndexOptions.defaults()
                                 .on(IndexType.HASH)
                                 .prefix(List.of(APP_KEY_PREFIX+TS_ENTITY_KEY_PREFIX+indexName.toLowerCase()+":")),
                                         FieldIndex.tag("id"),
                                         FieldIndex.numeric("asOf").sortMode(SortMode.NORMALIZED));
                                         
        }

    private void dropIndex(String indexName) {
        client.getSearch().dropIndex("ref"+indexName+"Idx");
        client.getSearch().dropIndex("ts"+indexName+"Idx");
    }

    // private void createBondInstruments(int totalInstruments) throws InterruptedException, ExecutionException {
    //     var columns = createRandomColumnNames(5); // attribute columns
    //     var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
    //     for (int i = 0; i <  totalInstruments; i++) {
    //         var id = String.format("BON.%06d", i);
            
    //         final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
    //         batch.getMap("tk1:rd:bond:"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, "bond", 0, 8));
            
    //         createRandomTimeSeries(id, ()->randomBondTimeSeriesRecord())
    //         .stream()
    //         .forEach(tsRecord -> {
    //             batch.getMap("tk1:ts:bond:"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
    //         });
            
    //         lst.add(batch.executeAsync().toCompletableFuture());
    //         System.out.println("saved TS for "+id);
    //         if(i%2500==2499){
    //             System.out.println("flushing batches...");
    //             CompletableFuture.allOf(lst.toArray(new CompletableFuture[lst.size()])).get();
    //             lst.clear();
    //         }
            
    //     }
    // }

    // private void createFutureInstruments(int totalInstruments) throws InterruptedException, ExecutionException {
    //     var columns = createRandomColumnNames(5); // attribute columns
    //     var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
    //     for (int i = 0; i <  totalInstruments; i++) {
    //         var id = String.format("FUT.%06d", i);
        
    //         final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
    //         batch.getMap("tk1:rd:future:"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, "future", 0, 8));
            
    //         createRandomTimeSeries(id, ()->randomFutureTimeSeriesRecord())
    //         .stream()
    //         .forEach(tsRecord -> {
    //             batch.getMap("tk1:ts:future:"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
    //         });
            
    //         lst.add(batch.executeAsync().toCompletableFuture());
    //         System.out.println("saved TS for "+id);
    //         if(i%5000==4999){
    //             System.out.println("flushing batches...");
    //             CompletableFuture.allOf(lst.toArray(new CompletableFuture[lst.size()])).get();
    //             lst.clear();
    //         }
    //     }
    // }

    // private void createOptionInstruments2(int totalInstruments) throws InterruptedException, ExecutionException {
    //     var columns = createRandomColumnNames(5); // attribute columns
    //     var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
    //     for (int i = 0; i <  totalInstruments; i++) {
    //         var id = String.format("OPT.%06d", i);

    //         final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
    //         batch.getMap("tk1:rd:option:"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, "future", 0, 8));
            
    //         createRandomTimeSeries(id, ()->randomOptionTimeSeriesRecord())
    //         .stream()
    //         .forEach(tsRecord -> {
    //             batch.getMap("tk1:ts:option:"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
    //         });
            
    //         lst.add(batch.executeAsync().toCompletableFuture());
    //         System.out.println("saved TS for "+id);
    //         if(i%5000==4999){
    //             System.out.println("flushing batches...");
    //             CompletableFuture.allOf(lst.toArray(new CompletableFuture[lst.size()])).get();
    //             lst.clear();
    //         }
    //     }
    // }

    // private void createOptionInstruments(int totalInstruments) throws InterruptedException, ExecutionException {
    //     // createInstruments(totalInstruments, "Option", ()->randomOptionTimeSeriesRecord());
    //     createOptionInstruments2(totalInstruments);
    // }

    private void createInstruments(int totalInstruments, String kind, Supplier<String> tsPayloadSupplier) throws InterruptedException, ExecutionException {
        var columns = createRandomColumnNames(5); // attribute columns
        var lst = new ArrayList<CompletableFuture<BatchResult<?>>>();
        for (int i = 0; i <  totalInstruments; i++) {
            var id = String.format(kind.substring(0,3).toUpperCase()+".%06d", i);

            final RBatch batch = client.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY));
            batch.getMap("tk1:rd:"+kind.toLowerCase()+":"+id, StringCodec.INSTANCE).putAllAsync(createRandomColumnValues(columns, id, kind.toLowerCase(), 0, 8));
            
            createRandomTimeSeries(id, tsPayloadSupplier)
            .stream()
            .forEach(tsRecord -> {
                batch.getMap("tk1:ts:"+kind.toLowerCase()+":"+id+":"+tsRecord.get("asOf"), StringCodec.INSTANCE).putAllAsync(tsRecord);
            });
            
            lst.add(batch.executeAsync().toCompletableFuture());
            System.out.println("scheduled REF+TS to save "+id);
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

    private List<Map<String,Object>> createRandomTimeSeries(String id, Supplier<String> payload) {
        ArrayList<Map<String,Object>> tsList = new ArrayList<>();
        for(int d=0;d<DAYS_TIMESERIES;d++) {
            for(int h=0;h<HOURS_PER_DAY_TIMESERIES;h++){
                for(int m=0;m<TICKS_PER_HOUR;m++){
                    var ts = (d*86400000) + (h*3600000) + (m*10*60*1000);
                    tsList.add(Map.of("id", id, "asOf", ts, "payload", payload.get()));
                }
            }
        }
        return tsList;
    }

    private List<Map<String,Object>> createRandomTimeSeries(String id, String payload) {
        ArrayList<Map<String,Object>> tsList = new ArrayList<>();
        for(int d=0;d<DAYS_TIMESERIES;d++) {
            for(int h=0;h<HOURS_PER_DAY_TIMESERIES;h++){
                for(int m=0;m<TICKS_PER_HOUR;m++){
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
    public RedissonClient getRedisClient() {
        Config config = new Config();
        config.useSingleServer()
        .setConnectionPoolSize(150)
        .setTimeout(30000)
        .setRetryAttempts(100)
        .setAddress("redis://localhost:6379");
        return Redisson.create(config);
    }
}