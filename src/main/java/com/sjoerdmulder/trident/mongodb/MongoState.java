package com.sjoerdmulder.trident.mongodb;

import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.marshall.jackson.JacksonProcessor;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * MongoDB State for Trident
 * @author Sjoerd Mulder (http://github.com/sjoerdmulder)
 * @param <T> POJO class it should encapsulate
 */
public class MongoState<T> implements IBackingMap<T> {

    private Class<T> type;
    private MongoCollection collection;

    public static class Options implements Serializable {
        int localCacheSize = 1000;
        String globalKey = "$GLOBAL$";
    }

    public static <T> StateFactory opaque(String mongoUri, Class<T> entityClass) {
        return opaque(mongoUri, entityClass, new Options());
    }

    public static <T> StateFactory opaque(String mongoUri, Class<T> entityClass, Options opts) {
        return new Factory<T>(mongoUri, StateType.OPAQUE, entityClass, opts);
    }

    public static <T> StateFactory transactional(String mongoUri, Class<T> entityClass) {
        return transactional(mongoUri, entityClass, new Options());
    }

    public static <T> StateFactory transactional(String mongoUri, Class<T> entityClass, Options opts) {
        return new Factory<T>(mongoUri, StateType.TRANSACTIONAL, entityClass, opts);
    }

    public static <T> StateFactory nonTransactional(String mongoUri, Class<T> entityClass) {
        return nonTransactional(mongoUri, entityClass, new Options());
    }

    public static <T> StateFactory nonTransactional(String mongoUri, Class<T> entityClass, Options opts) {
        return new Factory<T>(mongoUri, StateType.NON_TRANSACTIONAL, entityClass, opts);
    }

    protected static class Factory<T> implements StateFactory {

        private final StateType type;
        private final String mongoUri;
        private final Class<T> entityClass;
        private final Options opts;
        private MongoCollection collection;

        public Factory(String mongoUri, StateType type, Class<T> entityClass, Options opts) {
            this.type = type;
            this.mongoUri = mongoUri;
            this.entityClass = entityClass;
            this.opts = opts;
        }

        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            MongoURI mongoURI = new MongoURI(mongoUri);
            Mongo mongo;
            try {
                mongo = new Mongo(mongoURI);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            JacksonProcessor processor = new JacksonProcessor() {
                @Override
                public <T> T unmarshall(String json, Class<T> clazz) {
                    if (TransactionalValue.class.equals(clazz) || OpaqueValue.class.equals(clazz)) {
                        return (T) super.unmarshall(json, entityClass);
                    }
                    return super.unmarshall(json, clazz);
                }
            };
            Jongo jongo = new Jongo(mongo.getDB(mongoURI.getDatabase()), processor, processor);
            collection = jongo.getCollection(mongoURI.getCollection());

            MapState<T> mapState;
            switch (type) {
                case NON_TRANSACTIONAL:
                    mapState = buildNonTransactional();
                    break;
                case TRANSACTIONAL:
                    collection.getDBCollection().setWriteConcern(WriteConcern.SAFE);
                    mapState = buildTransactional();
                    break;
                case OPAQUE:
                    mapState = buildOpaque();
                    break;
                default:
                    throw new RuntimeException("Unknown state type: " + type);
            }

            return new SnapshottableMap<T>(mapState, Arrays.<Object>asList(opts.globalKey));
        }

        private MapState<T> buildTransactional() {
            MongoState<TransactionalValue> state = new MongoState<TransactionalValue>(collection, TransactionalValue.class);
            CachedMap<TransactionalValue> cachedMap = new CachedMap<TransactionalValue>(state, opts.localCacheSize);
            return TransactionalMap.build(cachedMap);
        }

        private MapState<T> buildOpaque() {
            MongoState<OpaqueValue> state = new MongoState<OpaqueValue>(collection, OpaqueValue.class);
            CachedMap<OpaqueValue> cachedMap = new CachedMap<OpaqueValue>(state, opts.localCacheSize);
            return OpaqueMap.build(cachedMap);
        }

        private MapState<T> buildNonTransactional() {
            MongoState<T> state = new MongoState<T>(collection, entityClass);
            CachedMap<T> cachedMap = new CachedMap<T>(state, opts.localCacheSize);
            return NonTransactionalMap.build(cachedMap);
        }
    }

    protected MongoState(MongoCollection collection, Class<T> type) {
        this.collection = collection;
        this.type = type;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> returns = new ArrayList<T>(keys.size());
        for (List<Object> key : keys) {
            T result = collection.findOne("{_id: #}", key.get(0)).as(type);
            if (result != null) {
                if (TransactionalValue.class.equals(type)) {
                    MongoValueResult value = (MongoValueResult) result;
                    result = (T) new TransactionalValue<T>(value.getTxid(), result);
                } else if (OpaqueValue.class.equals(type)) {
                    MongoValueResult value = (MongoValueResult) result;
                    result = (T) new OpaqueValue<T>(value.getCurrentTxId(), result, (T) value.getPrevious());
                }
            }
            returns.add(result);
        }
        return returns;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        for (T value : vals) {
            if (TransactionalValue.class.equals(type)) {
                TransactionalValue<T> transactionalValue = (TransactionalValue<T>) value;
                value = transactionalValue.getVal();
                ((MongoValueResult) value).setTxId(transactionalValue.getTxid());
            } else if (OpaqueValue.class.equals(type)) {
                OpaqueValue<T> opaqueValue = (OpaqueValue<T>) value;
                MongoValueResult<T> mongoValue = (MongoValueResult<T>) opaqueValue.getCurr();
                mongoValue.setCurrentTxId(opaqueValue.getCurrTxid());
                if (opaqueValue.getPrev() != null) {
                    MongoValueResult previous = ((MongoValueResult) opaqueValue.getPrev());
                    previous.setPrevious(null);
                    if (!previous.equals(mongoValue)) {
                        mongoValue.setPrevious((T) previous);
                    }
                }
                value = (T) mongoValue;
            }
            collection.save(value);
        }
    }

}
