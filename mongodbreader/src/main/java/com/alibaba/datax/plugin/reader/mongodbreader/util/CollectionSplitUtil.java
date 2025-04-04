package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 * Modified by baisui(baisui@qlangtech.com) on 2024/12/10
 */
public class CollectionSplitUtil {

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId);
        for (Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }


    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        if (col.estimatedDocumentCount() < 1) {
            throw new IllegalStateException("collection:" + collName + " have any doc ");
        }
        Document doc = col.find().limit(1).first();
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        if (id instanceof ObjectId) {
            return true;
        }
        return false;
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId) {

        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            return Arrays.asList(range);
        }

        BsonDocument result = database.runCommand(new Document("collStats", collName), BsonDocument.class);
        int docCount = result.getInt32("count").intValue();
        //  int docCount = result.getInteger("count");
        if (docCount == 0) {
            return rangeList;
        }
        int avgObjSize = 1;
        BsonValue avgObjSizeObj = result.get("avgObjSize");
        // avgObjSize = avgObjSizeObj.asInt32().intValue();
        if (avgObjSizeObj.isNumber()) {
            if (avgObjSizeObj.isInt32()) {
                avgObjSize = avgObjSizeObj.asInt32().intValue();
            } else if (avgObjSizeObj.isInt64()) {
                avgObjSize = avgObjSizeObj.asInt64().intValue();
            } else if (avgObjSizeObj.isDouble()) {
                avgObjSize = avgObjSizeObj.asDouble().intValue();
            } else {
                throw new IllegalStateException("illegal avgObjSizeObj value:" + avgObjSizeObj);
            }
        } else {
            throw new IllegalStateException("illegal avgObjSizeObj type:" + avgObjSizeObj.getBsonType());
        }
        int splitPointCount = adviceNumber - 1;
        int chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<Object>();

        // test if user has splitVector role(clusterManager)
        boolean supportSplitVector = true;
        try {
            database.runCommand(new Document("splitVector", dbName + "." + collName)
                    .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                    .append("force", true));
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == KeyConstant.MONGO_UNAUTHORIZED_ERR_CODE ||
                    e.getErrorCode() == KeyConstant.MONGO_ILLEGALOP_ERR_CODE) {
                supportSplitVector = false;
            }
        }

        if (supportSplitVector) {
            final boolean forceMedianSplit = true;
            int maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
            //int maxChunkSize = (chunkDocCount - 1) * 2 * avgObjSize / (1024 * 1024);
            if (maxChunkSize < 1) {
                // forceMedianSplit = true;
            }
            if (!forceMedianSplit) {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("maxChunkSize", maxChunkSize)
                        .append("maxSplitPoints", adviceNumber - 1), BsonDocument.class);
            } else {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("force", true), BsonDocument.class);
            }
            BsonArray splitKeys = result.getArray("splitKeys");

            for (int i = 0; i < splitKeys.size(); i++) {
                BsonDocument splitKey = splitKeys.get(i).asDocument();
                BsonValue id = splitKey.get(KeyConstant.MONGO_PRIMARY_ID);
                if (id.isObjectId()) {
                    ObjectId oid = id.asObjectId().getValue();
                    splitPoints.add(oid.toHexString());
                } else if (id.isString()) {
                    splitPoints.add(id.asString().getValue());
                }
            }
        } else {
            int skipCount = chunkDocCount;
            MongoCollection<Document> col = database.getCollection(collName);

            for (int i = 0; i < splitPointCount; i++) {
                Document doc = col.find().skip(skipCount).limit(chunkDocCount).first();
                Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId) id;
                    splitPoints.add(oid.toHexString());
                } else {
                    splitPoints.add(id);
                }
                skipCount += chunkDocCount;
            }
        }

        Object lastObjectId = "min";
        for (Object splitPoint : splitPoints) {
            Range range = new Range();
            range.lowerBound = lastObjectId;
            lastObjectId = splitPoint;
            range.upperBound = lastObjectId;
            rangeList.add(range);
        }
        Range range = new Range();
        range.lowerBound = lastObjectId;
        range.upperBound = "max";
        rangeList.add(range);

        return rangeList;
    }
}

class Range {
    Object lowerBound;
    Object upperBound;
}
