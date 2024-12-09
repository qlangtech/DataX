package com.alibaba.datax.plugin.reader.mongodbreader;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.IMongoTable;
import com.alibaba.datax.plugin.reader.mongodbreader.util.IMongoTableFinder;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

//import com.mongodb.MongoClient;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init() {

            this.originalConfig = super.getPluginJobConf();

            IDataSourceFactoryGetter dsGetter =
                    IDataSourceFactoryGetter.getReaderDataSourceFactoryGetter(this.originalConfig,
                            this.containerContext);

            try {

                this.mongoClient = dsGetter.getDataSourceFactory().unwrap(MongoClient.class);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            //            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME,
            //                    originalConfig.getString(KeyConstant.MONGO_USERNAME));
            //            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD,
            //                    originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            //            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME,
            //                    originalConfig.getString(KeyConstant.MONGO_DATABASE));
            //            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            //            if (!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
            //                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig, userName,
            //                password, authDb);
            //            } else {
            //                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            //            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;
        protected IMongoTableFinder mongoTableFinder;
        protected IMongoTable mongoTable;

        private String userName = null;
        private String password = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;
        private DataXCol2Index col2IndexMapper;


        @Override
        public void startRead(RecordSender recordSender) {

            if ( //lowerBound == null || upperBound == null || mongoClient == null || database == null || collection
                // == null || mongodbColumnMeta == null
                    mongoClient == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Document filter = mongoTable.getCollectionQueryFilter();// new Document();

            dbCursor = col.find(filter).iterator();
            Document item = null;
            // Record record = null;
            Objects.requireNonNull(this.col2IndexMapper, "col2IndexMapper can not be null");
            while (dbCursor.hasNext()) {
                item = dbCursor.next();
                recordSender.sendToWriter(this.mongoTable.convert2RecordByItem(recordSender.createRecord(this.col2IndexMapper), item));
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            //            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME,
            //                    readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            //            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD,
            //                    readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            //            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            //            if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
            //                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName,
            //                password, authDb);
            //            } else {
            //                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            //            }
            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            if (StringUtils.isEmpty(this.collection)) {
                throw new IllegalStateException("param " + KeyConstant.MONGO_COLLECTION_NAME + " must be present");
            }
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);


            IDataxReader dataxReader = this.loadDataXReader();
            ISelectedTab selectedTab = dataxReader.getSelectedTab(this.collection);
            this.col2IndexMapper = DataXCol2Index.getCol2Index(this.containerContext.getTransformerBuildCfg(), new RunningContext() {
            }, selectedTab.getCols());


            if (!(dataxReader instanceof IMongoTableFinder)) {
                throw new IllegalStateException("dataReader:" + dataxReader.getClass().getName() + " must be type of "
                        + IMongoTableFinder.class.getName());
            }
            this.mongoTableFinder = (IMongoTableFinder) dataxReader;
            this.mongoTable = this.mongoTableFinder.findMongoTable(this.collection);
            try {
                IDataSourceFactoryGetter dsGetter = IDataSourceFactoryGetter.getReaderDataSourceFactoryGetter( //
                        this.readerSliceConfig, this.containerContext);
                this.mongoClient = dsGetter.getDataSourceFactory().unwrap(MongoClient.class);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }


        }



        @Override
        public void destroy() {
            this.mongoClient.close();
        }

    }
}
