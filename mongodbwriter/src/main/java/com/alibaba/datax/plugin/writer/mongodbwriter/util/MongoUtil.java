package com.alibaba.datax.plugin.writer.mongodbwriter.util;

import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class MongoUtil {

//    public static MongoClient initMongoClient(Configuration conf) {
//
//        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
//        if(addressList == null || addressList.size() <= 0) {
//            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
//        }
//        try {
//            return new MongoClient(parseServerAddress(addressList));
//        } catch (UnknownHostException e) {
//           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
//        } catch (NumberFormatException e) {
//           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
//        } catch (Exception e) {
//            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
//        }
//    }

//    public static MongoClient initCredentialMongoClient(Configuration conf,String userName,String password,String database) {
//
//        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
//        if(!isHostPortPattern(addressList)) {
//            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
//        }
//        try {
//            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
//            return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential));
//
//        } catch (UnknownHostException e) {
//            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
//        } catch (NumberFormatException e) {
//            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
//        } catch (Exception e) {
//            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
//        }
//    }
    /**
     * 判断地址类型是否符合要求
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<Object> addressList) {
        for(Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if(!((String)address).matches(regex)) {
                return false;
            }
        }
        return true;
    }
    /**
     * 转换为mongo地址协议
     * @param rawAddressList
     * @return
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList) throws UnknownHostException{
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        for(Object address : rawAddressList) {
            String[] tempAddress = ((String)address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0],Integer.valueOf(tempAddress[1]));
                addressList.add(sa);
            } catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }

    public static void main(String[] args) {
        try {
            ArrayList hostAddress = new ArrayList();
            hostAddress.add("127.0.0.1:27017");
            System.out.println(MongoUtil.isHostPortPattern(hostAddress));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
