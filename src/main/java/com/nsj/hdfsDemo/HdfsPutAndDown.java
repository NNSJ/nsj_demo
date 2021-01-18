package com.nsj.hdfsDemo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
/**
 * 测试文件：
 *
 * HDFS文件上传下载实例
 */
public class HdfsPutAndDown {
    public static String HDFSUri = "hdfs://192.168.146.4:8020/";
    private static Logger logger = Logger.getLogger(HdfsPutAndDown.class);
    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    //读取配置文件
//hadoop fs的配置文件
    static Configuration conf = new HdfsConfiguration();
    static{
//指定hadoop fs的地址
//conf.set("fs.default.name", "hdfs://172.16.1.198:9000");
//        conf.addResource(HdfsPutAndDown.class.getClassLoader().getResource("core-site.xml"));
 //       System.out.println(conf.get("ha.zookeeper.quorum"));
//conf.addResource(HdfsPutAndDown.class.getClassLoader().getResource("yarn-site.xml"));
 //       conf.addResource(HdfsPutAndDown.class.getClassLoader().getResource("hdfs-site.xml"));
    }

    public static FileSystem getFileSystem() throws IOException {
        FileSystem fs = FileSystem.get(conf);
// 文件系统
// FileSystem fs = null;
        String hdfsUri = HDFSUri;
        if(StringUtils.isBlank(hdfsUri)){
// 返回默认文件系统 如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                System.out.println(e);
            }
        }else{
// 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
            try {
                URI uri = null;
                try {
                    uri = new URI(hdfsUri.trim());
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
                fs = FileSystem.get(uri,conf);
            } catch (IOException e) {
                logger.error(e);
            }
        }
        return fs;
    }

    // 创建目录
    public static void mkdir(String path) throws IllegalArgumentException, IOException {
// 获取文件系统
        FileSystem fs = getFileSystem();
        String hdfsUri = HDFSUri;
        if(StringUtils.isNotBlank(hdfsUri)){
            path = hdfsUri + path;
        }
// 创建目录
        fs.mkdirs(new Path(path));
//释放资源
        fs.close();
        System.out.println("目录"+ path + "创建成功");
    }
    /**
     * 将本地文件(filePath)上传到HDFS服务器的指定路径(dst)
     * @param filePath
     * @param dst
     * @throws Exception
     */

    public static void uploadFileToHDFS(String filePath,String dst) throws Exception {
//创建一个文件系统
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        Path dstPath = new Path(dst);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start_time = df.parse(df.format(new Date()));
// Long start = System.currentTimeMillis();
        fs.copyFromLocalFile(false, srcPath, dstPath);
//System.out.println("Time:"+ (System.currentTimeMillis() - start));
        Date end_time=df.parse(df.format(new Date()));
        long l= end_time.getTime() - start_time.getTime();
        long day=l/(24*60*60*1000);
        long hour=(l/(60*60*1000)-day*24);
        long min=((l/(60*1000))-day*24*60-hour*60);
        long s=(l/1000-day*24*60*60-hour*60*60-min*60);
        System.out.println("耗时: "+""+day+"天"+hour+"小时"+min+"分"+s+"秒");
        System.out.println("________________________Upload to "+conf.get("fs.default.name")+"________________________");
        fs.close();
        getDirectoryFromHdfs(dst);
    }
    /**
     * 下载文件
     * @param src
     * @throws Exception
     * 这个直接打印结果了，小文件适用
     */
    public static void downLoadFileFromHDFS(String src,String dst) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        InputStream in = fs.open(srcPath);
        try {
//将文件COPY到标准输出(即控制台输出)
//IOUtils.copyBytes(in, System.out, 4096,false);
            fs.copyToLocalFile(false,srcPath, dstPath,true);
        }finally{
            IOUtils.closeStream(in);
            fs.close();
        }
    }
    //遍历hdfs目录下载文件
    public static void getFile(String srcFile,String destPath) throws Exception {
// 源文件路径
        String hdfsUri = null;
        if(StringUtils.isNotBlank(hdfsUri)){
            srcFile = hdfsUri + srcFile;
        }
        Path srcPath = new Path(srcFile);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start_time = df.parse(df.format(new Date()));
// 目的路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/
        Path dstPath = new Path(destPath);
        try {
// 获取FileSystem对象
            FileSystem fs = getFileSystem();
// 下载hdfs上的文件
            fs.copyToLocalFile(false,srcPath, dstPath,true);
            System.out.println("下载成功");
            Date end_time=df.parse(df.format(new Date()));
            long l= end_time.getTime() - start_time.getTime();
            long day=l/(24*60*60*1000);
            long hour=(l/(60*60*1000)-day*24);
            long min=((l/(60*1000))-day*24*60-hour*60);
            long s=(l/1000-day*24*60*60-hour*60*60-min*60);
            System.out.println("耗时: "+""+day+"天"+hour+"小时"+min+"分"+s+"秒");
// 释放资源
            getDirectoryFromHdfs(srcFile);
            fs.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    //test
    public static ArrayList<String> ListFile1(String path) {
        ArrayList<String> files = new ArrayList<String>();
        try {
// 返回FileSystem对象
            FileSystem fs = getFileSystem();
            String hdfsUri = HDFSUri;
            if (StringUtils.isNotBlank(hdfsUri)) {
                path = hdfsUri + path;
            }

            Path path1 = new Path(path);
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(path1,true);
            while (it.hasNext()){
                LocatedFileStatus pathInfo = it.next();
//System.out.println(pathInfo.getPath());
                String path_desc = pathInfo.getPath().toString();
                files.add(path_desc);
            }
// 释放资源
            fs.close();
        } catch ( IOException e) {
            logger.error(e);
        }
        return files;
    }



    // 列出目录
    public static String[] ListFile(String path) {
        String[] files = new String[0];
        try {
// 返回FileSystem对象
            FileSystem fs = getFileSystem();
            String hdfsUri = HDFSUri;
            if(StringUtils.isNotBlank(hdfsUri)){
                path = hdfsUri + path;
            }
            FileStatus[] status;
// 列出目录内容
            status = fs.listStatus(new Path(path));
// 获取目录下的所有文件路径
            Path[] listedPaths = org.apache.hadoop.fs.FileUtil.stat2Paths(status);

// 转换String[]
            if (listedPaths != null && listedPaths.length > 0){
                files = new String[listedPaths.length];
                for (int i = 0; i < files.length; i++){
                    files[i] = listedPaths[i].toString();
                }
            }
// 释放资源
            fs.close();
        } catch ( IOException e) {
            logger.error(e);
        }

        return files;
    }
    /**
     * 遍历指定目录(direPath)下的所有文件
     * @param direPath
     * @throws Exception
     */
    public static void getDirectoryFromHdfs(String direPath) throws Exception{

        FileSystem fs = FileSystem.get(URI.create(direPath),conf);
        FileStatus[] filelist = fs.listStatus(new Path(direPath));
        for (int i = 0; i < filelist.length; i++) {
            System.out.println("_________________***********************____________________");
            FileStatus fileStatus = filelist[i];
            System.out.println("Name:"+fileStatus.getPath().getName());
            System.out.println("size:"+fileStatus.getLen());
            System.out.println("_________________***********************____________________");
        }
        fs.close();
    }
    /**
     * 测试方法
     * @param args
     */
    public static void main(String[] args) {
        try {
// getDirectoryFromHdfs("/hbase/");
// SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
//System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
            /*批量上传文件到大数据平台
             **t
             */
// mkdir("/data");
uploadFileToHDFS("C:\\a.txt", "/data");
//下载
   //         String destPath = "D:\\download";
    //        String srcFile = "/data/浅剖/XTL06";
     //       getFile(srcFile,destPath);
/* String destPath = "/home";
String List_path = "/data/multi-demp/XL01";
ArrayList<String> files = ListFile1(List_path);
for(String s: files) {
System.out.println(s);
getFile(s,destPath);
}*/

// getDirectoryFromHdfs("/data/multi-demp/XL01/all");

        } catch (Exception e) {
// TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }
}
