package com.school.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Bonze
 * @create 2020-09-10-9:39
 */
public class HdfsClient{
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");

        // 2 创建目录
        fs.mkdirs(new Path("/input"));

        // 3 关闭资源
        fs.close();
    }


    @Test
    public void test() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration con = new Configuration();
        con.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://master2:9000"),con,"admin");
        //2 上传文件
        fs.copyFromLocalFile(new Path("C:\\Users\\Bonze\\Desktop\\today.txt"), new Path("/test/today.txt"));
        //3 关闭资源
        fs.close();
    }
    @Test
    public void test1() throws IOException, URISyntaxException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");
        //2.执行下载操作
        //boolean delSrc 指是否将源文件删除
        //Path src 指要下载的文件的路径
        //Path dst 指将文件下载到的路径
        //boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false,new Path("/test/today.txt"),new Path("C:\\Users\\Bonze\\Desktop\\newToday.txt"), true);
        //3.关闭资源
        fs.close();
    }
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");

        fs.delete(new Path("/hadoop/mapreduceTest/output"), true);
        fs.close();
    }

    @Test
    public void testRenameFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master2:9000"),configuration, "admin");
        fs.rename(new Path("/test/today.txt"), new Path("/test/tomorrow.txt"));
        fs.close();
    }
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());
            //获取存储的块儿信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                //获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-------------");
        }
        fs.close();
    }

    @Test
    public void testListStatus() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for(FileStatus fs : listStatus){
            if(fs.isFile()){
                System.out.println("file:" + fs.getPath().getName());
            }else{
                System.out.println("file:" + fs.getPath().getName());
            }
        }
    }



    private FileSystem getFileSystem(){
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");
            return fs;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } finally {
            return fs;
        }
    }

    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\Bonze\\Desktop\\today.txt"));
        // 3 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/hadoop/mapreduceTest/input/today.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fis,fos,configuration);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    @Test
    public void readFileSeek1() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.2.zip"));
        FileOutputStream fos = new FileOutputStream(new File("hadoop-2.7.2.zip.pa0rt1"));
        byte[] buf = new byte[1024];
        for(int i = 0; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fileSystem.close();
    }

    @Test
    public void readFileSeek2() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.2.zip"));
        fis.seek(1024*1024*128);
        //4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("hadoop-2.7.2.zip.part2"));
        //5 流的对拷
        IOUtils.copyBytes(fis,fos,new Configuration());
        //6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }



    @Test
    public void getFileFromHDFS() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master2:9000"), configuration, "admin");
        FSDataInputStream fis = fileSystem.open(new Path("/today.txt"));
        FileOutputStream fos = new FileOutputStream("today.txt");
        IOUtils.copyBytes(fis,fos, configuration);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }



}
