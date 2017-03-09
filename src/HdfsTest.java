import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;



public class HdfsTest {

	public static void main(String[] args) throws IOException {

		String path = "/peixun";
		String fileName = "";
		try {
			// 测试查询文件列表
			
			Configuration conf = new Configuration();
			String uri = "hdfs://jszxcluster";
			conf.set("fs.defaultFS", uri);
			// 在hdfs-site.xml文件中可以找到相关的
			conf.set("dfs.nameservices", "jszxcluster");
			conf.set("dfs.ha.namenodes.jszxcluster", "nn1,nn2");
			conf.set("dfs.namenode.rpc-address.jszxcluster.nn1",
					"idap-agent-238.idap.com:8020");
			conf.set("dfs.namenode.rpc-address.jszxcluster.nn2",
					"idap-agent-242.idap.com:8020");
			conf.set("dfs.client.failover.proxy.provider.jszxcluster",
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
			
			final Configuration myconf = new Configuration();
			String myuri = "hdfs://cluster-test";
			myconf.set("fs.defaultFS", myuri);
			// 在hdfs-site.xml文件中可以找到相关的
			myconf.set("dfs.nameservices", "cluster-test");
			myconf.set("dfs.ha.namenodes.cluster-test", "fx108,fx9");
			myconf.set("dfs.namenode.rpc-address.cluster-test.fx108",
					"fx108:8020");
			myconf.set("dfs.namenode.rpc-address.cluster-test.fx9",
					"fx9:8020");
			myconf.set("dfs.client.failover.proxy.provider.cluster-test",
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
			
			final Configuration myconf2 = new Configuration();
			String myuri2 = "hdfs://testcluster";
			myconf2.set("fs.defaultFS", myuri2);
			// 在hdfs-site.xml文件中可以找到相关的
			myconf2.set("dfs.nameservices", "testcluster");
			myconf2.set("dfs.ha.namenodes.testcluster", "nn1,nn2");
			myconf2.set("dfs.namenode.rpc-address.testcluster.nn1",
					"idap-agent-85.idap.com:8020");
			myconf2.set("dfs.namenode.rpc-address.testcluster.nn2",
					"idap-agent-86.idap.com:8020");
			myconf2.set("dfs.client.failover.proxy.provider.testcluster",
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
			myconf2.set("hadoop.security.authentication","kerberos");
			myconf2.set("hadoop.security.authorization","true");
			// 找到集群的管理员用户
			// 对应的配置dfs.cluster.administrators.在ambaria界面 Advanced hdfs-site
			String user = "zhangxia";
			FileSystem fs = null;		
			
			try {
				 long start=System.currentTimeMillis();
				
				 fs = FileSystem.get(new URI(myuri), myconf,"root");
				 System.out.println(System.currentTimeMillis()-start);
				 //设置配额
				/* Path path2=new Path("/123");
				((DistributedFileSystem)fs).setQuota(path2, 9999999, -1);				
				 ContentSummary c = fs.getContentSummary(path2);
				 System.out.println(c.getSpaceQuota());
				 System.out.println(c.getLength());
				 System.out.println(c.getQuota());*/
				 //文件个数
				 //QuotaUsage quotaUsage = ((FileSystem)fs).getgetQuotaUsage();
				 //输出/目录的所有信息
				 /*FileStatus[] statuses=fs.listStatus(new Path("/"));
				 for (FileStatus fileStatus : statuses) {
						Map map = new HashMap();
						map.put("fileName", fileStatus.getPath().getName());
						map.put("permission", fileStatus.getPermission().toString());
						map.put("owner", fileStatus.getOwner());
						map.put("group", fileStatus.getGroup());
						map.put("length", fileStatus.getLen());
						map.put("replication", fileStatus.getReplication());
						map.put("blockSize", fileStatus.getBlockSize());
						map.put("modificationTime",fileStatus.getModificationTime());
						map.put("path", fileStatus.getPath().toString());
						map.put("isDirectory", fileStatus.isDirectory());
						System.out.println(map);
					}*/
				 //创建目录
				/* Path path3 = new Path("/123/234");
				 boolean isSuccess = fs.mkdirs(path3);
				 System.out.println(isSuccess);
				 */
				 //删除目录
				 /*Path path4 = new Path("/123/234");
				 boolean isSuccess = fs.delete(path4);
				 System.out.println(isSuccess);*/
				 //重命名
				/* boolean res = fs.rename(new Path("/111"), new Path("/movies/111"));
				   if (res)
						System.out.println("重命名文件夹或文件成功.....");*/
				 //判断路径是否存在
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*FileStatus[] statuses = fs.listStatus(new Path("/"));
			List<Map> fileList = FileSystemUtil
					.listFileStatuses("/", fileName);
			System.out.println("目录" + path + "下的文件列表如下：" + fileList);

			// 测试创建目录
			String newDirPath = "/peixun/newDir";
			boolean result = FileSystemUtil.mkdir(newDirPath);

			// 测试创建文件
			String newFilePath ="/zhangxia";
			Path hdfsDir = new Path(newFilePath);
			boolean res = fs.mkdirs(hdfsDir);
			if (res)
				System.out.println("创建文件成功.....");
			else System.out.println("创建失败");*/
			
			/*//测试重命名文件
			String oldFilePath="/peixun/newFile";
			String newFilePath ="/peixun/newName";
			boolean result2 =FileSystemUtil.rename(oldFilePath, newFilePath);*/
			
		    /*//测试删除文件
			String deleteFilePath = "/peixun/newName";
			boolean result3 = FileSystemUtil.deleteFile(deleteFilePath);*/	
			
			/*//测试删除目录
			String deleteDirPath ="/peixun/newDir";
			boolean result4 = FileSystemUtil.deleteFile(deleteDirPath);*/
			
			/*//测试将文件从本地磁盘拷贝到hdfs
			String localPath = "C:\\Users\\huxiaoqing\\Desktop\\hbase-client-1.1.1.jar";
			String hdfsPath ="/peixun";
			FileSystemUtil.copyFromLocalFile(localPath, hdfsPath);*/
			
			/*//测试读文件
			String fileDir ="/peixun/file1.txt";
			FileSystemUtil.readHDFSFile(fileDir);*/
			
			/*//测试以输入流方式上传文件到hdfs
			String uploadPath="/peixun/uploadfile";
			File localFile=new File("C:\\Users\\huxiaoqing\\Desktop\\file2.txt");
			FileInputStream in = new FileInputStream(localFile);
			FileSystemUtil.uploadFile(in, uploadPath);
			in.close();*/
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
