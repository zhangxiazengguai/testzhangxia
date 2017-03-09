import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;



public class ProxyUserTest {

	public static void main(String[] args) throws IOException {

		String path = "/peixun";
		String fileName = "";
		try {
			
		
			
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

			
			try {
				 
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("joe", UserGroupInformation.getLoginUser());
			    ugi.doAs(new PrivilegedExceptionAction<Void>() {
			      public Void run() throws Exception {
			    	  
			    	
						System.clearProperty("java.security.krb5.conf");
						String krbStr=Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getFile();
						String userInfo="hdfs-test@IDAP.COM";
						//String userInfo="nn/idap-agent-86.idap.com@IDAP.COM";
						String userkeytab=Thread.currentThread().getContextClassLoader().getResource("hdfs.headless.keytab")
			                    .getFile();
	
						// 初始化配置文件
						System.setProperty("java.security.krb5.conf",krbStr);	
						// 使用票据和凭证进行认证(需替换为自己申请的kerberos票据信息)
						UserGroupInformation.setConfiguration(myconf2);
						try{
							sun.security.krb5.Config.refresh();
						}catch (Exception e) {
							
						}
						UserGroupInformation.loginUserFromKeytab(userInfo,userkeytab);	
						
			    	FileSystem  fs2 = FileSystem.get(myconf2);
			    	FileStatus[] statuses=fs2.listStatus(new Path("/"));
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
					}
		
			    	Path hdfsDir = new Path("/12312321321aa3");
					boolean res = fs2.mkdirs(hdfsDir);
					if (res)
						System.out.println("创建文件成功.....");
					else System.out.println("创建失败");
			        return null;
			      }
			    
			     }
			   
			    );  
				 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
