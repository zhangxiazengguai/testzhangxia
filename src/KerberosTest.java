import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.net.PrintCommandListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;



public class KerberosTest {

	public static void main(String[] args) throws IOException {

		try {
					
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
			
			
			System.clearProperty("java.security.krb5.conf");
			/*String krbStr="/etc/krb5.conf";
			String userInfo="hdfs-test@IDAP.COM";
			String userkeytab="/etc/security/keytabs/hdfs.headless.keytab";*/
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
	    	//System.out.println(statuses);
	    	Path hdfsDir = new Path("/123123213213");
			//boolean res = fs2.mkdirs(hdfsDir);
			//if (res)
			//	System.out.println("创建文件成功.....");
			//else System.out.println("创建失败");
		
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
