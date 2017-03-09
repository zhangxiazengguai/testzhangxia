package org.inspur.hdfs.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemUtil {

	public static FileSystem fs = null;

	/**
	 * 获取文件系统
	 * 
	 * @return 文件系统
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	static {
		if (fs == null) {
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
		
			// 找到集群的管理员用户
			// 对应的配置dfs.cluster.administrators.在ambaria界面 Advanced hdfs-site
			String user = "hdfs";
			try {
				fs = FileSystem.get(new URI(uri), conf, user);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 获得指定path下模糊匹配fileName的所有的文件
	 * 
	 * @param path
	 *            路径
	 * @param fileName
	 *            文件名称
	 * @return
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static List<Map> listFileStatuses(String path, String fileName)
			throws IOException, InterruptedException, URISyntaxException {
		List resList = new ArrayList();
		if (path == null || path.equals(""))
			path = "/";
		FileStatus[] statuses;
		if (fileName == null || fileName.equals("")) {
			statuses = fs.listStatus(new Path(path));
		} else {
			MyFilePathFileter pathFileter = new MyFilePathFileter(fileName);
			statuses = fs.listStatus(new Path(path), pathFileter);
		}
		for (FileStatus fileStatus : statuses) {
			Map map = new HashMap();
			map.put("fileName", fileStatus.getPath().getName());
			map.put("permission", fileStatus.getPermission().toString());
			map.put("owner", fileStatus.getOwner());
			map.put("group", fileStatus.getGroup());
			map.put("length", fileStatus.getLen());
			map.put("replication", fileStatus.getReplication());
			map.put("blockSize", fileStatus.getBlockSize());
			map.put("modificationTime",
					format(fileStatus.getModificationTime()));
			map.put("path", fileStatus.getPath().toString());
			map.put("isDirectory", fileStatus.isDirectory());
			resList.add(map);
		}
		return resList;

	}

	/**
	 * 创建目录
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	public static boolean mkdir(String path) throws IOException,
			InterruptedException, URISyntaxException {
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("创建目录成功!");
		} else {
			System.out.println("创建目录失败");
		}
		return isok;
	}

	/**
	 * 创建文件
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	public static boolean createFile(String path) throws IOException,
			InterruptedException, URISyntaxException {
		Path hdfsDir = new Path(path);
		boolean res = fs.createNewFile(hdfsDir);
		if (res)
			System.out.println("创建文件成功.....");
		else System.out.println("创建失败");
		return res;

	}

	/**
	 * 重命名文件
	 * 
	 * @param path1
	 *            旧文件路径
	 * @param path2
	 *            新文件路径
	 * @return
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static boolean rename(String path1, String path2)
			throws IOException, InterruptedException, URISyntaxException {
		boolean res = fs.rename(new Path(path1), new Path(path2));
		if (res)
			System.out.println("重命名文件夹或文件成功.....");
		return res;
	}

	/**
	 * 删除文件,只能删除文件和空的文件夹
	 * 
	 * @param path
	 * @return
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static boolean deleteFile(String path) throws IOException,
			InterruptedException, URISyntaxException {
		boolean result = fs.delete(new Path(path), false);
		if (result)
			System.out.println("删除文件夹或文件成功.....");
		return result;
	}

	/**
	 * 将本地磁盘文件拷贝到hdfs
	 * 
	 * @param localPath
	 *            本地磁盘路径
	 * @param hdfsPath
	 *            hdfs文件路径
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	public static void copyFromLocalFile(String localPath, String hdfsPath)
			throws IllegalArgumentException, IOException, InterruptedException,
			URISyntaxException {
		fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
	}

	/**
	 * 将文件从hdfs拷贝到本地磁盘文件
	 * 
	 * @param localPath
	 *            本地磁盘路径
	 * @param hdfsPath
	 *            hdfs文件路径
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	public static void copyToLocalFile(String localPath, String hdfsPath)
			throws IOException, InterruptedException, URISyntaxException {
		fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
	}

	/**
	 * 读文件
	 * 
	 * @param fileDir
	 */
	public static void readHDFSFile(String fileDir) {
		try {
			Path path = new Path(fileDir);
			if (fs.exists(path)) {
				FSDataInputStream is = fs.open(path);
				FileStatus fileStatus = fs.getFileStatus(path);
				// byte[] buffer = new byte[(int) fileStatus.getLen()];
				byte[] buffer = new byte[is.available()];
				is.readFully(0, buffer);
				String content = new String(buffer);
				System.out.println(content);
				is.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 以输入流方式上传文件到hdfs
	 * 
	 * @param fis
	 * @param path
	 */
	public static void uploadFile(InputStream fis, String path) {
		BufferedInputStream bi = null;
		BufferedOutputStream bo = null;
		FSDataOutputStream os = null;
		try {
			bi = new BufferedInputStream(fis);
			Path p = new Path(path);
			// 同名文件覆盖
			os = fs.create(p, true);
			bo = new BufferedOutputStream(os);
			byte[] buffer = new byte[1024 * 4];
			int size = 0;
			while (true) {
				size = bi.read(buffer);
				if (size == -1) {
					break;
				} else {
					bo.write(buffer, 0, size);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (bo != null) {
					bo.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 格式化时间
	 * 
	 * @param time
	 * @return
	 */
	private static String format(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Timestamp(time));
	}
}
