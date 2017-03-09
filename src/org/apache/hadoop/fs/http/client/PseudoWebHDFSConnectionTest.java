package org.apache.hadoop.fs.http.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

public class PseudoWebHDFSConnectionTest {

	static PseudoWebHDFSConnection pConn = null;
	
	public static void main(String[] args) {
		try {
			setUpBeforeClass();
			System.out.println(pConn.listStatus("/"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void setUpBeforeClass() throws Exception {
		pConn = new PseudoWebHDFSConnection("http://10.110.13.108:50070", "wesley", "anything");
	}
	

	public void setUp() throws Exception {
		System.out.println(" setUp... per Test ...");
		//pConn = new PseudoWebHDFSConnection("http://api.0.efoxconn.com:14000", "wesley", "anything");
	}

	
	public static void  getHomeDirectory() throws MalformedURLException, IOException, AuthenticationException {
		pConn.getHomeDirectory();
	}
	
	
	
	public void listStatus() throws MalformedURLException, IOException, AuthenticationException {
		String path= "user/wesley";
		pConn.listStatus(path);
	}
	
	
	public void create() throws MalformedURLException, IOException, AuthenticationException {
		String path = "user/wesley/Visual_Paradigm_for_UML_Linux_NoInstall_9_0_20120418.tar.gz";
		FileInputStream is = new FileInputStream(new File("/home/wesley/Visual_Paradigm_for_UML_Linux_NoInstall_9_0_20120418.tar.gz"));
		pConn.create(path, is);
	}
	
	
	//TODO Test Other PseudoWebHDFSConnectionTest's Method

}
