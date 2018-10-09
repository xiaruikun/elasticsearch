package com.xc.demo;

  
import java.io.DataInputStream;  
import java.io.DataOutputStream;  
import java.io.File;  
import java.io.FileOutputStream;  
import java.io.IOException;  
import java.net.HttpURLConnection;  
import java.net.URL;  
  
public class tianditu {  
  
    /** 
     * 远程文件下载 
     * @param url 下载地址 
     * @param file 保存文件地址 
     */  
	public static boolean download(URL url, File file) throws IOException {  
        boolean flag = true;  
        DataOutputStream dos = null;  
        DataInputStream dis = null;  
        try {  
            if(!file.getParentFile().exists()) file.getParentFile().mkdirs();  
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();  
            dos = new DataOutputStream(new FileOutputStream(file));  
            dis = new DataInputStream(conn.getInputStream());  
            byte[] data = new byte[2048];  
            int i = 0;  
            while ((i = dis.read(data)) != -1) {  
                dos.write(data, 0, i);  
            }  
            dos.flush();  
        } catch (IOException e) {  
            flag = false;  
            throw e;  
        } finally {  
            if(dis != null) dis.close();  
            if(dos != null) dos.close();  
        }  
        return flag;  
    }  
  
    /** 
     * 计算分辨率 
     * @param maxLevel 最大级别 
     */  
    public static double[] getResolutions(int maxLevel){  
        double max = 360.0/256.0;  
        double[] resolutions = new double[maxLevel+1];  
        for(int z=0;z<=maxLevel;z++) resolutions[z] = max/Math.pow(2, z);  
        return resolutions;  
    }  
      
    public static void main(String[] arg) throws IOException{  
        double[] re = getResolutions(18);  
        double[] extent = {106.54,21.59,108.26,23.20};  
          
        for(int z=0;z<18;z++){  
        	
        	int totalX = (int)Math.ceil(360.0/(re[z]*256.0)); //列数向上取整  
            int totalY = (int)Math.ceil(180.0/(re[z]*256.0)); //行数向上取整 */     
              
            //起始结束列  
            int sX = (int)Math.floor(((extent[0] + 180) / 360) * totalX);  
            int eX = (int)Math.floor(((extent[2] + 180) / 360) * totalX);  
  
            //起始结束行  
            int sY = (int)Math.floor(((90 - extent[3]) / 180) * totalY);  
            int eY = (int)Math.floor(((90 - extent[1]) / 180) * totalY);  
              
            for(int y=sY;y<=eY;y++){  
                for(int x=sX;x<=eX;x++){  
                    String urlstr = "http://t0.tianditu.com/DataServer?T=vec_c&x="+x+"&y="+y+"&l="+z; //天地图服务器t0-t8间选一个  
                    System.out.println(urlstr);  
                    String path = "D:/tiandituwapian/"+z+File.separator+y+File.separator+x+".png";  
                    File file = new File(path);  
                    URL url = new URL(urlstr);  
                    download(url,file);  
                }  
            }  
              
              
        }  
    }  
}  
