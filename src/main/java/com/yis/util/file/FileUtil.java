package com.yis.util.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * March, or die.
 *
 * @Description:
 * @Created by yisany on 2020/01/13
 */
public class FileUtil {

    private static final Logger logger = LogManager.getLogger(FileUtil.class);

    private static final String LINE_BREAK = System.getProperty("line.separator");
    private static final Long SIZE_BUFFER = 4 * 1024 * 1024L;
    // 缓冲池大小
    private static int bufferSize = 4 * 1024 * 1024;

    private FileUtil() { }

    public static int getBufferSize() {
        return bufferSize;
    }

    public static void setBufferSize(int bufferSize) {
        FileUtil.bufferSize = bufferSize;
    }

    /**
     * 读取文件
     * @param fileDir 文件路径
     * @return
     */
    public static List<String> read(String fileDir) {
        List<String> result = new ArrayList<>();
        RandomAccessFile file = null;
        FileChannel iChannel = null;
        ByteBuffer buf = null;
        try {
            file = new RandomAccessFile(fileDir, "r");
            iChannel = file.getChannel();
            buf = ByteBuffer.allocate(SIZE_BUFFER.intValue());

            while (true) {
                int count = iChannel.read(buf);
                if (count <= -1) {
                    break;
                }
                buf.flip();
                while (buf.hasRemaining()) {
                    char ch = (char) buf.get();
                    result.add(String.valueOf(ch));
                }
                buf.compact();
            }
        } catch (FileNotFoundException e) {
            logger.error("FileUtil.read FileNotFoundException error, e={}", e);
        } catch (IOException e) {
            logger.error("FileUtil.read IOException error, e={}", e);
        } finally {
            try {
                if (file != null) {
                    file.close();
                }
                if (iChannel != null) {
                    iChannel.close();
                }
                if (buf != null) {
                    buf.clear();
                }
            } catch (IOException e) {
                logger.error("FileUtil.read Close resources error, e={}", e);
            }
        }
        return result;
    }

    /**
     * 追加写入文件
     * @param fileDir 文件前置路径
     * @param fileName 文件名
     * @param content 内容
     */
    public static void append(String fileDir, String fileName, String content) {
        String dir;
        FileOutputStream fos = null;
        FileChannel oChannel = null;
        ByteBuffer buf = null;
        if (fileDir.endsWith("/")) {
            dir = fileDir + fileName;
        } else {
            dir = String.format("%s/%s", fileDir, fileName);
        }
        try {
            buf = ByteBuffer.allocate(SIZE_BUFFER.intValue());

            File fileTemp = new File(dir);
            // 判断父文件夹是否存在
            File parent = fileTemp.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            // 判断要写入文件是否存在
            if (!fileTemp.exists()) {
                fileTemp.createNewFile();
            }

            fos = new FileOutputStream(dir, true);


            oChannel = fos.getChannel();

            buf.put(content.getBytes());
            buf.put(LINE_BREAK.getBytes());
            buf.flip();

            while (buf.hasRemaining()) {
                oChannel.write(buf);
            }

            // buf压缩
            buf.compact();
        } catch (IOException e) {
            logger.error("FileUtil.append IOException error, e={}", e);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                if (oChannel != null) {
                    oChannel.close();
                }
                if (buf != null) {
                    buf.clear();
                }
            } catch (IOException e) {
                logger.error("FileUtil.append Close resources error, e={}", e);
            }
        }
    }

    /**
     * 获取所有的文件
     * @param dir 文件夹路径
     * @return
     */
    public static List<String> getFileDir(String dir) {
        return Arrays.stream(new File(dir).listFiles())
                .filter(File::isFile)
                .map(File::getName)
                .collect(Collectors.toList());
    }

    /**
     * 获取当前路径下所有文件夹的绝对路径
     * 暂不支持windows
     * @param dir
     * @param folder
     * @return
     */
    public static List<String> getFolderAll(File dir, List<String> folder) {
        File[] fs = dir.listFiles();
        for (int i = 0; i < fs.length; i++) {
            //若为文件夹，就调用getAllFiles方法
            if (fs[i].isDirectory()) {
                folder.add(fs[i].getAbsolutePath());
                try {
                    getFolderAll(fs[i], folder);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return folder;
    }

}
