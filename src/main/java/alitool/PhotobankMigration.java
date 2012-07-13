package alitool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

/**
 * @author Leo Young Jul 10, 2012
 */
public class PhotobankMigration {
	public static final Logger mainLog = Logger.getLogger("mainLogger");
	public static final Logger migrationLog = Logger.getLogger("migrationLogger");
	private static String PREFIX_OLD = "/Users/leo/inc-workspace/PhotobankMigration/oldrepo/";// "/mnt/photobank_repository/";
	private static String PREFIX_NEW = "/Users/leo/inc-workspace/PhotobankMigration/newrepo/"; // "/mnt/photobank_repository1/";

	public static void main(String[] args) {
		mainLog.info("starting PhotobankMigration job.");
		// get the id/name pair
		ParameterInfo parameterInfo = getParameterInfo(args);
		mainLog.debug(parameterInfo);
		if (parameterInfo == null) {
			// TODO print usage
			return;
		}
		final int size = parameterInfo.getCopyThreads();
		final List<String> filenameList = parameterInfo.getDumpFiles();
		final BlockingQueue<Integer> copyFinishedBlockingQueue = new LinkedBlockingQueue<Integer>();
		final ExecutorService copyExecutorService = Executors.newFixedThreadPool(size);
		final ExecutorService futureExecutorService = Executors.newFixedThreadPool(size);

		if (mainLog.isDebugEnabled()) {
			Thread monitorQueue = new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(1000);
							mainLog.debug("copyFinishedBlockingQueue size: " + copyFinishedBlockingQueue.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			});
			monitorQueue.setDaemon(true);
			monitorQueue.start();
		}

		for (String filename : filenameList) {
			try {
				File dumpFile = new File(filename);
				if (!dumpFile.exists()) {
					mainLog.warn("no dumpfile: " + dumpFile.getAbsolutePath());
					continue;
				}
				BufferedReader in = new BufferedReader(new FileReader(dumpFile));
				String line;
				int queueSizeHolder = size;
				while ((line = in.readLine()) != null) {
					mainLog.debug("initThreadCount: " + queueSizeHolder);
					if (queueSizeHolder > 0) {
						queueSizeHolder--;
					} else {
						copyFinishedBlockingQueue.take();
					}
					File[] files = needMigration(line);
					if (files != null) {
						Future<?> future = copyExecutorService.submit(new CopyTask(files[0], files[1]));
						futureExecutorService.execute(new CopyFutureTask(future, copyFinishedBlockingQueue));
					} else {
						queueSizeHolder++;
					}
				}
				copyExecutorService.shutdown();
				futureExecutorService.shutdown();
				in.close();
			} catch (Exception e) {
				mainLog.error(e);
			}
		}
		PhotobankMigration.mainLog.info("PhotobankMigration job done.");
	}

	private static File[] needMigration(String line) {
		String relativePath = MigrationUtils.getRelativePathInfo(line);
		// if need migration & if old file exist & if new file not exist, copy file
		if (!MigrationUtils.isHashNeedMigration(relativePath)) {
			mainLog.debug("relativePath: " + relativePath + ", NOT migration.");
			return null;
		}
		File oldFile = new File(PREFIX_OLD + relativePath);
		File newFile = new File(PREFIX_NEW + relativePath);
		synchronized (PhotobankMigration.mainLog) {
			if (!oldFile.exists() || oldFile.isDirectory() || newFile.exists()) {
				mainLog.warn("relativePath: " + relativePath + ", NEED migration, BUT: !oldFile.exists(): "
						+ !oldFile.exists() + ", oldFile.isDirectory(): " + oldFile.isDirectory()
						+ ", newFile.exists(): " + newFile.exists());
				return null;
			}
		}
		mainLog.debug("need migration file: " + oldFile.getAbsolutePath());
		return new File[] { oldFile, newFile };
	}

	private static ParameterInfo getParameterInfo(String[] args) {
		ParameterInfo parameterInfo = new ParameterInfo();
		if (args == null || args.length < 2) {
			return null;
		}
		List<String> list = new LinkedList<String>();
		for (String arg : args) {
			if (NumberUtils.isNumber(arg)) {
				parameterInfo.setCopyThreads(Integer.valueOf(arg));
			} else {
				list.add(arg);
			}
		}
		parameterInfo.setDumpFiles(list);
		return parameterInfo;
	}
}

class ParameterInfo {
	private List<String> dumpFiles = new LinkedList<String>();
	private int copyThreads;

	public List<String> getDumpFiles() {
		return dumpFiles;
	}

	public void setDumpFiles(List<String> dumpFiles) {
		this.dumpFiles = dumpFiles;
	}

	public int getCopyThreads() {
		return copyThreads;
	}

	public void setCopyThreads(int copyThreads) {
		this.copyThreads = copyThreads;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("copy threads: " + copyThreads + ", dumpFiles: ");
		for (String file : dumpFiles) {
			sb.append(file).append("; ");
		}
		return sb.toString();
	}
}

class CopyTask implements Runnable {
	private File oldFile;
	private File newFile;

	public CopyTask(File oldFile, File newFile) {
		this.oldFile = oldFile;
		this.newFile = newFile;
	}

	@Override
	public void run() {
		PhotobankMigration.mainLog.debug("copy tast start running for: " + newFile.getAbsolutePath());
		
		synchronized (PhotobankMigration.mainLog) {
			if (!newFile.getParentFile().exists()) {
				newFile.getParentFile().mkdirs();
			}
		}
		FileChannel in = null, out = null;
		try {
			in = new FileInputStream(oldFile).getChannel();
			out = new FileOutputStream(newFile).getChannel();
			in.transferTo(0, in.size(), out);
		} catch (Exception e) {
			PhotobankMigration.mainLog.error(e);
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
				PhotobankMigration.mainLog.error(e);
			}
		}
		// log original file path to migration.log, for future delete
		PhotobankMigration.migrationLog.info(oldFile.getAbsolutePath());
	}
}

class CopyFutureTask implements Runnable {
	private Future<?> future;
	private BlockingQueue<Integer> copyFinishedBlockingQueue;

	public CopyFutureTask(Future<?> future, BlockingQueue<Integer> copyFinishedBlockingQueue) {
		this.future = future;
		this.copyFinishedBlockingQueue = copyFinishedBlockingQueue;
	}

	@Override
	public void run() {
		PhotobankMigration.mainLog.debug("CopyFutureTask running.");
		try {
			future.get();
			copyFinishedBlockingQueue.put(0);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
}

class MigrationUtils {

	static boolean isHashNeedMigration(String relativePath) {
		int hashCode = HashUtils.jkHash(relativePath);
		int m = hashCode % 1024;
		if (m > 511) {
			return true;
		}
		return false;
	}

	/**
	 * 101584057|1227694640551jpg.jpg
	 * 
	 * @param line
	 *            one line of dump file
	 * @return file path without first slash, if illegal,return null
	 */
	static String getRelativePathInfo(String line) {
		if (line == null)
			return null;
		String[] pair = StringUtils.split(line, '|');
		if (pair.length != 2)
			return null;
		String id = pair[0];
		String name = pair[1];
		if (StringUtils.isBlank(id) || StringUtils.isBlank(name)) {
			return null;
		}
		StringBuilder idStringBuilder = new StringBuilder();
		int idPrefixZeroCount = 9 - id.length();
		for (int i = 0; i < idPrefixZeroCount; i++) {
			idStringBuilder.append("0");
		}
		idStringBuilder.append(id);
		String idString = idStringBuilder.toString();
		String idOne = StringUtils.substring(idString, 0, 3);
		String idTwo = StringUtils.substring(idString, 3, 6);
		String idThree = StringUtils.substring(idString, 6, 9);
		return new StringBuilder("photobank/").append(idThree).append(File.separator).append(idTwo)
				.append(File.separator).append(idOne).append(File.separator).append(name).toString();
	}
}

/**
 * copy from aranda
 * 
 * @author Leo Young Jul 11, 2012
 */
class HashUtils {

	public static String path2hashFileName(String path) {
		if (path == null) {
			return "" + jkHash(UUID.randomUUID().toString());
		} else {
			return "" + jkHash(path);
		}
	}

	/**
	 * Jenkins One At A Time Hash algorithm
	 * <p/>
	 * See <a href="http://www.burtleburtle.net/bob/hash/doobs.html">this article</a> by Bob Jenkins for more details.
	 * 
	 * @param key
	 * @return
	 */
	public static int jkHash(byte[] key) {
		int key_len = key.length;
		int hashValue = 0;
		int i;
		for (i = 0; i < key_len; i++) {
			hashValue += key[i];
			hashValue += (hashValue << 10);
			hashValue ^= (hashValue >> 6);
		}
		hashValue += (hashValue << 3);
		hashValue ^= (hashValue >> 11);
		hashValue += (hashValue << 15);
		return hashValue < 0 ? -hashValue : hashValue;
	}

	public static int jkHash(String input) {
		try {
			return jkHash(input.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
