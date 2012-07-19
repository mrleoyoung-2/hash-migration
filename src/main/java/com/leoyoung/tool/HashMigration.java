package com.leoyoung.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
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
 * a simple migration tool based on hash code of the relative path
 * 
 * @author Leo Young Jul 10, 2012
 */
public class HashMigration {
	public static final Logger mainLog = Logger.getLogger("mainLogger");
	public static final Logger migrationLog = Logger.getLogger("migrationLogger");
	private static final String OLD_REPO_PREFIX = MigrationSettings.getOldRepoAbsoluteDir();
	private static final String NEW_REPO_PREFIX = MigrationSettings.getNewRepoAbsoluteDir();

	public static void main(String[] args) {
		mainLog.info("starting HashMigration copy job.");
		// get the id/name pair
		ParameterInfo parameterInfo = getParameterInfo(args);
		mainLog.debug(parameterInfo);
		if (parameterInfo == null) {
			mainLog.error("Usage: java -jar HashMigration-1.0-jar-with-dependencies.jar [dump_file] [count] [-t]");
			mainLog.error("Exampe: java -jar HashMigration-1.0-jar-with-dependencies.jar photobank_db_dump 500");
			return;
		}
		final int size = parameterInfo.getCopyThreads();
		final List<String> filenameList = parameterInfo.getDumpFiles();
		final BlockingQueue<Integer> copyFinishedBlockingQueue = new LinkedBlockingQueue<Integer>();
		final ExecutorService copyExecutorService = Executors.newFixedThreadPool(size);
		final ExecutorService futureExecutorService = Executors.newFixedThreadPool(size);

		if (mainLog.isDebugEnabled()) {
			Thread queueMonitorThread = new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(1000);
							mainLog.debug("copyFinishedBlockingQueue size: " + copyFinishedBlockingQueue.size());
						} catch (InterruptedException e) {
							mainLog.error(e);
						}
					}
				}
			});
			queueMonitorThread.setDaemon(true);
			queueMonitorThread.start();
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
					if (mainLog.isDebugEnabled()) {
						mainLog.debug("initThreadCount: " + queueSizeHolder);
					}

					if (queueSizeHolder > 0) {
						queueSizeHolder--;
					} else {
						copyFinishedBlockingQueue.take();
					}
					File[] files = findMigrationFiles(line);
					if (files != null) {
						Future<?> future = copyExecutorService.submit(CopyTaskFactory.getCopyTask(files[0], files[1],
								parameterInfo));
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
		mainLog.info("HashMigration job copy done.");
	}

	/**
	 * find files need migration
	 * 
	 * @param line
	 *            the dump file line
	 * @return the file pair that need migration. if line is illegal, or hash code means do not need, then return null.
	 */
	private static File[] findMigrationFiles(String line) {
		String relativePath = MigrationUtils.getPhotobankRelativePath(line);
		if (relativePath == null) {
			return null;
		}
		if (!MigrationUtils.isHashNeedMigration(relativePath)) {
			if (mainLog.isDebugEnabled())
				mainLog.debug("relativePath: " + relativePath + ", NOT migration.");
			return null;
		}
		File oldFile = new File(OLD_REPO_PREFIX + relativePath);
		File newFile = new File(NEW_REPO_PREFIX + relativePath);
		if (mainLog.isDebugEnabled())
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
			} else if (StringUtils.equals(arg, "-t")) {
				parameterInfo.setTest(true);
			} else {
				list.add(arg);
			}
		}
		parameterInfo.setDumpFiles(list);
		return parameterInfo;
	}
}

/**
 * the parameter infomation from the command line
 * 
 * @author Leo Young Jul 19, 2012
 */
class ParameterInfo {
	private List<String> dumpFiles = new LinkedList<String>();
	private int copyThreads;
	private boolean isTest;

	public boolean isTest() {
		return isTest;
	}

	public void setTest(boolean isTest) {
		this.isTest = isTest;
	}

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
	protected File oldFile;
	protected File newFile;

	public CopyTask(File oldFile, File newFile) {
		this.oldFile = oldFile;
		this.newFile = newFile;
	}

	@Override
	public void run() {
		if (HashMigration.mainLog.isDebugEnabled()) {
			HashMigration.mainLog.debug("CopyTask start running for: " + newFile.getAbsolutePath());
		}

		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		try {
			fileInputStream = new FileInputStream(oldFile);
			fileOutputStream = new FileOutputStream(newFile);
			// create dir directly
			newFile.getParentFile().mkdirs();
			FileChannel in = fileInputStream.getChannel();
			FileChannel out = fileOutputStream.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(50 * 1024);
			while (in.read(buffer) != -1) {
				buffer.flip();
				out.write(buffer);
				buffer.clear();
			}
			// log the copyed file path
			HashMigration.migrationLog.info(oldFile.getAbsolutePath());
		} catch (FileNotFoundException fnfe) {
			return;
		} catch (Exception e) {
			HashMigration.mainLog.error(e);
		} finally {
			try {
				if (fileOutputStream != null)
					fileOutputStream.close();
				if (fileInputStream != null)
					fileInputStream.close();
			} catch (IOException e) {
				HashMigration.mainLog.error(e);
			}
		}
	}
}

/**
 * <pre>
 * readonly test task
 * 
 * read the data from the input stream, but not write to the destination file
 * </pre>
 * 
 * @author Leo Young Jul 19, 2012
 */
class CopyTaskTest extends CopyTask {

	public CopyTaskTest(File oldFile, File newFile) {
		super(oldFile, newFile);
	}

	@Override
	public void run() {
		if (HashMigration.mainLog.isDebugEnabled()) {
			HashMigration.mainLog.debug("CopyTaskTest start running for: " + newFile.getAbsolutePath());
		}

		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(oldFile);
			FileChannel in = fileInputStream.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(50 * 1024);
			while (in.read(buffer) != -1) {
				buffer.flip();
				// do nothing
				buffer.clear();
			}
			// log the copyed file path, FAKE!
			HashMigration.migrationLog.info("FAKE: " + oldFile.getAbsolutePath());
		} catch (FileNotFoundException fnfe) {
			return;
		} catch (Exception e) {
			HashMigration.mainLog.error(e);
		} finally {
			try {
				if (fileInputStream != null)
					fileInputStream.close();
			} catch (IOException e) {
				HashMigration.mainLog.error(e);
			}
		}
	}
}

/**
 * the task that monitor the copy task
 * 
 * @author Leo Young Jul 19, 2012
 */
class CopyFutureTask implements Runnable {
	private Future<?> future;
	private BlockingQueue<Integer> copyFinishedBlockingQueue;

	public CopyFutureTask(Future<?> future, BlockingQueue<Integer> copyFinishedBlockingQueue) {
		this.future = future;
		this.copyFinishedBlockingQueue = copyFinishedBlockingQueue;
	}

	@Override
	public void run() {
		HashMigration.mainLog.debug("CopyFutureTask running.");
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

/**
 * simple factory to swith the copy task and test copy task
 * 
 * @author Leo Young Jul 19, 2012
 */
class CopyTaskFactory {
	public static CopyTask getCopyTask(File oldFile, File newFile, ParameterInfo parameterInfo) {
		if (parameterInfo.isTest()) {
			return new CopyTaskTest(oldFile, newFile);
		} else {
			return new CopyTask(oldFile, newFile);
		}
	}
}