package com.leoyoung.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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
						Future<?> future = copyExecutorService.submit(CopyTaskFactory.getCopyTask(files[0], files[1],
								parameterInfo));
						futureExecutorService.execute(new CopyFutureTask(future, copyFinishedBlockingQueue));
					} else {
						queueSizeHolder++;
					}
					// make the test count equals the queueSizeHolder
					if (parameterInfo.isTest() && (queueSizeHolder <= 0)) {
						break;
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

	private static File[] needMigration(String line) {
		String relativePath = MigrationUtils.getRelativePathInfo(line);
		// if need migration & if old file exist & if new file not exist, copy file
		if (!MigrationUtils.isHashNeedMigration(relativePath)) {
			mainLog.debug("relativePath: " + relativePath + ", NOT migration.");
			return null;
		}
		File oldFile = new File(OLD_REPO_PREFIX + relativePath);
		File newFile = new File(NEW_REPO_PREFIX + relativePath);
		if (!oldFile.exists() || oldFile.isDirectory() || newFile.exists()) {
			mainLog.warn("relativePath: " + relativePath + ", NEED migration, BUT: !oldFile.exists(): "
					+ !oldFile.exists() + ", oldFile.isDirectory(): " + oldFile.isDirectory() + ", newFile.exists(): "
					+ newFile.exists());
			return null;
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
		HashMigration.mainLog.debug("CopyTask start running for: " + newFile.getAbsolutePath());

		if (!newFile.getParentFile().exists()) {
			newFile.getParentFile().mkdirs();
		}

		FileChannel in = null, out = null;
		try {
			in = new FileInputStream(oldFile).getChannel();
			out = new FileOutputStream(newFile).getChannel();
			in.transferTo(0, in.size(), out);
		} catch (Exception e) {
			HashMigration.mainLog.error(e);
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
				HashMigration.mainLog.error(e);
			}
		}
		// log original file path to migration.log, for future delete
		HashMigration.migrationLog.info(oldFile.getAbsolutePath());
	}
}

class CopyTaskTest extends CopyTask {

	public CopyTaskTest(File oldFile, File newFile) {
		super(oldFile, newFile);
	}

	@Override
	public void run() {
		HashMigration.mainLog.info("CopyTaskTest start running for: oldFile(" + oldFile.getAbsolutePath()
				+ "), newFile(" + newFile.getAbsolutePath() + ")");
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

class CopyTaskFactory {
	public static CopyTask getCopyTask(File oldFile, File newFile, ParameterInfo parameterInfo) {
		if (parameterInfo.isTest()) {
			return new CopyTaskTest(oldFile, newFile);
		} else {
			return new CopyTask(oldFile, newFile);
		}
	}
}