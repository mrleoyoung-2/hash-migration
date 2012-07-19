package com.leoyoung.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Random;

public class PhotobankTestData {

	/**
	 * create test dump file
	 * 
	 * @param filename
	 *            test dump file name
	 * @param size
	 *            the file amount
	 * @throws Exception
	 */
	public static void create(String filename, int size) throws Exception {
		// make dump file
		int[] ids = new int[size];
		Random random = new Random();
		int i = 0;
		while (i < size) {
			ids[i++] = random.nextInt(1000000000);
		}
		File testDumpFile = new File(filename);
		if (!testDumpFile.getParentFile().exists()) {
			testDumpFile.getParentFile().mkdirs();
		}
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(testDumpFile));
		for (int id : ids) {
			bufferedWriter.write(id + "|" + random.nextInt(1000) + ".data\n");
		}
		bufferedWriter.close();
		// make files
		BufferedReader in = new BufferedReader(new FileReader(testDumpFile));
		String line;
		while ((line = in.readLine()) != null) {
			String relativePath = MigrationUtils.getPhotobankRelativePath(line);
			File testfile = new File("oldrepo/" + relativePath);
			File parentFile = testfile.getParentFile();
			if (!parentFile.exists()) {
				parentFile.mkdirs();
			}
			FileOutputStream fileOutputStream = new FileOutputStream(testfile);
			fileOutputStream.write("PhotobankTestData".getBytes());
			fileOutputStream.close();
		}
		in.close();
	}
}
