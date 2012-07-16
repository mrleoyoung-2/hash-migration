package com.leoyoung.tool;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Leo Young Jul 16, 2012
 */
public class HashMigrationTest {

	private static final String DUMP_FILE = "oldrepo/photobank_db_dump";

	@Before
	public void before() {
		try {
			PhotobankTestData.create(DUMP_FILE, 20);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void after() {
		File oldrepo = new File("oldrepo");
		File newrepo = new File("newrepo");
		try {
			FileUtils.deleteDirectory(oldrepo);
			FileUtils.deleteDirectory(newrepo);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPhotobankRelativePath() {
		String line = "101584057|1227694640551jpg.jpg";
		Assert.assertEquals("photobank/057/584/101/1227694640551jpg.jpg", MigrationUtils.getRelativePathInfo(line));

		String line2 = "67374|cnjinchangcheng_9.jpg";
		Assert.assertEquals("photobank/374/067/000/cnjinchangcheng_9.jpg", MigrationUtils.getRelativePathInfo(line2));
	}

	/**
	 * make real copy for all
	 */
	@Test
	public void testMigrationFull() {
		String[] args = new String[] { DUMP_FILE, "10" };
		HashMigration.main(args);
	}

	/**
	 * print to the console
	 */
	@Test
	public void testMigrationTest() {
		String[] args = new String[] { DUMP_FILE, "10", "-t" };
		HashMigration.main(args);
	}
}
