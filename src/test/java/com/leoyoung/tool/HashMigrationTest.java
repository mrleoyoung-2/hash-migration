package com.leoyoung.tool;

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
			PhotobankTestData.create(DUMP_FILE, 50);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void after() {

	}

	@Test
	public void testPhotobankRelativePath() {
		String line = "101584057|1227694640551jpg.jpg";
		Assert.assertEquals("photobank/057/584/101/1227694640551jpg.jpg", MigrationUtils.getRelativePathInfo(line));

		String line2 = "67374|cnjinchangcheng_9.jpg";
		Assert.assertEquals("photobank/374/067/000/cnjinchangcheng_9.jpg", MigrationUtils.getRelativePathInfo(line2));
	}

	@Test
	public void testMigrationFull() {
		String[] args = new String[] { DUMP_FILE, "10" };
		HashMigration.main(args);
	}

	@Test
	public void testMigrationTest() {
		String[] args = new String[] { DUMP_FILE, "10", "-t" };
		HashMigration.main(args);
	}
}
