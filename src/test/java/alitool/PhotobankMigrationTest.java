package alitool;

import org.junit.Before;
import org.junit.Test;


public class PhotobankMigrationTest {

	private static final String DUMP_FILE = "photobank_db_dump";

	//@Before
	public void before() {
		try {
			PhotobankTestData.create(DUMP_FILE, 200);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// @Test
	// public void testRelativePath() {
	// String line = "101584057|1227694640551jpg.jpg";
	// Assert.assertEquals("photobank/057/584/101/1227694640551jpg.jpg", MigrationUtils.getRelativePathInfo(line));
	//
	// String line2 = "67374|cnjinchangcheng_9.jpg";
	// Assert.assertEquals("photobank/374/067/000/cnjinchangcheng_9.jpg", MigrationUtils.getRelativePathInfo(line2));
	// }

	@Test
	public void testMigration() {
		String[] args = new String[] { DUMP_FILE, "10" };
	//	PhotobankMigration.main(args);
	}
}
