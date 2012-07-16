package com.leoyoung.tool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Leo Young Jul 16, 2012
 */
public class MigrationSettings {
	private static final String OLD_REPO_KEY = "migration.oldrepo.rootdir";
	private static final String NEW_REPO_KEY = "migration.newrepo.rootdir";
	private static final Properties properties;
	static {
		properties = new Properties();
		InputStream inputStream = MigrationSettings.class.getResourceAsStream("/migration.properties");
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			HashMigration.mainLog.error("MigrationSettings init error.", e);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static String getOldRepoAbsoluteDir() {
		return properties.getProperty(OLD_REPO_KEY);
	}

	public static String getNewRepoAbsoluteDir() {
		return properties.getProperty(NEW_REPO_KEY);
	}
}
