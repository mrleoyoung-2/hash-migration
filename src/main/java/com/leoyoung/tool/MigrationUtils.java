package com.leoyoung.tool;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Leo Young Jul 16, 2012
 */
public class MigrationUtils {

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
	static String getPhotobankRelativePath(String line) {
		line = StringUtils.trimToNull(line);
		if (StringUtils.isEmpty(line)) {
			return null;
		}
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
