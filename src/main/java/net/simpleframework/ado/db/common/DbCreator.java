package net.simpleframework.ado.db.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Iterator;

import javax.sql.DataSource;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.common.StringUtils;
import net.simpleframework.common.Version;
import net.simpleframework.common.logger.Log;
import net.simpleframework.common.logger.LogFactory;
import net.simpleframework.common.xml.XmlDocument;
import net.simpleframework.common.xml.XmlDocument.XmlDocumentException;
import net.simpleframework.common.xml.XmlElement;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class DbCreator {
	static Log log = LogFactory.getLogger(DbCreator.class);

	static final String stateAttri = "state";

	public static void executeSql(final DataSource dataSource, final String filepath)
			throws IOException {
		executeSql(dataSource, DbCreatorCallback.defaultCallback, filepath);
	}

	public static void executeSql(final DataSource dataSource, final DbCreatorCallback callback,
			final String filepath) throws IOException {
		SqlScriptDocument document;
		try {
			document = new SqlScriptDocument(new File(filepath));
		} catch (final FileNotFoundException e) {
			log.warn(e);
			return;
		}

		update(dataSource, callback, document);

		// 运行sql补丁
		final File[] patchArr = new File(document.configFile.getParentFile().getAbsolutePath()
				+ File.separator + "patch").listFiles();
		if (patchArr != null) {
			final Version version = document.getVersion();
			for (final File patchFile : patchArr) {
				document = new SqlScriptDocument(patchFile);
				if (document.getVersion().complies(version)) {
					update(dataSource, callback, document);
				}
			}
		}
	}

	static void update(final DataSource dataSource, final DbCreatorCallback callback,
			final SqlScriptDocument document) {
		if (callback != null) {
			callback.execute(document.getName(), document.getVersion(), document.getDescription());
		}
		Connection connection = null;
		Statement stat = null;
		try {
			final Iterator<?> it = document.sqlIterator();
			while (it.hasNext()) {
				final XmlElement xmlElement = (XmlElement) it.next();
				final String state = xmlElement.attributeValue(stateAttri);
				if (state != null && (state.equals("update") || state.equals("ignore"))) {
					continue;
				}
				if (connection == null) {
					connection = dataSource.getConnection();
					stat = connection.createStatement();
				}
				final String sqlText = xmlElement.getText();
				final String[] sqlArr = StringUtils.split(sqlText,
						StringUtils.text(xmlElement.attributeValue("delimiter"), ";"));
				if (sqlArr != null && sqlArr.length > 0) {
					try {
						final long l = System.currentTimeMillis();
						for (final String sql : sqlArr) {
							if (StringUtils.hasText(sql)) {
								stat.execute(sql);
							}
						}
						if (callback != null) {
							callback.execute(sqlText, System.currentTimeMillis() - l, null,
									xmlElement.elementText("description"));
						}
						xmlElement.addAttribute(stateAttri, "update");
					} catch (final SQLException e) {
						if (callback != null) {
							callback.execute(sqlText, 0, e, xmlElement.elementText("description"));
						}
						xmlElement.addAttribute(stateAttri, SQLSyntaxErrorException.class
								.isAssignableFrom(e.getClass()) ? "ignore" : "exception");
					}
				}
			}
			if (connection != null && !connection.getAutoCommit()) {
				connection.commit();
			}

			try {
				document.saveToFile(document.configFile);
			} catch (final IOException e) {
				throw XmlDocumentException.of(e);
			}
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			try {
				if (stat != null) {
					stat.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (final SQLException e) {
			}
		}

	}

	static class SqlScriptDocument extends XmlDocument {
		final File configFile;

		public SqlScriptDocument(final File configFile) throws FileNotFoundException {
			super(new FileInputStream(configFile));
			this.configFile = configFile;
		}

		String getName() {
			return getRoot().elementText("name");
		}

		Version getVersion() {
			return Version.getVersion(getRoot().elementText("version"));
		}

		String getDescription() {
			return getRoot().elementText("description");
		}

		Iterator<?> sqlIterator() {
			return getRoot().elementIterator("tran-sql");
		}
	}
}
