package net.simpleframework.ado.db.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.common.ClassUtils;
import net.simpleframework.common.Convert;
import net.simpleframework.common.ID;
import net.simpleframework.common.IoUtils;
import net.simpleframework.common.StringUtils;
import net.simpleframework.common.coll.ParameterMap;
import net.simpleframework.common.logger.Log;
import net.simpleframework.common.logger.LogFactory;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class JdbcUtils {
	static Log log = LogFactory.getLogger(JdbcUtils.class);

	public static class _DatabaseMetaData {
		private String _url;

		private String _databaseProductName;

		private final ParameterMap _alias = new ParameterMap().add("Oracle", SqlConstants.Oracle)
				.add("MySQL", SqlConstants.MySQL).add("HSQL Database Engine", SqlConstants.HSQL);

		public String dbName() {
			return StringUtils.blank(_alias.get(_databaseProductName));
		}

		public String url() {
			return _url;
		}
	}

	private static Map<DataSource, _DatabaseMetaData> databaseMetaDataMap = new ConcurrentHashMap<DataSource, _DatabaseMetaData>();

	public static _DatabaseMetaData getDatabaseMetaData(final DataSource dataSource) {
		_DatabaseMetaData _metaData = databaseMetaDataMap.get(dataSource);
		if (_metaData != null) {
			return _metaData;
		}
		Connection connection = null;
		try {
			connection = getConnection(dataSource);
			final DatabaseMetaData metaData = connection.getMetaData();
			_metaData = new _DatabaseMetaData();
			_metaData._databaseProductName = metaData.getDatabaseProductName();
			_metaData._url = metaData.getURL();
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (final SQLException e) {
					log.warn(e);
				}
			}
		}
		databaseMetaDataMap.put(dataSource, _metaData);

		return _metaData;
	}

	public static int lookupColumnIndex(final ResultSetMetaData resultSetMetaData,
			final String columnName) throws SQLException {
		if (columnName != null) {
			final int columnCount = resultSetMetaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				if (columnName.equalsIgnoreCase(lookupColumnName(resultSetMetaData, i))) {
					return i;
				}
			}
		}
		return 0;
	}

	public static String lookupColumnName(final ResultSetMetaData resultSetMetaData,
			final int columnIndex) throws SQLException {
		String name = resultSetMetaData.getColumnLabel(columnIndex);
		if (name == null || name.length() < 1) {
			name = resultSetMetaData.getColumnName(columnIndex);
		}
		return name;
	}

	/*------------------------------------- Ope --------------------------------*/

	public static boolean isTableExists(final DataSource dataSource, final String tablename) {
		Connection connection = null;
		try {
			connection = getConnection(dataSource);
			return connection.getMetaData().getTables(null, null, tablename, new String[] { "TABLE" })
					.next();
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, null, null);
		}
	}

	// 判断 oracle sequence 是否已经存在
	public static boolean isSequenceExists(final DataSource dataSource, final String sequencename) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = getConnection(dataSource);
			ps = getNativeConnection(connection).prepareStatement(
					"select sequence_name from user_sequences where sequence_name='"
							+ sequencename.toUpperCase() + "'");
			return ps.executeQuery().next();
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, ps, null);
		}
	}

	public static int[] doBatch(final DataSource dataSource, final String[] sqlArr) {
		Connection connection = null;
		Statement stmt = null;
		try {
			connection = getConnection(dataSource);
			stmt = getNativeConnection(connection).createStatement();
			for (final String sql : sqlArr) {
				stmt.addBatch(sql);
			}
			return stmt.executeBatch();
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, stmt, null);
		}
	}

	public static int[] doBatch(final DataSource dataSource, final String sql, final int batchCount,
			final IBatchValueSetter setter) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = getConnection(dataSource);
			ps = getNativeConnection(connection).prepareStatement(sql);
			for (int i = 0; i < batchCount; i++) {
				setter.setValues(ps, i);
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, ps, null);
		}
	}

	public static <T> T doExecute(final DataSource dataSource, final IConnectionCallback<T> callback) {
		Connection connection = null;
		try {
			connection = getConnection(dataSource);
			return callback.doInConnection(connection);
		} catch (final SQLException ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, null, null);
		}
	}

	public static int doUpdate(final DataSource dataSource, final SQLValue sqlValue) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = getConnection(dataSource);
			ps = createPreparedStatement(connection, sqlValue, ResultSet.TYPE_FORWARD_ONLY);
			return ps.executeUpdate();
		} catch (final Exception ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, ps, null);
		}
	}

	public static void doQuery(final DataSource dataSource, final SQLValue sqlValue,
			final IQueryCallback callback) {
		doQuery(dataSource, sqlValue, callback, ResultSet.TYPE_FORWARD_ONLY);
	}

	public static void doQuery(final DataSource dataSource, final SQLValue sqlValue,
			final IQueryCallback callback, final int resultSetType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = getConnection(dataSource);
			ps = createPreparedStatement(connection, sqlValue, resultSetType);
			callback.processRow(rs = ps.executeQuery());
		} catch (final Exception ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, ps, rs);
		}
	}

	public static <T> T queryObject(final DataSource dataSource, final SQLValue sqlValue,
			final IQueryExtractor<T> extractor, final int resultSetType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = getConnection(dataSource);
			ps = createPreparedStatement(connection, sqlValue, resultSetType);
			return extractor.extractData(rs = ps.executeQuery());
		} catch (final Exception ex) {
			throw DataAccessException.of(ex);
		} finally {
			closeAll(connection, ps, rs);
		}
	}

	public static PreparedStatement createPreparedStatement(final Connection connection,
			final SQLValue sqlValue, final int resultSetType) throws SQLException, IOException {
		final PreparedStatement ps = getNativeConnection(connection).prepareStatement(
				sqlValue.getSql(), resultSetType, ResultSet.CONCUR_READ_ONLY);
		final Object[] values = getParameterValues(sqlValue.getValues());
		if (values != null) {
			for (int i = 1; i <= values.length; i++) {
				final Object value = values[i - 1];
				setParameterValueInternal(ps, i,
						getParameterType(value == null ? null : value.getClass()), value);
			}
		}
		return ps;
	}

	private static Object[] getParameterValues(final Object[] values) {
		if (values == null) {
			return null;
		}
		final Object[] newValues = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			if (values[i] instanceof Enum<?>) {
				newValues[i] = ((Enum<?>) values[i]).ordinal();
			} else if (values[i] instanceof ID) {
				newValues[i] = ((ID) values[i]).getValue();
			} else {
				newValues[i] = values[i];
			}
		}
		return newValues;
	}

	private static void setParameterValueInternal(final PreparedStatement ps, final int paramIndex,
			final int sqlType, final Object inValue) throws SQLException, IOException {
		boolean object = false;
		if (inValue == null) {
			ps.setNull(paramIndex, sqlType);
		} else {
			if (sqlType == Types.VARCHAR || sqlType == Types.LONGVARCHAR) {
				ps.setString(paramIndex, inValue.toString());
			} else if (sqlType == Types.INTEGER || sqlType == Types.TINYINT
					|| sqlType == Types.SMALLINT || sqlType == Types.BIT || sqlType == Types.BIGINT
					|| sqlType == Types.FLOAT || sqlType == Types.DOUBLE || sqlType == Types.NUMERIC
					|| sqlType == Types.DECIMAL) {
				if (inValue instanceof Long) {
					ps.setLong(paramIndex, (Long) inValue);
				} else if (inValue instanceof Integer) {
					ps.setInt(paramIndex, (Integer) inValue);
				} else if (inValue instanceof Short) {
					ps.setShort(paramIndex, (Short) inValue);
				} else if (inValue instanceof Byte) {
					ps.setByte(paramIndex, (Byte) inValue);
				} else if (inValue instanceof Float) {
					ps.setFloat(paramIndex, (Float) inValue);
				} else if (inValue instanceof Double) {
					ps.setDouble(paramIndex, (Double) inValue);
				} else if (inValue instanceof BigDecimal) {
					ps.setBigDecimal(paramIndex, (BigDecimal) inValue);
				} else {
					object = true;
				}
			} else if (sqlType == Types.DATE) {
				if (inValue instanceof java.util.Date) {
					if (inValue instanceof java.sql.Date) {
						ps.setDate(paramIndex, (java.sql.Date) inValue);
					} else {
						ps.setDate(paramIndex, new java.sql.Date(((java.util.Date) inValue).getTime()));
					}
				} else if (inValue instanceof Calendar) {
					final Calendar cal = (Calendar) inValue;
					ps.setDate(paramIndex, new java.sql.Date(cal.getTime().getTime()), cal);
				} else {
					object = true;
				}
			} else if (sqlType == Types.TIME) {
				if (inValue instanceof java.util.Date) {
					if (inValue instanceof java.sql.Time) {
						ps.setTime(paramIndex, (java.sql.Time) inValue);
					} else {
						ps.setTime(paramIndex, new java.sql.Time(((java.util.Date) inValue).getTime()));
					}
				} else if (inValue instanceof Calendar) {
					final Calendar cal = (Calendar) inValue;
					ps.setTime(paramIndex, new java.sql.Time(cal.getTime().getTime()), cal);
				} else {
					object = true;
				}
			} else if (sqlType == Types.TIMESTAMP) {
				if (inValue instanceof java.util.Date) {
					if (inValue instanceof java.sql.Timestamp) {
						ps.setTimestamp(paramIndex, (java.sql.Timestamp) inValue);
					} else {
						ps.setTimestamp(paramIndex,
								new java.sql.Timestamp(((java.util.Date) inValue).getTime()));
					}
				} else if (inValue instanceof Calendar) {
					final Calendar cal = (Calendar) inValue;
					ps.setTimestamp(paramIndex, new java.sql.Timestamp(cal.getTime().getTime()), cal);
				} else {
					object = true;
				}
			} else if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY
					|| sqlType == Types.LONGVARBINARY) {
				if (inValue instanceof byte[]) {
					ps.setBytes(paramIndex, (byte[]) inValue);
				} else if (inValue instanceof InputStream) {
					if (setBinaryStreamMethod != null) {
						ClassUtils.invoke(setBinaryStreamMethod, ps, paramIndex, inValue);
					} else if (inValue instanceof ByteArrayInputStream) {
						try {
							ps.setBinaryStream(paramIndex, (InputStream) inValue,
									Convert.toInt(ClassUtils.getFieldValue("count", inValue), -1));
						} catch (final NoSuchFieldException e) {
						}
					} else if (inValue instanceof FileInputStream) {
						final FileInputStream fStream = (FileInputStream) inValue;
						ps.setBinaryStream(paramIndex, fStream, (int) fStream.getChannel().size());
					} else {
						final File file = new File(System.getProperty("java.io.tmpdir")
								+ StringUtils.hash(inValue));
						IoUtils.copyFile((InputStream) inValue, file);
						final FileInputStream fStream = new FileInputStream(file);
						ps.setBinaryStream(paramIndex, fStream, (int) fStream.getChannel().size());
					}
				} else {
					object = true;
				}
			} else if (sqlType == Types.CLOB) {
				if (inValue instanceof char[]) {
					ps.setString(paramIndex, new String((char[]) inValue));
				} else if (inValue instanceof Properties) {
					final Properties props = (Properties) inValue;
					if (props.size() > 0) {
						ps.setString(paramIndex, Convert.toString(props, null));
					} else {
						ps.setNull(paramIndex, sqlType);
					}
				} else if (inValue instanceof Reader) {
					if (setCharacterStreamMethod != null) {
						ClassUtils.invoke(setCharacterStreamMethod, ps, paramIndex, inValue);
					} else if (inValue instanceof StringReader) {
						try {
							ps.setString(paramIndex, (String) ClassUtils.getFieldValue("str", inValue));
						} catch (final NoSuchFieldException e) {
						}
					} else {
						ps.setString(paramIndex, IoUtils.getStringFromReader((Reader) inValue));
					}
				} else {
					object = true;
				}
			}
			if (object == true) {
				ps.setObject(paramIndex, inValue, sqlType);
			}
		}
	}

	private static Method setBinaryStreamMethod, setCharacterStreamMethod;
	static {
		try {
			// jdbc4
			setBinaryStreamMethod = PreparedStatement.class.getMethod("setBinaryStream", int.class,
					InputStream.class);
			if (Modifier.isAbstract(setBinaryStreamMethod.getModifiers())) {
				setBinaryStreamMethod = null;
			}
			setCharacterStreamMethod = PreparedStatement.class.getMethod("setCharacterStream",
					int.class, Reader.class);
			if (Modifier.isAbstract(setCharacterStreamMethod.getModifiers())) {
				setCharacterStreamMethod = null;
			}
		} catch (final NoSuchMethodException e) {
		}
	}

	private static int getParameterType(final Class<?> paramType) {
		if (paramType == null) {
			return Types.NULL;
		}
		if (paramType.equals(String.class)) {
			return Types.VARCHAR;
		} else if (Enum.class.isAssignableFrom(paramType)) {
			// 枚举保存索引
			return Types.NUMERIC;
		} else if (paramType.equals(boolean.class) || paramType.equals(Boolean.class)) {
			// 数据库一般没有boolean类型，这里用NUMERIC替代
			return Types.NUMERIC;
		} else if (paramType.equals(int.class) || paramType.equals(long.class)
				|| paramType.equals(double.class) || paramType.equals(float.class)
				|| paramType.equals(short.class) || paramType.equals(byte.class)
				|| Number.class.isAssignableFrom(paramType)) {
			return Types.NUMERIC;
		} else if (paramType.equals(java.util.Date.class)
				|| paramType.equals(java.util.Calendar.class)) {
			return Types.TIMESTAMP;
		} else if (paramType.equals(java.sql.Timestamp.class)) {
			return Types.TIMESTAMP;
		} else if (paramType.equals(java.sql.Date.class)) {
			return Types.DATE;
		} else if (paramType.equals(java.sql.Time.class)) {
			return Types.TIME;
		} else if (paramType.equals(byte[].class) || InputStream.class.isAssignableFrom(paramType)) {
			return Types.BLOB;
		} else if (paramType.equals(char[].class) || paramType.equals(Properties.class)
				|| Reader.class.isAssignableFrom(paramType)) {
			return Types.CLOB;
		}
		return Types.NULL;
	}

	public static Object getResultSetValue(final ResultSet rs, final int index) throws SQLException {
		Object obj = rs.getObject(index);
		String className = null;
		if (obj != null) {
			className = obj.getClass().getName();
		}
		if (obj instanceof Blob) {
			obj = ((Blob) obj).getBinaryStream();
		} else if (obj instanceof Clob) {
			obj = ((Clob) obj).getCharacterStream();
		} else if (className != null
				&& ("oracle.sql.TIMESTAMP".equals(className) || "oracle.sql.TIMESTAMPTZ"
						.equals(className))) {
			obj = rs.getTimestamp(index);
		} else if (className != null && className.startsWith("oracle.sql.DATE")) {
			final String metaDataClassName = rs.getMetaData().getColumnClassName(index);
			if ("java.sql.Timestamp".equals(metaDataClassName)
					|| "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
				obj = rs.getTimestamp(index);
			} else {
				obj = rs.getDate(index);
			}
		} else if (obj != null && obj instanceof java.sql.Date) {
			if ("java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(index))) {
				obj = rs.getTimestamp(index);
			}
		}
		return obj;
	}

	public static Object getResultSetValue(final ResultSet rs, final int columnIndex,
			final Class<?> requiredType) throws SQLException {
		if (requiredType == null) {
			return getResultSetValue(rs, columnIndex);
		}
		Object obj = null;
		boolean wasNullCheck = false;
		if (String.class.equals(requiredType)) {
			obj = rs.getString(columnIndex);
		} else if (boolean.class.equals(requiredType) || Boolean.class.equals(requiredType)) {
			obj = rs.getBoolean(columnIndex);
			wasNullCheck = true;
		} else if (byte.class.equals(requiredType) || Byte.class.equals(requiredType)) {
			obj = rs.getByte(columnIndex);
			wasNullCheck = true;
		} else if (short.class.equals(requiredType) || Short.class.equals(requiredType)) {
			obj = rs.getShort(columnIndex);
			wasNullCheck = true;
		} else if (int.class.equals(requiredType) || Integer.class.equals(requiredType)) {
			obj = rs.getInt(columnIndex);
			wasNullCheck = true;
		} else if (long.class.equals(requiredType) || Long.class.equals(requiredType)) {
			obj = rs.getLong(columnIndex);
			wasNullCheck = true;
		} else if (float.class.equals(requiredType) || Float.class.equals(requiredType)) {
			obj = rs.getFloat(columnIndex);
			wasNullCheck = true;
		} else if (double.class.equals(requiredType) || Double.class.equals(requiredType)
				|| Number.class.equals(requiredType)) {
			obj = rs.getDouble(columnIndex);
			wasNullCheck = true;
		} else if (java.sql.Date.class.equals(requiredType)) {
			obj = rs.getDate(columnIndex);
		} else if (java.sql.Time.class.equals(requiredType)) {
			obj = rs.getTime(columnIndex);
		} else if (java.sql.Timestamp.class.equals(requiredType)
				|| java.util.Date.class.equals(requiredType)) {
			obj = rs.getTimestamp(columnIndex);
		} else if (BigDecimal.class.equals(requiredType)) {
			obj = rs.getBigDecimal(columnIndex);
		} else if (byte[].class.equals(requiredType)) {
			obj = rs.getBytes(columnIndex);
		} else if (Blob.class.equals(requiredType)
				|| InputStream.class.isAssignableFrom(requiredType)) {
			final Blob blob = rs.getBlob(columnIndex);
			if (blob != null) {
				obj = blob.getBinaryStream();
			}
		} else if (char[].class.equals(requiredType)) {
			final String str = rs.getString(columnIndex);
			if (str != null) {
				obj = str.toCharArray();
			}
		} else if (Properties.class.equals(requiredType)) {
			obj = Convert.toProperties(rs.getString(columnIndex));
		} else if (Clob.class.equals(requiredType) || Reader.class.isAssignableFrom(requiredType)) {
			final Clob clob = rs.getClob(columnIndex);
			if (clob != null) {
				obj = clob.getCharacterStream();
			}
		} else {
			obj = getResultSetValue(rs, columnIndex);
		}
		if (wasNullCheck && obj != null && rs.wasNull()) {
			obj = null;
		}
		return obj;
	}

	public static Connection getConnection(final DataSource dataSource) throws SQLException {
		return TransactionUtils.getConnection(dataSource);
	}

	public static Connection getNativeConnection(final Connection connection) {
		final Class<?> cClazz = connection.getClass();
		final String clazzName = cClazz.getName();
		try {
			if (clazzName.startsWith("com.mchange.v2.c3p0")) {
				if (getRawConnectionMethod == null) {
					getRawConnectionMethod = JdbcUtils.class.getMethod("getNativeConnection",
							Connection.class);
				}
				if (rawConnectionOperationMethod == null) {
					rawConnectionOperationMethod = cClazz.getMethod("rawConnectionOperation",
							Method.class, Object.class, Object[].class);
				}
				if (RAW_CONNECTION == null) {
					RAW_CONNECTION = cClazz.getField("RAW_CONNECTION").get(null);
				}
				return (Connection) ClassUtils.invoke(rawConnectionOperationMethod, connection,
						getRawConnectionMethod, null, new Object[] { RAW_CONNECTION });
			} else if (clazzName.startsWith("weblogic.jdbc")) {
				if (getVendorConnectionMethod == null) {
					getVendorConnectionMethod = ClassUtils.forName(
							"weblogic.jdbc.extensions.WLConnection").getMethod("getVendorConnection");
				}
				return (Connection) ClassUtils.invoke(getVendorConnectionMethod, connection);
			}
		} catch (final Exception e) {
			log.warn(e);
		}
		return connection;
	}

	public static void closeAll(final Connection connection, final Statement stat, final ResultSet rs) {
		try {
			if (connection != null && !connection.isClosed()) {
				if (!TransactionUtils.inTrans(connection)) {
					connection.close();
				}
			}
			if (stat != null && !stat.isClosed()) {
				stat.close();
			}
			if (rs != null && !rs.isClosed()) {
				rs.close();
			}
		} catch (final SQLException e) {
			log.warn(e);
		}
	}

	// c3p0
	private static Method getRawConnectionMethod, rawConnectionOperationMethod;
	private static Object RAW_CONNECTION;

	// weblogic
	private static Method getVendorConnectionMethod;
}
