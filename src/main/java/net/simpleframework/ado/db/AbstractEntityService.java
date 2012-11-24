package net.simpleframework.ado.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;

import javax.sql.DataSource;

import net.simpleframework.ado.AbstractDataService;
import net.simpleframework.ado.DataAccessException;
import net.simpleframework.ado.IDataServiceListener;
import net.simpleframework.ado.db.common.DbColumn;
import net.simpleframework.ado.db.common.IBatchValueSetter;
import net.simpleframework.ado.db.common.IConnectionCallback;
import net.simpleframework.ado.db.common.IQueryExtractor;
import net.simpleframework.ado.db.common.ITransactionCallback;
import net.simpleframework.ado.db.common.JdbcUtils;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.common.TransactionObjectCallback;
import net.simpleframework.ado.db.common.TransactionUtils;
import net.simpleframework.ado.db.event.IEntityListener;
import net.simpleframework.common.coll.KVMap;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class AbstractEntityService extends AbstractDataService implements IEntityService {
	protected DataSource dataSource;

	public AbstractEntityService(final DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(final DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public <T> T executeQuery(final SQLValue sqlValue, final IQueryExtractor<T> extractor,
			final int resultSetType) {
		return JdbcUtils.queryObject(getDataSource(), sqlValue, extractor, resultSetType);
	}

	@Override
	public <T> T executeQuery(final SQLValue value, final IQueryExtractor<T> extractor) {
		return executeQuery(value, extractor, ResultSet.TYPE_FORWARD_ONLY);
	}

	protected int executeUpdate(final String sql) {
		return executeUpdate(new SQLValue(sql));
	}

	protected int executeUpdate(final SQLValue sqlValue) {
		return JdbcUtils.doUpdate(getDataSource(), sqlValue);
	}

	@Override
	public <T> T execute(final IConnectionCallback<T> callback) {
		return JdbcUtils.doExecute(getDataSource(), callback);
	}

	@Override
	public int execute(final IEntityListener l, final SQLValue... sqlValues) {
		if (sqlValues == null || sqlValues.length == 0) {
			return 0;
		}
		if (l != null) {
			l.beforeExecute(this, sqlValues);
		}
		final Collection<IDataServiceListener> listeners = getListeners();
		for (final IDataServiceListener listener : listeners) {
			((IEntityListener) listener).beforeExecute(this, sqlValues);
		}
		int ret = 0;
		for (final SQLValue sqlValue : sqlValues) {
			ret += executeUpdate(sqlValue);
		}
		if (l != null) {
			l.afterExecute(this, sqlValues);
		}
		for (final IDataServiceListener listener : listeners) {
			((IEntityListener) listener).afterExecute(this, sqlValues);
		}
		return ret;
	}

	@Override
	public int execute(final SQLValue... sqlValues) {
		return execute(null, sqlValues);
	}

	@Override
	public int[] batchUpdate(final String... sqlArr) {
		return JdbcUtils.doBatch(getDataSource(), sqlArr);
	}

	@Override
	public int[] batchUpdate(final String sql, final int batchCount, final IBatchValueSetter setter) {
		return JdbcUtils.doBatch(getDataSource(), sql, batchCount, setter);
	}

	@Override
	public int executeTransaction(final SQLValue... sqlValues) {
		return executeTransaction(null, sqlValues);
	}

	@Override
	public int executeTransaction(final IEntityListener l, final SQLValue... sqlValues) {
		return doExecuteTransaction(new TransactionObjectCallback<Integer>() {

			@Override
			public Integer doTransactionCallback() throws DataAccessException {
				return execute(l, sqlValues);
			}
		});
	}

	@Override
	public <T> T doExecuteTransaction(final ITransactionCallback<T> callback) {
		Connection connection = null;
		try {
			connection = TransactionUtils.begin(getDataSource());
			final T t = callback.doTransactionCallback();
			connection.commit();
			return t;
		} catch (final Throwable th) {
			try {
				connection.rollback();
			} catch (final SQLException e2) {
				log.warn(e2);
			}
			log.warn(th);
			throw DataAccessException.of(th);
		} finally {
			TransactionUtils.end(connection);
		}
	}

	protected DbColumn[] getColumns(final String[] columns) {
		if (columns != null) {
			final DbColumn[] objects = new DbColumn[columns.length];
			for (int i = 0; i < columns.length; i++) {
				objects[i] = new DbColumn(columns[i]);
			}
			return objects;
		} else {
			return null;
		}
	}

	protected Map<String, Object> mapRowData(final Object[] columns, final ResultSet rs)
			throws SQLException {
		final ResultSetMetaData rsmd = rs.getMetaData();
		final int columnCount = columns != null ? columns.length : rsmd.getColumnCount();
		final Map<String, Object> mapData = new KVMap(columnCount).setCaseInsensitive(true);
		for (int i = 1; i <= columnCount; i++) {
			final Object column = columns != null ? columns[i - 1] : null;
			String key;
			Object obj;
			if (column != null) {
				key = columnKey(column);
				if (column instanceof DbColumn) {
					final DbColumn oColumn = (DbColumn) column;
					final int j = JdbcUtils.lookupColumnIndex(rsmd, oColumn.getColumnSqlName());
					obj = JdbcUtils.getResultSetValue(rs, j, oColumn.getPropertyClass());
				} else {
					final int j = JdbcUtils.lookupColumnIndex(rsmd, key);
					obj = JdbcUtils.getResultSetValue(rs, j, null);
				}
			} else {
				key = JdbcUtils.lookupColumnName(rsmd, i);
				obj = JdbcUtils.getResultSetValue(rs, i, null);
			}
			mapData.put(key, obj);
		}
		return mapData;
	}

	protected String columnKey(final Object column) {
		return column instanceof DbColumn ? ((DbColumn) column).getColumnName() : column.toString();
	}

	protected <T> T createQueryObject(final String[] columns, final SQLValue sqlValue,
			final Class<T> beanClass) {
		final BeanWrapper<T> wrapper = new BeanWrapper<T>(columns, beanClass);
		return executeQuery(sqlValue, new IQueryExtractor<T>() {

			@Override
			public T extractData(final ResultSet rs) throws SQLException, DataAccessException {
				return rs.next() ? wrapper.toBean(rs) : null;
			}
		});
	}

	protected Map<String, Object> createQueryMap(final Object[] columns, final SQLValue sqlValue) {
		return executeQuery(sqlValue, new IQueryExtractor<Map<String, Object>>() {

			@Override
			public Map<String, Object> extractData(final ResultSet rs) throws SQLException,
					DataAccessException {
				return rs.next() ? mapRowData(columns, rs) : null;
			}
		});
	}

	protected QueryEntitySet<Map<String, Object>> createQueryEntitySet(final Object[] columns,
			final SQLValue sqlValue) {
		return new QueryEntitySet<Map<String, Object>>(getDataSource(), sqlValue) {

			@Override
			public Map<String, Object> mapRow(final ResultSet rs, final int rowNum)
					throws SQLException {
				return mapRowData(columns, rs);
			}
		};
	}

	protected <T> QueryEntitySet<T> createQueryEntitySet(final String[] columns,
			final SQLValue sqlValue, final Class<T> beanClass) {
		final BeanWrapper<T> wrapper = new BeanWrapper<T>(columns, beanClass);
		return new QueryEntitySet<T>(getDataSource(), sqlValue) {

			@Override
			public T mapRow(final ResultSet rs, final int rowNum) throws SQLException {
				return wrapper.toBean(rs);
			}
		};
	}

	private Collection<IDataServiceListener> listeners;

	@Override
	public Collection<IDataServiceListener> getListeners() {
		if (listeners == null) {
			listeners = new LinkedHashSet<IDataServiceListener>();
		}
		return listeners;
	}

	@Override
	public void addListener(final IDataServiceListener listener) {
		getListeners().add(listener);
	}

	@Override
	public boolean removeListener(final IDataServiceListener listener) {
		return getListeners().remove(listener);
	}
}
