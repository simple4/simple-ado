package net.simpleframework.ado.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.ado.db.common.IQueryExtractor;
import net.simpleframework.ado.db.common.JdbcUtils;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.common.SqlUtils;
import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.IParamsValue;
import net.simpleframework.common.ado.query.AbstractDataQuery;
import net.simpleframework.common.ado.query.IDataQueryListener;
import net.simpleframework.common.coll.LRUMap;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class QueryEntitySet<T> extends AbstractDataQuery<T> implements IQueryEntitySet<T> {
	private DataSource dataSource;

	private SQLValue sqlValue;

	private QueryDialect queryDialect;

	private int fetchSize = -1;

	private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;

	private Map<Integer, T> dataCache;

	protected BeanWrapper<T> beanWrapper;

	public QueryEntitySet(final DataSource dataSource, final SQLValue sqlValue,
			final Class<T> beanClass) {
		this.dataSource = dataSource;
		this.sqlValue = sqlValue;
		if (beanClass != null) {
			this.beanWrapper = new BeanWrapper<T>(null, beanClass);
		}
	}

	public QueryEntitySet(final DataSource dataSource, final String sql, final Object[] values,
			final Class<T> beanClass) {
		this(dataSource, new SQLValue(sql, values), beanClass);
	}

	public QueryEntitySet(final DataSource dataSource, final SQLValue sqlValue) {
		this(dataSource, sqlValue, null);
	}

	public QueryEntitySet(final DataSource dataSource, final String sql, final Object[] values) {
		this(dataSource, sql, values, null);
	}

	@Override
	public DataSource getDataSource() {
		return dataSource;
	}

	@Override
	public void setFetchSize(final int fetchSize) {
		if (this.fetchSize != fetchSize) {
			this.fetchSize = fetchSize;
			dataCache = null;
		}
	}

	@Override
	public int getFetchSize() {
		if (fetchSize < 0 || fetchSize >= Integer.MAX_VALUE) {
			setFetchSize(100);
		}
		return fetchSize;
	}

	private Map<Integer, T> getDataCache() {
		if (dataCache == null) {
			dataCache = Collections.synchronizedMap(new LRUMap<Integer, T>(getFetchSize() * 5));
		}
		return dataCache;
	}

	@Override
	public void reset() {
		super.reset();
		dataCache = null;
	}

	@Override
	public int getCount() {
		if (count < 0) {
			Object[] values = null;
			String sql = null;
			final IParamsValue countSQL = getQueryDialect().getCountSQL();
			if (countSQL != null) {
				values = ((SQLValue) countSQL).getValues();
				sql = ((SQLValue) countSQL).getSql();
			}

			if (!StringUtils.hasText(sql)) {
				sql = SqlUtils.wrapCount(dataSource, sqlValue.getSql());
				if (values == null || values.length == 0) {
					values = sqlValue.getValues();
				}
			}

			count = JdbcUtils.queryObject(dataSource, new SQLValue(sql, values),
					new IQueryExtractor<Integer>() {

						@Override
						public Integer extractData(final ResultSet rs) throws SQLException,
								DataAccessException {
							return rs.next() ? rs.getInt(1) : 0;
						}
					}, getResultSetType());
		}
		return count;
	}

	public T mapRow(final ResultSet rs, final int rowNum) throws SQLException {
		return beanWrapper != null ? beanWrapper.toBean(rs) : null;
	}

	/*------------------------- fetchSize==0 --------------------------*/
	private Connection _conn;
	private PreparedStatement _ps;
	private ResultSet _rs;

	@Override
	public void close() {
		JdbcUtils.closeAll(_conn, _ps, _rs);
	}

	@Override
	public T next() {
		T bean = null;
		i++;
		final int fetchSize = getFetchSize();
		if (fetchSize <= 0) {
			try {
				if (i == 0 && _rs == null) {
					_conn = dataSource.getConnection();
					_ps = JdbcUtils.createPreparedStatement(_conn, sqlValue, getResultSetType());
					_rs = _ps.executeQuery();
				}
				if (_rs != null) {
					if (_rs.next()) {
						bean = mapRow(_rs, i);
					} else {
						close();
					}
				}
			} catch (final Exception e) {
				throw DataAccessException.of(e);
			}
		} else if (i < getCount() && i >= 0) {
			final Map<Integer, T> dataCache = getDataCache();
			bean = dataCache.get(i);
			if (bean == null) {
				final String sql = sqlValue.getSql();
				final String lsql = getQueryDialect().getNativeSQLValue(sql, i, fetchSize);
				final boolean absolute = lsql.equals(sql);
				final IQueryExtractor<T> extractor = new IQueryExtractor<T>() {

					@Override
					public T extractData(final ResultSet rs) throws SQLException, DataAccessException {
						if (absolute && i > 0) {
							rs.getStatement().setFetchSize(fetchSize);
							rs.absolute(i);
						}
						int j = -1;
						T first = null;
						while (rs.next()) {
							final int k = i + ++j;
							if (dataCache.containsKey(k)) {
								break;
							}
							final T row = mapRow(rs, j);
							if (j == 0) {
								first = row;
							}
							dataCache.put(k, row);
							// 在oracle测试中
							if (j == fetchSize - 1) {
								break;
							}
						}
						return first;
					}
				};
				bean = JdbcUtils.queryObject(dataSource, new SQLValue(lsql, sqlValue.getValues()),
						extractor, getResultSetType());
			}
		}
		if (bean != null) {
			pIndex++;
		} else {
			pIndex = -1;
		}
		final boolean pageEnd = (pIndex + 1) == (fetchSize == 0 ? getCount() : fetchSize);
		for (final IDataQueryListener<T> listener : getListeners()) {
			listener.next(this, bean, pIndex, pageEnd);
		}
		if (pageEnd) {
			pIndex = -1;
		}
		return bean;
	}

	private int pIndex = -1;

	@Override
	public SQLValue getSqlValue() {
		return sqlValue;
	}

	@Override
	public QueryDialect getQueryDialect() {
		if (queryDialect == null) {
			setQueryDialect(new QueryDialect());
		}
		return queryDialect;
	}

	@Override
	public void setQueryDialect(final QueryDialect queryDialect) {
		this.queryDialect = queryDialect;
		this.queryDialect.qs = this;
	}

	@Override
	public int getResultSetType() {
		return resultSetType;
	}

	@Override
	public void setResultSetType(final int resultSetType) {
		this.resultSetType = resultSetType;
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		close();
	}

	public Object doResultSetMetaData(final ResultSetMetaDataCallback callback) {
		final String sql = SqlUtils.addCondition(dataSource, sqlValue.getSql(), "1 = 2");
		return JdbcUtils.queryObject(dataSource, new SQLValue(sql, sqlValue.getValues()),
				new IQueryExtractor<Object>() {

					@Override
					public Object extractData(final ResultSet rs) throws SQLException,
							DataAccessException {
						return callback.doResultSetMetaData(rs.getMetaData());
					}
				}, getResultSetType());
	}

	public static interface ResultSetMetaDataCallback {

		Object doResultSetMetaData(ResultSetMetaData metaData) throws SQLException;
	}
}
