package net.simpleframework.ado.db.cache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.ado.db.BeanWrapper;
import net.simpleframework.ado.db.IQueryEntitySet;
import net.simpleframework.ado.db.QueryEntitySet;
import net.simpleframework.ado.db.TableEntityService;
import net.simpleframework.ado.db.common.DbColumn;
import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.ado.db.common.ExpressionValue;
import net.simpleframework.ado.db.common.ITransactionCallback;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.common.TransactionUtils;
import net.simpleframework.ado.db.event.IEntityListener;
import net.simpleframework.ado.db.event.ITableEntityListener;
import net.simpleframework.common.ado.IParamsValue;
import net.simpleframework.common.ado.IParamsValue.AbstractParamsValue;
import net.simpleframework.common.bean.BeanUtils;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class AbstractCacheTableEntityService extends TableEntityService implements
		ITableEntityCache {

	public AbstractCacheTableEntityService(final DataSource dataSource, final DbTable dbTable) {
		super(dataSource, dbTable);
	}

	protected String toUniqueString(final Object object) {
		if (object == null) {
			return null;
		}
		final DbTable dbTable = getTable();
		if (dbTable.isNoCache()) {
			return null;
		}

		final StringBuilder sb = new StringBuilder();
		if (object instanceof SQLValue) {
			sb.append(((SQLValue) object).getKey());
		} else {
			sb.append(dbTable.getName());
			if (object instanceof IParamsValue) {
				sb.append("-").append(((IParamsValue) object).getKey());
			} else {
				for (final String uniqueColumn : dbTable.getUniqueColumns()) {
					sb.append("-");
					try {
						if (object instanceof ResultSet) {
							sb.append(AbstractParamsValue.valueToString(((ResultSet) object)
									.getObject(uniqueColumn)));
						} else {
							final Object o = BeanUtils.getProperty(object,
									DbColumn.propertyName(object.getClass(), uniqueColumn));
							sb.append(AbstractParamsValue.valueToString(o));
						}
					} catch (final Exception e) {
						return null;
					}
				}
			}
		}
		return sb.toString();
	}

	@Override
	protected int delete(final ITableEntityListener l, final IParamsValue paramsValue) {
		try {
			return super.delete(l, paramsValue);
		} finally {
			reset();
		}
	}

	@Override
	protected int update(final ITableEntityListener l, final Object[] columns,
			final Object... objects) {
		doUpdateObjects(objects);
		return super.update(l, columns, objects);
	}

	protected void doUpdateObjects(final Object... objects) {
		if (objects == null) {
			return;
		}
		final Map<String, ITableEntityCache> updates = entityCache.get();
		if (updates != null) {
			for (final Object object : objects) {
				final String key = toUniqueString(object);
				if (key != null) {
					updates.put(key, this);
				}
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T queryBean(final String[] columns, final IParamsValue paramsValue,
			final Class<T> beanClass) {
		final String key = toUniqueString(paramsValue);
		if (key == null) {
			return super.queryBean(columns, paramsValue, beanClass);
		}
		Object t = getCache(key);
		if (t == null || t instanceof Map) {
			// 此处传递null，而非columns，目的是在缓存情况下，忽略columns参数
			// 以后再考虑更好的方法
			if ((t = super.queryBean(null/* columns */, paramsValue, beanClass)) != null) {
				putCache(key, t);
			}
		} else {
			doUpdateObjects(t);
		}
		return (T) t;
	}

	@Override
	public <T> IQueryEntitySet<T> query(final String[] columns, final IParamsValue paramsValue,
			final Class<T> beanClass) {
		if (getTable().isNoCache()) {
			return super.query(columns, paramsValue, beanClass);
		} else {
			final BeanWrapper<T> wrapper = new BeanWrapper<T>(columns, beanClass);
			return new QueryEntitySet<T>(getDataSource(), createSQLValue(null /* columns */,
					paramsValue)) {
				@Override
				@SuppressWarnings("unchecked")
				public T mapRow(final ResultSet rs, final int rowNum) throws SQLException {
					final String key = toUniqueString(rs);
					if (key == null) {
						return wrapper.toBean(rs);
					}
					Object t = getCache(key);
					if (t == null || t instanceof Map) {
						if ((t = wrapper.toBean(rs)) != null) {
							putCache(key, t);
						}
					}
					return (T) t;
				}
			};
		}
	}

	/************************* Map Object ************************/

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> queryMap(final String[] columns, final IParamsValue paramsValue) {
		final String key = toUniqueString(paramsValue);
		if (key == null) {
			return super.queryMap(columns, paramsValue);
		}
		Object data = getCache(key);
		if (!(data instanceof Map)) {
			if ((data = super.queryMap(columns, paramsValue)) != null) {
				putCache(key, data);
			}
		} else {
			if (columns != null && columns.length > 0
					&& !((Map<String, Object>) data).containsKey(columnKey(columns[0]))) {
				final Map<String, Object> nData = super.queryMap(columns, paramsValue);
				if (nData != null) {
					((Map<String, Object>) data).putAll(nData);
				}
			}
			doUpdateObjects(data);
		}
		return (Map<String, Object>) data;
	}

	@Override
	public IQueryEntitySet<Map<String, Object>> query(final String[] columns,
			final IParamsValue paramsValue) {
		if (getTable().isNoCache()) {
			return super.query(columns, paramsValue);
		} else {
			return new QueryEntitySet<Map<String, Object>>(getDataSource(), createSQLValue(columns,
					paramsValue)) {

				@SuppressWarnings("unchecked")
				@Override
				public Map<String, Object> mapRow(final ResultSet rs, final int rowNum)
						throws SQLException {
					final String key = toUniqueString(rs);
					if (key == null) {
						return mapRowData(columns, rs);
					}
					Object data = getCache(key);
					if (!(data instanceof Map)) {
						if ((data = mapRowData(columns, rs)) != null) {
							putCache(key, data);
						}
					} else if (columns != null && columns.length > 0
							&& !((Map<String, Object>) data).containsKey(columnKey(columns[0]))) {
						final Map<String, Object> nData = mapRowData(columns, rs);
						if (nData != null) {
							((Map<String, Object>) data).putAll(nData);
						}
					}
					return (Map<String, Object>) data;
				}
			};
		}
	}

	/************************* ope ************************/

	@Override
	public int execute(final IEntityListener l, final SQLValue... sqlValues) {
		final int ret = super.execute(l, sqlValues);
		boolean delete = false;
		for (final SQLValue sqlValue : sqlValues) {
			if (sqlValue.getSql().trim().toLowerCase().startsWith("delete")) {
				delete = true;
				break;
			}
		}
		if (delete) {
			reset();
		}
		return ret;
	}

	@Override
	public Object exchange(final Object o1, final Object o2, final DbColumn order, final boolean up) {
		final long[] ret = (long[]) super.exchange(o1, o2, order, up);
		if (ret == null) {
			return null;
		}
		final String orderName = order.getColumnSqlName();
		final StringBuilder sb = new StringBuilder();
		sb.append(orderName).append(">=? and ").append(orderName).append("<=?");
		try {
			final IQueryEntitySet<Map<String, Object>> qs = query(new ExpressionValue(sb.toString(),
					ret[0], ret[1]));
			Map<String, Object> mObj;
			while ((mObj = qs.next()) != null) {
				final String key = toUniqueString(mObj);
				if (key != null) {
					removeCache(key);
				}
			}
			return ret;
		} catch (final Throwable th) {
			reset();
			throw DataAccessException.of(th);
		}
	}

	/************************* transaction ************************/

	private static ThreadLocal<Map<String, ITableEntityCache>> entityCache;
	static {
		entityCache = new ThreadLocal<Map<String, ITableEntityCache>>();
	}

	@Override
	public <T> T doExecuteTransaction(final ITransactionCallback<T> callback) {
		Connection connection = null;
		try {
			connection = TransactionUtils.begin(dataSource);
			entityCache.set(new HashMap<String, ITableEntityCache>());
			final T t = callback.doTransactionCallback();
			connection.commit();
			return t;
		} catch (final Throwable th) {
			try {
				connection.rollback();
			} catch (final SQLException e2) {
				log.warn(e2);
			}

			// 错误时，清除cache
			final Map<String, ITableEntityCache> cache = entityCache.get();
			if (cache != null) {
				for (final Map.Entry<String, ITableEntityCache> entry : cache.entrySet()) {
					entry.getValue().removeCache(entry.getKey());
				}
			}
			log.warn(th);
			throw DataAccessException.of(th);
		} finally {
			TransactionUtils.end(connection);
			entityCache.remove();
		}
	}
}
