package net.simpleframework.ado.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import net.simpleframework.ado.db.common.DbColumn;
import net.simpleframework.ado.db.common.JdbcUtils;
import net.simpleframework.common.ObjectEx;
import net.simpleframework.common.bean.BeanUtils;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class BeanWrapper<T> extends ObjectEx {

	private final Collection<PropertyCache> collection;

	private final Class<T> beanClass;

	public BeanWrapper(final String[] columns, final Class<T> beanClass) {
		this.beanClass = beanClass;
		final Set<String> fields = BeanUtils.fields(beanClass);
		collection = new ArrayList<PropertyCache>(fields.size());

		for (final String field : fields) {
			final DbColumn col = DbColumn.dbColumns(beanClass).get(field);
			if (col == null) {
				continue;
			}
			if (columns != null && columns.length > 0) {
				boolean find = false;
				for (final String column : columns) {
					if (col.getColumnName().equals(column) || col.getColumnSqlName().equals(column)) {
						find = true;
						break;
					}
				}
				if (!find) {
					continue;
				}
			}
			final PropertyCache cache = new PropertyCache();
			cache.propertyName = field;
			cache.dbColumn = col;
			collection.add(cache);
		}
	}

	public T toBean(final ResultSet rs) throws SQLException {
		T bean = null;
		try {
			bean = beanClass.newInstance();
		} catch (final Exception e) {
			log.error(e);
			return null;
		}

		for (final PropertyCache cache : collection) {
			if (cache.sqlColumnIndex <= 0) {
				final int sqlColumnIndex = JdbcUtils.lookupColumnIndex(rs.getMetaData(),
						cache.dbColumn.getColumnName());
				if (sqlColumnIndex <= 0) {
					continue;
				} else {
					cache.sqlColumnIndex = sqlColumnIndex;
				}
			}

			final Class<?> propertyType = cache.dbColumn.getPropertyClass();
			final Object object = JdbcUtils.getResultSetValue(rs, cache.sqlColumnIndex, propertyType);
			if (object == null) {
				continue;
			}
			BeanUtils.setProperty(bean, cache.propertyName, object);
		}
		return bean;
	}

	private class PropertyCache {
		String propertyName;

		DbColumn dbColumn;

		int sqlColumnIndex;
	}
}
