package net.simpleframework.ado.db.common;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.ColumnData;
import net.simpleframework.common.ado.EOrder;
import net.simpleframework.common.bean.BeanUtils;
import net.simpleframework.lib.net.minidev.asm.Accessor;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class DbColumn extends ColumnData {
	public static DbColumn order = (DbColumn) new DbColumn("oorder").setOrder(EOrder.desc);

	private DbTable dbTable;

	/** 列为表达式函数? */
	private String columnSqlName;

	public DbColumn(final String columnName) {
		super(columnName, null);
	}

	public DbColumn(final String columnName, final String columnText) {
		super(columnName, columnText, null);
	}

	public DbColumn(final String columnName, final String columnText, final Class<?> propertyClass) {
		super(columnName, columnText, propertyClass);
	}

	public DbTable getTable() {
		return dbTable;
	}

	public void setTable(final DbTable dbTable) {
		this.dbTable = dbTable;
	}

	public String getColumnSqlName() {
		return StringUtils.text(columnSqlName, getColumnName());
	}

	public DbColumn setColumnSqlName(final String columnSqlName) {
		this.columnSqlName = columnSqlName;
		return this;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		final DbTable dbTable = getTable();
		if (dbTable != null) {
			sb.append(dbTable.getName()).append(".");
		}
		sb.append(getColumnSqlName());
		return sb.toString();
	}

	private final static Map<Class<?>, Map<String, DbColumn>> columnsCache = new ConcurrentHashMap<Class<?>, Map<String, DbColumn>>();

	public static Map<String, DbColumn> dbColumns(final Class<?> beanClass) {
		Map<String, DbColumn> data = columnsCache.get(beanClass);
		if (data != null) {
			return data;
		}
		data = new HashMap<String, DbColumn>();
		for (final Accessor accessor : BeanUtils.getBeansAccess(beanClass).getMap().values()) {
			final String propertyName = accessor.getName();
			final Method readMethod = accessor.getGetter();
			if (readMethod == null) {
				continue;
			}
			final ColumnMeta meta = readMethod.getAnnotation(ColumnMeta.class);
			if (meta != null && meta.ignore()) {
				continue;
			}
			String columnName;
			if (meta == null || !StringUtils.hasText(columnName = meta.columnName())) {
				columnName = propertyName;
			}
			final DbColumn col = new DbColumn(columnName);
			col.setPropertyClass(readMethod.getReturnType());
			if (meta != null) {
				String columnText, columnSqlName;
				if (StringUtils.hasText(columnText = meta.columnText())) {
					col.setColumnText(columnText);
				}
				if (StringUtils.hasText(columnSqlName = meta.columnSqlName())) {
					col.setColumnSqlName(columnSqlName);
				}
			}
			data.put(propertyName, col);
		}
		columnsCache.put(beanClass, data);
		return data;
	}

	public static String propertyName(final Class<?> beanClass, final String columnSqlName) {
		assert columnSqlName != null;
		for (final Map.Entry<String, DbColumn> entry : dbColumns(beanClass).entrySet()) {
			if (columnSqlName.equals(entry.getValue().getColumnName())) {
				return entry.getKey();
			}
		}
		return columnSqlName;
	}

	private static final long serialVersionUID = -241399268622218668L;
}
