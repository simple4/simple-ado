package net.simpleframework.ado.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.ado.db.common.DbColumn;
import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.common.SqlUtils;
import net.simpleframework.common.Convert;
import net.simpleframework.common.ID;
import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.EOrder;
import net.simpleframework.common.bean.BeanUtils;
import net.simpleframework.common.coll.KVMap;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class SQLBuilder {

	private static StringBuilder buildSelectSQL(final StringBuilder sb, final DbTable dbTable,
			final Object[] columns) {
		sb.append("select ");
		if (columns == null || columns.length == 0) {
			sb.append("*");
		} else {
			int i = 0;
			for (final Object column : columns) {
				if (column == null) {
					continue;
				}
				if (i++ > 0) {
					sb.append(",");
				}
				if (column instanceof DbColumn) {
					final DbColumn col = (DbColumn) column;
					sb.append(col.getColumnSqlName());
					final String text = col.getColumnText();
					if (StringUtils.hasText(text)) {
						sb.append(" as ").append(text);
					}
				} else {
					sb.append(Convert.toString(column));
				}
			}
		}
		sb.append(" from ").append(dbTable.getName());
		return sb;
	}

	private static StringBuilder buildDeleteSQL(final StringBuilder sb, final DbTable dbTable) {
		return sb.append("delete from ").append(dbTable.getName());
	}

	static StringBuilder buildUniqueColumns(final StringBuilder sb, final DbTable dbTable) {
		final Object[] columns = dbTable.getUniqueColumns();
		for (int i = 0; i < columns.length; i++) {
			if (i > 0) {
				sb.append(" and ");
			}
			sb.append(columns[i]).append("=?");
		}
		return sb;
	}

	private static String trimExpression(String expression) {
		if (expression != null) {
			expression = SqlUtils.trimSQL(expression);
			if (expression.toLowerCase().startsWith("where")) {
				expression = expression.substring(5).trim();
			}
		}
		return expression;
	}

	public static String getSelectUniqueSQL(final DbTable dbTable, final Object[] columns) {
		final StringBuilder sb = new StringBuilder();
		buildSelectSQL(sb, dbTable, columns);
		sb.append(" where ");
		buildUniqueColumns(sb, dbTable);
		return sb.toString();
	}

	public static String getSelectExpressionSQL(final DbTable dbTable, final Object[] columns,
			String expression) {
		final StringBuilder sb = new StringBuilder();
		buildSelectSQL(sb, dbTable, columns);
		expression = trimExpression(expression);
		if (StringUtils.hasText(expression)) {
			sb.append(" where ").append(expression);
			final DbColumn col = dbTable.getDefaultOrder();
			if (col != null) {
				if (expression.toLowerCase().indexOf("order by") == -1) {
					sb.append(" order by ").append(col.getColumnSqlName());
					final EOrder o = col.getOrder();
					if (o != EOrder.normal) {
						sb.append(" ").append(o);
					}
				}
			}
		}
		return sb.toString();
	}

	public static String getDeleteUniqueSQL(final DbTable dbTable) {
		final StringBuilder sb = new StringBuilder();
		buildDeleteSQL(sb, dbTable);
		sb.append(" where ");
		buildUniqueColumns(sb, dbTable);
		return sb.toString();
	}

	public static String getDeleteExpressionSQL(final DbTable dbTable, String expression) {
		final StringBuilder sb = new StringBuilder();
		buildDeleteSQL(sb, dbTable);
		expression = trimExpression(expression);
		if (StringUtils.hasText(expression)) {
			sb.append(" where ").append(expression);
		}
		return sb.toString();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Map toMapData(final DbTable dbTable, final Object object) {
		Map data = null;
		if (object instanceof Map) {
			data = (Map) object;
		} else {
			final Map<String, DbColumn> dbColumns = DbColumn.dbColumns(object.getClass());
			data = new KVMap(dbColumns.size()).setCaseInsensitive(true);
			for (final Map.Entry<String, DbColumn> entry : dbColumns.entrySet()) {
				final String propertyName = entry.getKey();
				final DbColumn dbColumn = entry.getValue();
				Object vObject = BeanUtils.getProperty(object, propertyName);
				if (vObject == null && ID.class.isAssignableFrom(dbColumn.getPropertyClass())) {
					vObject = ID.nullId;
				}
				data.put(dbColumn.getColumnName(), vObject);
			}
		}
		return data;
	}

	public static SQLValue getInsertSQLValue(final DbTable dbTable, final Object object) {
		final StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(dbTable.getName()).append("(");
		final Map<?, ?> data = toMapData(dbTable, object);
		final int size = data.size();
		if (data == null || size == 0) {
			return null;
		}
		sb.append(StringUtils.join(data.keySet(), ","));
		final Object[] values = data.values().toArray();
		sb.append(") values(");
		for (int i = 0; i < size; i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append("?");
		}
		sb.append(")");
		return new SQLValue(sb.toString(), values);
	}

	public static SQLValue getUpdateSQLValue(final DbTable dbTable, final Object[] columns,
			final Object object) {
		final Object[] uniqueColumns = dbTable.getUniqueColumns();
		if (uniqueColumns == null || uniqueColumns.length == 0) {
			return null;
		}
		final List<Object> vl = new ArrayList<Object>();
		final StringBuilder sb = new StringBuilder();
		sb.append("update ").append(dbTable.getName()).append(" set ");
		final Map<?, ?> data = toMapData(dbTable, object);
		if (data == null || data.size() == 0) {
			return null;
		}

		final Collection<?> coll = (columns != null && columns.length > 0) ? Arrays.asList(columns)
				: data.keySet();
		int i = 0;
		for (final Object key : coll) {
			if (i++ > 0) {
				sb.append(",");
			}
			sb.append(key).append("=?");
			vl.add(data.get(key));
		}
		sb.append(" where ");
		buildUniqueColumns(sb, dbTable);
		for (final Object column : uniqueColumns) {
			final Object value = data.get(column);
			if (value == null) {
				throw DataAccessException.of("");
			}
			vl.add(value);
		}
		return new SQLValue(sb.toString(), vl.toArray());
	}
}
