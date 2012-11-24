package net.simpleframework.ado.db.common;

import javax.sql.DataSource;

import net.simpleframework.common.ClassUtils;
import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.EOrder;
import net.simpleframework.common.logger.Log;
import net.simpleframework.common.logger.LogFactory;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class SqlUtils {
	static Log log = LogFactory.getLogger(SqlUtils.class);

	public static String sqlEscape(final String aString) {
		if (aString == null) {
			return "";
		}
		if (aString.indexOf("'") == -1) {
			return aString;
		}
		final StringBuilder aBuffer = new StringBuilder(aString);
		int insertOffset = 0;
		for (int i = 0; i < aString.length(); i++) {
			if (aString.charAt(i) == '\'') {
				aBuffer.insert(i + insertOffset++, "'");
			}
		}
		return aBuffer.toString();
	}

	public static String trimSQL(final String sql) {
		return sql == null ? "" : sql.trim().replaceAll("  +", " ").replaceAll(" *, *", ",");
	}

	public static String getLocSelectSQL(final DataSource dataSource, final String sql, final int i,
			final int fetchSize) {
		final String db = JdbcUtils.getDatabaseMetaData(dataSource).dbName();
		final StringBuilder sb = new StringBuilder();
		if (db.equals(SqlConstants.Oracle)) {
			sb.append("select * from (select ROWNUM as rn, t_orcl.* from (");
			sb.append(sql).append(") t_orcl) where rn > ").append(i).append(" and rn <= ")
					.append(i + fetchSize);
			return sb.toString();
		} else if (db.equals(SqlConstants.MySQL)) {
			sb.append(sql).append(" limit ");
			sb.append(i).append(",").append(fetchSize);
			return sb.toString();
		} else if (db.equals(SqlConstants.HSQL)) {
			if (sql.trim().toLowerCase().startsWith("select")) {
				sb.append("select limit ").append(i).append(" ").append(fetchSize)
						.append(sql.substring(6));
				return sb.toString();
			}
		}
		return sql;
	}

	public static String getIdsSQLParam(final String idColumnName, final int size) {
		final StringBuilder sb = new StringBuilder();
		sb.append(idColumnName);
		if (size == 1) {
			sb.append(" = ?");
		} else {
			sb.append(" in (");
			for (int i = 0; i < size; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append("?");
			}
			sb.append(")");
		}
		return sb.toString();
	}

	/*------------------------------SqlParser----------------------------------*/

	private static boolean sqlParser;
	static {
		try {
			ClassUtils.forName("com.alibaba.druid.sql.ast.SQLExpr");
			sqlParser = true;
		} catch (final ClassNotFoundException e) {
			log.warn(e);
		}
	}

	// ------- Order By

	public static String addOrderBy(final DataSource dataSource, final String sql,
			final DbColumn... orderBy) {
		if (sqlParser) {
			return net.simpleframework.ado.db.common.JSqlParser.addOrderBy(sql, JdbcUtils
					.getDatabaseMetaData(dataSource).dbName(), orderBy);
		}
		return _addOrderBy(sql, orderBy);
	}

	static String _addOrderBy(final String sql, final DbColumn... columns) {
		final StringBuilder sb = new StringBuilder();
		sb.append("select * from (").append(sql).append(") t_order_by order by ");
		int i = 0;
		for (final DbColumn dbColumn : columns) {
			if (dbColumn.getOrder() == EOrder.normal) {
				continue;
			}
			if (i++ > 0) {
				sb.append(",");
			}
			sb.append(dbColumn).append(" ").append(dbColumn.getOrder());
		}
		return sb.toString();
	}

	// ------- Condition

	public static String addCondition(final DataSource dataSource, final String sql,
			final String condition) {
		if (sqlParser) {
			return net.simpleframework.ado.db.common.JSqlParser.addCondition(sql, JdbcUtils
					.getDatabaseMetaData(dataSource).dbName(), condition);
		}
		return _addCondition(sql, condition);
	}

	static String _addCondition(final String sql, final String condition) {
		if (!StringUtils.hasText(condition)) {
			return sql;
		}
		final StringBuilder sb = new StringBuilder();
		sb.append("select * from (").append(sql).append(") t_condition where ").append(condition);
		return sb.toString();
	}

	// ------- Count

	public static String wrapCount(final DataSource dataSource, final String sql) {
		if (sqlParser) {
			return net.simpleframework.ado.db.common.JSqlParser.wrapCount(sql, JdbcUtils
					.getDatabaseMetaData(dataSource).dbName());
		}
		return _wrapCount(sql);
	}

	static String _wrapCount(final String sql) {
		final StringBuilder sb = new StringBuilder();
		sb.append("select count(*) from (").append(sql).append(") t_count");
		return sb.toString();
	}

	public static String format(final DataSource dataSource, final String sql) {
		if (sqlParser) {
			return net.simpleframework.ado.db.common.JSqlParser.format(sql, JdbcUtils
					.getDatabaseMetaData(dataSource).dbName());
		}
		return sql;
	}
}
