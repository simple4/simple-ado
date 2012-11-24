package net.simpleframework.ado.db;

import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.ado.db.common.ExpressionValue;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.common.SqlUtils;
import net.simpleframework.common.ado.IParamsValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class QueryDialect {
	IQueryEntitySet<?> qs;

	private SQLValue countSQL;

	public void setCountSQL(final IParamsValue countSQL, final DbTable dbTable) {
		final Class<?> c = countSQL.getClass();
		if (SQLValue.class.equals(c)) {
			this.countSQL = (SQLValue) countSQL;
		} else if (dbTable != null && ExpressionValue.class.equals(c)) {
			final ExpressionValue ev = (ExpressionValue) countSQL;
			final StringBuilder sb = new StringBuilder();
			sb.append("select count(*) from ").append(dbTable.getName()).append(" where ")
					.append(ev.getExpression());
			this.countSQL = new SQLValue(sb.toString(), ev.getValues());
		}
	}

	public SQLValue getCountSQL() {
		return countSQL;
	}

	public String getNativeSQLValue(final String sql, final int i, final int fetchSize) {
		return SqlUtils.getLocSelectSQL(qs.getDataSource(), sql, i, fetchSize);
	}
}
