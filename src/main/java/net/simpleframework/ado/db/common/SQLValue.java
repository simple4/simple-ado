package net.simpleframework.ado.db.common;

import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.IParamsValue.AbstractParamsValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class SQLValue extends AbstractParamsValue {
	private static final long serialVersionUID = -293936736340487065L;

	private String sql;

	public SQLValue(final String sql) {
		this.sql = sql;
	}

	public SQLValue(final String sql, final Object... values) {
		this.sql = sql;
		setValues(values);
	}

	public String getSql() {
		return sql;
	}

	public void setSql(final String sql) {
		this.sql = sql;
	}

	@Override
	public String getKey() {
		final StringBuffer sb = new StringBuffer();
		sb.append(StringUtils.hash(sql)).append("-").append(valuesToString());
		return sb.toString();
	}
}
