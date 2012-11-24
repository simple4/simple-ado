package net.simpleframework.ado.db.common;

import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.UniqueValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class ExpressionValue extends UniqueValue {
	private static final long serialVersionUID = 8251357074671323990L;

	private final String expression;

	public ExpressionValue(final String expression, final Object... values) {
		super(values);
		this.expression = StringUtils.text(expression, "1=1");
	}

	public String getExpression() {
		return expression;
	}

	@Override
	public String getKey() {
		final StringBuilder sb = new StringBuilder();
		final String expression = getExpression();
		if (StringUtils.hasText(expression)) {
			sb.append(expression).append("-");
		}
		sb.append(valuesToString());
		return sb.toString();
	}
}
