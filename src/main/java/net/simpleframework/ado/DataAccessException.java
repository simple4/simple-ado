package net.simpleframework.ado;

import net.simpleframework.common.SimpleRuntimeException;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class DataAccessException extends SimpleRuntimeException {

	public DataAccessException(final String msg, final Throwable cause) {
		super(msg, cause);
	}

	public static DataAccessException of(final String message) {
		return _of(DataAccessException.class, message, null);
	}

	public static DataAccessException of(final Throwable cause) {
		return _of(DataAccessException.class, null, cause);
	}

	private static final long serialVersionUID = -539640491680179667L;
}
