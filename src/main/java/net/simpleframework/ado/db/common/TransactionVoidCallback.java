package net.simpleframework.ado.db.common;

import net.simpleframework.ado.DataAccessException;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class TransactionVoidCallback extends TransactionObjectCallback<Object> {

	@Override
	public Object doTransactionCallback() throws DataAccessException {
		doTransactionVoidCallback();
		return null;
	}

	protected abstract void doTransactionVoidCallback() throws DataAccessException;
}
