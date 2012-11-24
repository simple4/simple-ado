package net.simpleframework.ado.db.event;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.ado.IDataServiceListener;
import net.simpleframework.ado.db.IEntityService;
import net.simpleframework.ado.db.common.SQLValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface IEntityListener extends IDataServiceListener {

	void beforeExecute(IEntityService service, SQLValue[] sqlValues) throws DataAccessException;

	void afterExecute(IEntityService service, SQLValue[] sqlValues) throws DataAccessException;
}
