package net.simpleframework.ado.db.event;

import net.simpleframework.ado.db.ITableEntityService;
import net.simpleframework.common.ado.IParamsValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface ITableEntityListener extends IEntityListener {

	/* delete event */

	void beforeDelete(ITableEntityService service, IParamsValue paramsValue);

	void afterDelete(ITableEntityService service, IParamsValue paramsValue);

	/* insert event */

	void beforeInsert(ITableEntityService service, Object[] beans);

	void afterInsert(ITableEntityService service, Object[] beans);

	/* update event */

	void beforeUpdate(ITableEntityService service, Object[] beans);

	void afterUpdate(ITableEntityService service, Object[] beans);
}
