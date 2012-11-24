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
public abstract class TableEntityAdapter extends AbstractEntityListener implements
		ITableEntityListener {

	@Override
	public void beforeInsert(final ITableEntityService service, final Object[] beans) {
	}

	@Override
	public void afterInsert(final ITableEntityService service, final Object[] beans) {
		afterEvent(service, beans);
	}

	@Override
	public void beforeUpdate(final ITableEntityService service, final Object[] beans) {
	}

	@Override
	public void afterUpdate(final ITableEntityService service, final Object[] beans) {
		afterEvent(service, beans);
	}

	@Override
	public void beforeDelete(final ITableEntityService service, final IParamsValue paramsValue) {
	}

	@Override
	public void afterDelete(final ITableEntityService service, final IParamsValue paramsValue) {
		afterEvent(service, paramsValue);
	}
}
