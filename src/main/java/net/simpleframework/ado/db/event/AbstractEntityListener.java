package net.simpleframework.ado.db.event;

import net.simpleframework.ado.db.IEntityService;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.common.ObjectEx;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class AbstractEntityListener extends ObjectEx implements IEntityListener {

	@Override
	public void beforeExecute(final IEntityService service, final SQLValue[] sqlValues) {
	}

	@Override
	public void afterExecute(final IEntityService service, final SQLValue[] sqlValues) {
		afterEvent(service, sqlValues);
	}

	protected void afterEvent(final IEntityService service, final Object params) {
	}
}
