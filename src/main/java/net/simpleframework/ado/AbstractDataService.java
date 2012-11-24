package net.simpleframework.ado;

import java.util.Collection;
import java.util.LinkedHashSet;

import net.simpleframework.common.ObjectEx;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class AbstractDataService extends ObjectEx implements IDataService {

	@Override
	public void reset() {
	}

	/**
	 * 监听器
	 */
	private Collection<IDataServiceListener> listeners;

	@Override
	public Collection<IDataServiceListener> getListeners() {
		if (listeners == null) {
			listeners = new LinkedHashSet<IDataServiceListener>();
		}
		return listeners;
	}

	@Override
	public void addListener(final IDataServiceListener listener) {
		getListeners().add(listener);
	}

	@Override
	public boolean removeListener(final IDataServiceListener listener) {
		return getListeners().remove(listener);
	}
}
