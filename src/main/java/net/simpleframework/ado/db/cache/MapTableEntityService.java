package net.simpleframework.ado.db.cache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.common.StringUtils;
import net.simpleframework.common.bean.IIdBeanAware;
import net.simpleframework.common.coll.LRUMap;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class MapTableEntityService extends AbstractCacheTableEntityService {

	private int maxCacheSize = 0;

	private Map<Object, Object> idCache, vCache;

	public MapTableEntityService(final DataSource dataSource, final DbTable dbTable) {
		super(dataSource, dbTable);
		setMaxCacheSize(0);
	}

	public int getMaxCacheSize() {
		return maxCacheSize;
	}

	public void setMaxCacheSize(final int maxCacheSize) {
		this.maxCacheSize = maxCacheSize;
		if (maxCacheSize > 0) {
			idCache = Collections.synchronizedMap(new LRUMap<Object, Object>(maxCacheSize));
			vCache = Collections.synchronizedMap(new LRUMap<Object, Object>(maxCacheSize));
		} else {
			idCache = new ConcurrentHashMap<Object, Object>();
			vCache = new ConcurrentHashMap<Object, Object>();
		}
	}

	@Override
	public synchronized void reset() {
		idCache.clear();
	}

	@Override
	public Object getCache(final String key) {
		final Object id = idCache.get(key);
		return id == null ? null : vCache.get(id);
	}

	@Override
	public void putCache(final String key, final Object val) {
		Object id = null;
		if (val instanceof IIdBeanAware) {
			id = (((IIdBeanAware) val).getId()).getValue();
		} else if (val instanceof Map) {
			id = ((Map<?, ?>) val).get("ID");
		}
		if (id == null) {
			id = StringUtils.hash(val);
		}
		idCache.put(key, id);
		vCache.put(id, val);
	}

	@Override
	public void removeCache(final String key) {
		idCache.remove(key);
	}
}
