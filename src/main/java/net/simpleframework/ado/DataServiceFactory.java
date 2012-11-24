package net.simpleframework.ado;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import net.simpleframework.ado.db.IQueryEntityService;
import net.simpleframework.ado.db.ITableEntityService;
import net.simpleframework.ado.db.QueryEntityService;
import net.simpleframework.ado.db.cache.MapTableEntityService;
import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.common.ObjectEx;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class DataServiceFactory extends ObjectEx {

	private final DataSource dataSource;

	public DataServiceFactory(final DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public ITableEntityService createEntityService(final DbTable dbTable) {
		return new MapTableEntityService(dataSource, dbTable);
	}

	private static Map<Class<?>, ITableEntityService> entityServiceCache;
	static {
		entityServiceCache = new ConcurrentHashMap<Class<?>, ITableEntityService>();
	}

	public DataServiceFactory putEntityService(final Class<?> beanClass, final DbTable dbTable) {
		entityServiceCache.put(beanClass, createEntityService(dbTable));
		return this;
	}

	public ITableEntityService getEntityService(final Class<?> beanClass) {
		for (final Map.Entry<Class<?>, ITableEntityService> e : entityServiceCache.entrySet()) {
			final Class<?> beanClass2 = e.getKey();
			if (beanClass2.equals(beanClass) || beanClass.isAssignableFrom(beanClass2)) {
				return e.getValue();
			}
		}
		return null;
	}

	public IQueryEntityService getQueryService() {
		return singleton(QueryEntityService.class, new ISingletonCallback<QueryEntityService>() {
			@Override
			public void onCreated(final QueryEntityService bean) {
				bean.setDataSource(dataSource);
			}
		});
	}
}
