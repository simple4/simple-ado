package net.simpleframework.ado.db;

import java.util.Map;

import net.simpleframework.ado.db.common.SQLValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface IQueryEntityService extends IEntityService {

	/* select single */
	Map<String, Object> queryForMap(String sql);

	Map<String, Object> queryForMap(SQLValue value);

	long queryForLong(SQLValue value);

	int queryForInt(SQLValue value);

	/* select multi */
	IQueryEntitySet<Map<String, Object>> query(SQLValue value);

	IQueryEntitySet<Map<String, Object>> query(String sql);

	<T> IQueryEntitySet<T> query(SQLValue value, Class<T> beanClass);
}
