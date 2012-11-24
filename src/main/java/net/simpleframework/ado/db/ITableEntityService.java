package net.simpleframework.ado.db;

import java.util.Map;

import net.simpleframework.ado.db.common.DbColumn;
import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.ado.db.event.ITableEntityListener;
import net.simpleframework.common.ado.IParamsValue;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface ITableEntityService extends IEntityService {

	DbTable getTable();

	/* select single */

	Map<String, Object> queryMap(IParamsValue paramsValue);

	Map<String, Object> queryMap(String[] columns, IParamsValue paramsValue);

	<T> T queryBean(IParamsValue paramsValue, Class<T> beanClass);

	<T> T queryBean(String[] columns, IParamsValue paramsValue, Class<T> beanClass);

	<T> T getBean(Object id, Class<T> beanClass);

	<T> T getBean(String[] columns, Object id, Class<T> beanClass);

	/* select multi */

	IQueryEntitySet<Map<String, Object>> query(IParamsValue paramsValue);

	IQueryEntitySet<Map<String, Object>> query(String[] columns, IParamsValue paramsValue);

	<T> IQueryEntitySet<T> query(IParamsValue paramsValue, Class<T> beanClass);

	<T> IQueryEntitySet<T> query(String[] columns, IParamsValue paramsValue, Class<T> beanClass);

	/* update */

	int update(String[] columns, Object... beans);

	int update(Object... beans);

	int updateTransaction(String[] columns, Object... beans);

	int updateTransaction(ITableEntityListener l, String[] columns, Object... beans);

	int updateTransaction(Object... beans);

	int updateTransaction(ITableEntityListener l, Object... beans);

	/* insert */

	int insert(Object... beans);

	int insertTransaction(ITableEntityListener l, Object... beans);

	int insertTransaction(Object... beans);

	/* delete */

	int delete(IParamsValue paramsValue);

	int deleteTransaction(ITableEntityListener l, IParamsValue paramsValue);

	int deleteTransaction(IParamsValue paramsValue);

	/* utils */

	int count(IParamsValue paramsValue);

	int sum(String column, IParamsValue paramsValue);

	int max(String column, IParamsValue paramsValue);

	Object exchange(Object bean1, Object bean2, DbColumn order, boolean up);
}
