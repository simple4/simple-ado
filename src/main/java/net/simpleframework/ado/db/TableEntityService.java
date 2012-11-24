package net.simpleframework.ado.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import net.simpleframework.ado.DataAccessException;
import net.simpleframework.ado.IDataServiceListener;
import net.simpleframework.ado.db.common.DbColumn;
import net.simpleframework.ado.db.common.DbTable;
import net.simpleframework.ado.db.common.ExpressionValue;
import net.simpleframework.ado.db.common.IQueryExtractor;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.common.TransactionObjectCallback;
import net.simpleframework.ado.db.common.TransactionVoidCallback;
import net.simpleframework.ado.db.event.ITableEntityListener;
import net.simpleframework.common.ID;
import net.simpleframework.common.ado.EOrder;
import net.simpleframework.common.ado.IParamsValue;
import net.simpleframework.common.ado.UniqueValue;
import net.simpleframework.common.bean.BeanUtils;
import net.simpleframework.common.bean.IIdBeanAware;
import net.simpleframework.common.bean.IOrderBeanAware;
import net.simpleframework.common.coll.ArrayUtils;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class TableEntityService extends AbstractEntityService implements ITableEntityService {

	private final DbTable dbTable;

	public TableEntityService(final DataSource dataSource, final DbTable dbTable) {
		super(dataSource);
		this.dbTable = dbTable;
	}

	@Override
	public DbTable getTable() {
		return dbTable;
	}

	/* select for map */

	@Override
	public Map<String, Object> queryMap(final IParamsValue paramsValue) {
		return queryMap(null, paramsValue);
	}

	@Override
	public Map<String, Object> queryMap(final String[] columns, final IParamsValue paramsValue) {
		return createQueryMap(columns, createSQLValue(columns, paramsValue));
	}

	@Override
	public IQueryEntitySet<Map<String, Object>> query(final IParamsValue paramsValue) {
		return query((String[]) null, paramsValue);
	}

	protected SQLValue createSQLValue(final Object[] columns, final IParamsValue paramsValue) {
		if (paramsValue != null) {
			final Class<?> clazz = paramsValue.getClass();
			if (clazz.equals(SQLValue.class)) {
				return (SQLValue) paramsValue;
			} else if (clazz.equals(UniqueValue.class)) {
				return new SQLValue(SQLBuilder.getSelectUniqueSQL(getTable(), columns),
						paramsValue != null ? paramsValue.getValues() : null);
			} else if (clazz.equals(ExpressionValue.class)) {
				return new SQLValue(SQLBuilder.getSelectExpressionSQL(getTable(), columns,
						((ExpressionValue) paramsValue).getExpression()),
						paramsValue != null ? paramsValue.getValues() : null);
			}
		}
		return new SQLValue(SQLBuilder.getSelectExpressionSQL(getTable(), columns, null));
	}

	protected String getSelectUniqueSQL(final Object[] columns) {
		return SQLBuilder.getSelectUniqueSQL(getTable(), columns);
	}

	@Override
	public IQueryEntitySet<Map<String, Object>> query(final String[] columns,
			final IParamsValue paramsValue) {
		return createQueryEntitySet(columns, createSQLValue(columns, paramsValue));
	}

	/* select for object */

	@Override
	public <T> T queryBean(final IParamsValue paramsValue, final Class<T> beanClass) {
		return queryBean(null, paramsValue, beanClass);
	}

	@Override
	public <T> T getBean(final Object id, final Class<T> beanClass) throws DataAccessException {
		if (id == null) {
			return null;
		}
		final ID id2 = ID.Gen.id(id);
		return queryBean(new UniqueValue(id2.getValue()), beanClass);
	}

	@Override
	public <T> T queryBean(final String[] columns, final IParamsValue paramsValue,
			final Class<T> beanClass) {
		return createQueryObject(columns, createSQLValue(columns, paramsValue), beanClass);
	}

	@Override
	public <T> T getBean(final String[] columns, final Object id, final Class<T> beanClass)
			throws DataAccessException {
		Object id2;
		if (id instanceof ID) {
			id2 = ((ID) id).getValue();
		} else if (id instanceof IIdBeanAware) {
			id2 = ((IIdBeanAware) id).getId().getValue();
		} else {
			id2 = id;
		}
		return queryBean(columns, new UniqueValue(id2), beanClass);
	}

	@Override
	public <T> IQueryEntitySet<T> query(final IParamsValue ev, final Class<T> beanClass) {
		return query(null, ev, beanClass);
	}

	@Override
	public <T> IQueryEntitySet<T> query(final String[] columns, final IParamsValue paramsValue,
			final Class<T> beanClass) {
		return createQueryEntitySet(columns, createSQLValue(columns, paramsValue), beanClass);
	}

	/* delete */

	@Override
	public int delete(final IParamsValue paramsValue) {
		return delete(null, paramsValue);
	}

	protected int delete(final ITableEntityListener l, final IParamsValue paramsValue) {
		if (paramsValue == null) {
			return 0;
		}
		SQLValue sqlValue = null;
		final Class<?> clazz = paramsValue.getClass();
		if (clazz.equals(SQLValue.class)) {
			sqlValue = (SQLValue) paramsValue;
		} else if (clazz.equals(UniqueValue.class)) {
			sqlValue = new SQLValue(SQLBuilder.getDeleteUniqueSQL(getTable()), paramsValue.getValues());
		} else if (clazz.equals(ExpressionValue.class)) {
			sqlValue = new SQLValue(SQLBuilder.getDeleteExpressionSQL(getTable(),
					((ExpressionValue) paramsValue).getExpression()), paramsValue.getValues());
		}
		if (sqlValue == null) {
			return 0;
		}
		if (l != null) {
			l.beforeDelete(this, paramsValue);
		}
		for (final IDataServiceListener listener : getListeners()) {
			((ITableEntityListener) listener).beforeDelete(this, paramsValue);
		}
		final int ret = executeUpdate(sqlValue);
		if (l != null) {
			l.afterDelete(this, paramsValue);
		}
		for (final IDataServiceListener listener : getListeners()) {
			((ITableEntityListener) listener).afterDelete(this, paramsValue);
		}
		return ret;
	}

	@Override
	public int deleteTransaction(final ITableEntityListener l, final IParamsValue paramsValue) {
		return doExecuteTransaction(new TransactionObjectCallback<Integer>() {

			@Override
			public Integer doTransactionCallback() throws DataAccessException {
				return delete(l, paramsValue);
			}
		});
	}

	@Override
	public int deleteTransaction(final IParamsValue paramsValue) {
		return deleteTransaction(null, paramsValue);
	}

	/* insert */

	@Override
	public int insertTransaction(final Object... beans) {
		return insertTransaction(null, beans);
	}

	@Override
	public int insertTransaction(final ITableEntityListener l, final Object... beans) {
		return doExecuteTransaction(new TransactionObjectCallback<Integer>() {

			@Override
			public Integer doTransactionCallback() throws DataAccessException {
				return insert(l, beans);
			}
		});
	}

	@Override
	public int insert(final Object... beans) {
		return insert(null, beans);
	}

	protected int insert(final ITableEntityListener l, Object... beans) {
		beans = ArrayUtils.removeDuplicatesAndNulls(beans);
		if (beans == null) {
			return 0;
		}

		if (l != null) {
			l.beforeInsert(this, beans);
		}
		for (final IDataServiceListener listener : getListeners()) {
			((ITableEntityListener) listener).beforeInsert(this, beans);
		}

		int ret = 0;
		for (final Object bean : beans) {
			if (bean instanceof IIdBeanAware) {
				final IIdBeanAware idBean = (IIdBeanAware) bean;
				if (idBean.getId() == null) {
					idBean.setId(ID.Gen.uuid());
				}
				if (idBean instanceof IOrderBeanAware) {
					final IOrderBeanAware oBean = (IOrderBeanAware) bean;
					if (oBean.getOorder() == 0) {
						int max = max("oorder", null);
						oBean.setOorder(++max);
					}
				}
			}
			final SQLValue sqlValue = SQLBuilder.getInsertSQLValue(dbTable, bean);
			if (sqlValue != null) {
				ret += executeUpdate(sqlValue);
			}
		}

		if (l != null) {
			l.afterInsert(this, beans);
		}
		for (final IDataServiceListener listener : getListeners()) {
			((ITableEntityListener) listener).afterInsert(this, beans);
		}
		return ret;
	}

	/* update */

	@Override
	public int update(final Object... beans) {
		return update(null, beans);
	}

	@Override
	public int update(final String[] columns, final Object... beans) {
		return update(null, columns, beans);
	}

	protected int update(final ITableEntityListener l, final Object[] columns, Object... beans) {
		beans = ArrayUtils.removeDuplicatesAndNulls(beans);
		if (beans == null) {
			return 0;
		}
		if (l != null) {
			l.beforeUpdate(this, beans);
		}
		for (final IDataServiceListener listener : getListeners()) {
			((ITableEntityListener) listener).beforeUpdate(this, beans);
		}
		int ret = 0;
		for (final Object bean : beans) {
			final SQLValue sqlValue = SQLBuilder.getUpdateSQLValue(dbTable, columns, bean);
			if (sqlValue != null) {
				ret += executeUpdate(sqlValue);
			}
		}
		if (l != null) {
			l.afterUpdate(this, beans);
		}
		for (final IDataServiceListener listener : getListeners()) {
			((ITableEntityListener) listener).afterUpdate(this, beans);
		}
		return ret;
	}

	@Override
	public int updateTransaction(final ITableEntityListener l, final String[] columns,
			final Object... beans) {
		return doExecuteTransaction(new TransactionObjectCallback<Integer>() {

			@Override
			public Integer doTransactionCallback() throws DataAccessException {
				return update(l, columns, beans);
			}
		});
	}

	@Override
	public int updateTransaction(final String[] columns, final Object... beans) {
		return updateTransaction(null, columns, beans);
	}

	@Override
	public int updateTransaction(final ITableEntityListener l, final Object... beans) {
		return updateTransaction(l, null, beans);
	}

	@Override
	public int updateTransaction(final Object... beans) {
		return updateTransaction((ITableEntityListener) null, beans);
	}

	/* utils */

	@Override
	public int count(final IParamsValue paramsValue) {
		return query(paramsValue).getCount();
	}

	@Override
	public int sum(final String column, final IParamsValue paramsValue) {
		return function(column, "sum", paramsValue);
	}

	@Override
	public int max(final String column, final IParamsValue paramsValue) {
		return function(column, "max", paramsValue);
	}

	private int function(final String column, final String function, final IParamsValue paramsValue) {
		SQLValue sqlValue;
		if (paramsValue instanceof SQLValue) {
			sqlValue = (SQLValue) paramsValue;
		} else {
			final StringBuilder sql = new StringBuilder();
			sql.append("select ").append(function).append("(").append(column).append(") from ")
					.append(getTable().getName());
			if (paramsValue != null) {
				final Class<?> clazz = paramsValue.getClass();
				if (clazz.equals(UniqueValue.class)) {
					sql.append(" where ");
					SQLBuilder.buildUniqueColumns(sql, getTable());
				} else if (clazz.equals(ExpressionValue.class)) {
					sql.append(" where ").append(((ExpressionValue) paramsValue).getExpression());
				}
			}
			sqlValue = new SQLValue(sql.toString(), paramsValue != null ? paramsValue.getValues()
					: null);
		}
		return executeQuery(sqlValue, new IQueryExtractor<Integer>() {

			@Override
			public Integer extractData(final ResultSet rs) throws SQLException, DataAccessException {
				return rs.next() ? rs.getInt(1) : 0;
			}
		});
	}

	@Override
	public Object exchange(final Object bean1, final Object bean2, final DbColumn order,
			final boolean up) {
		if (bean1 == null || bean2 == null || order == null) {
			return null;
		}
		if (!bean1.getClass().equals(bean2.getClass())) {
			return null;
		}

		final String orderName;
		final long i1;
		final long i2;

		orderName = order.getColumnSqlName();
		i1 = ((Number) BeanUtils.getProperty(bean1, orderName)).longValue();
		i2 = ((Number) BeanUtils.getProperty(bean2, orderName)).longValue();
		if (i1 == i2) {
			return null;
		}

		final long max = Math.max(i1, i2);
		final long min = Math.min(i1, i2);
		doExecuteTransaction(new TransactionVoidCallback() {

			@Override
			protected void doTransactionVoidCallback() throws DataAccessException {
				final String tn = getTable().getName();
				final StringBuilder sb = new StringBuilder();
				sb.append("update ").append(tn).append(" set ");
				sb.append(orderName).append("=? where ").append(orderName).append("=?");
				final String sql = sb.toString();
				sb.setLength(0);
				sb.append("update ").append(tn).append(" set ").append(orderName);
				sb.append("=").append(orderName).append("+? where ");
				sb.append(orderName).append(">? and ").append(orderName).append("<?");
				final String sql2 = sb.toString();
				if (order.getOrder() == EOrder.asc && up) {
					executeUpdate(new SQLValue(sql, -1, min));
					executeUpdate(new SQLValue(sql, min, max));
					executeUpdate(new SQLValue(sql2, 1, min, max));
					executeUpdate(new SQLValue(sql, min + 1, -1));
				} else {
					executeUpdate(new SQLValue(sql, -1, max));
					executeUpdate(new SQLValue(sql, max, min));
					executeUpdate(new SQLValue(sql2, -1, min, max));
					executeUpdate(new SQLValue(sql, max - 1, -1));
				}
			}
		});
		return new long[] { min, max };
	}
}
