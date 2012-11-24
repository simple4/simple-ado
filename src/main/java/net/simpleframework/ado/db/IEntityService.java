package net.simpleframework.ado.db;

import javax.sql.DataSource;

import net.simpleframework.ado.IDataService;
import net.simpleframework.ado.db.common.IBatchValueSetter;
import net.simpleframework.ado.db.common.IConnectionCallback;
import net.simpleframework.ado.db.common.IQueryExtractor;
import net.simpleframework.ado.db.common.ITransactionCallback;
import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.ado.db.event.IEntityListener;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface IEntityService extends IDataService {
	DataSource getDataSource();

	<T> T execute(IConnectionCallback<T> connection);

	/* query */
	<T> T executeQuery(SQLValue value, IQueryExtractor<T> extractor, int resultSetType);

	<T> T executeQuery(SQLValue value, IQueryExtractor<T> extractor);

	/* update */
	int execute(IEntityListener l, SQLValue... sqlValues);

	int execute(SQLValue... sqlValues);

	int executeTransaction(IEntityListener l, SQLValue... sqlValues);

	int executeTransaction(SQLValue... sqlValues);

	int[] batchUpdate(String... sql);

	int[] batchUpdate(String sql, int batchCount, IBatchValueSetter setter);

	/**
	 * Transaction
	 * 
	 * @param callback
	 * @return
	 */
	<T> T doExecuteTransaction(ITransactionCallback<T> callback);
}
