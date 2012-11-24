package net.simpleframework.ado.db;

import javax.sql.DataSource;

import net.simpleframework.ado.db.common.SQLValue;
import net.simpleframework.common.ado.query.IDataQuery;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface IQueryEntitySet<T> extends IDataQuery<T> {

	DataSource getDataSource();

	SQLValue getSqlValue();

	QueryDialect getQueryDialect();

	void setQueryDialect(QueryDialect queryDialect);

	int getResultSetType();

	void setResultSetType(int resultSetType);
}
