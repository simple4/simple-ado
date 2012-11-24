package net.simpleframework.ado.db.common;

import java.sql.ResultSet;
import java.sql.SQLException;

import net.simpleframework.ado.DataAccessException;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface IQueryExtractor<T> {

	T extractData(ResultSet rs) throws SQLException, DataAccessException;
}
