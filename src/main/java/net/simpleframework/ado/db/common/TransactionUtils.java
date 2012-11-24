package net.simpleframework.ado.db.common;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class TransactionUtils {

	private static ThreadLocal<Connection> connections;
	static {
		connections = new ThreadLocal<Connection>();
	}

	public static Connection begin(final DataSource dataSource) throws SQLException {
		final Connection connection = getConnection(dataSource);
		if (connection.getAutoCommit()) {
			connection.setAutoCommit(false);
		}
		connections.set(connection);
		return connection;
	}

	public static void end(final Connection connection) {
		connections.remove();
		JdbcUtils.closeAll(connection, null, null);
	}

	public static boolean inTrans(final Connection connection) {
		return connection == connections.get();
	}

	public static Connection getConnection(final DataSource dataSource) throws SQLException {
		Connection connection = connections.get();
		if (connection == null) {
			connection = dataSource.getConnection();
			if (!connection.getAutoCommit()) {
				connection.setAutoCommit(true);
			}
		}
		return connection;
	}
}
