package net.simpleframework.ado.db.common;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.simpleframework.common.StringUtils;
import net.simpleframework.common.ado.EOrder;
import net.simpleframework.common.bean.BeanUtils;
import net.simpleframework.common.coll.LRUMap;
import net.simpleframework.common.logger.Log;
import net.simpleframework.common.logger.LogFactory;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.parser.SQLParserUtils;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public abstract class JSqlParser {
	private static Log log = LogFactory.getLogger(JSqlParser.class);

	private static void warn(final Throwable e, final String sql) {
		log.info(e.getMessage() + "\nSQL: [" + sql + "]");
	}

	private static Map<String, String> mSQL = Collections
			.synchronizedMap(new LRUMap<String, String>(1000));

	public static String wrapCount(final String selectSql, final String db) {
		final String key = SqlUtils._wrapCount(selectSql);
		String nsql = mSQL.get(key);
		if (nsql != null) {
			return nsql;
		}
		try {
			final SQLSelect sqlSelect = SQLParserUtils.createSQLStatementParser(selectSql, db)
					.parseSelect().getSelect();
			sqlSelect.setOrderBy(null);
			final SQLSelectQueryBlock qBlock = (SQLSelectQueryBlock) sqlSelect.getQuery();
			if (qBlock.getGroupBy() == null) {
				final List<SQLSelectItem> items = qBlock.getSelectList();
				boolean aggregate = false;
				for (final SQLSelectItem item : items) {
					if (item.getExpr() instanceof SQLAggregateExpr) {
						aggregate = true;
						break;
					}
				}
				if (aggregate) {
					final SQLAggregateExpr count = new SQLAggregateExpr("count");
					count.getArguments().add(new SQLAllColumnExpr());
					items.clear();
					items.add(new SQLSelectItem(count));
					try {
						BeanUtils.setProperty(qBlock, "orderBy", null);
					} catch (final Exception e) {
					}
					nsql = SQLUtils.toSQLString(sqlSelect, db);
				}
			}
		} catch (final Exception e) {
			warn(e, selectSql);
		}
		mSQL.put(key, nsql == null ? (nsql = key) : nsql);
		return nsql;
	}

	public static String addOrderBy(final String selectSql, final String db,
			final DbColumn... columns) {
		if (columns == null || columns.length == 0) {
			return selectSql;
		}
		final String key = SqlUtils._addOrderBy(selectSql, columns);
		String nsql = mSQL.get(key);
		if (nsql != null) {
			return nsql;
		}
		try {
			final SQLSelect sqlSelect = SQLParserUtils.createSQLStatementParser(selectSql, db)
					.parseSelect().getSelect();
			final SQLSelectQueryBlock qBlock = (SQLSelectQueryBlock) sqlSelect.getQuery();
			SQLOrderBy orderBy = null;
			if (BeanUtils.hasProperty(qBlock, "orderBy")) {
				orderBy = (SQLOrderBy) BeanUtils.getProperty(qBlock, "orderBy");
				if (orderBy == null) {
					BeanUtils.setProperty(qBlock, "orderBy", orderBy = new SQLOrderBy());
				}
			} else {
				orderBy = sqlSelect.getOrderBy();
				if (orderBy == null) {
					sqlSelect.setOrderBy(orderBy = new SQLOrderBy());
				}
			}

			final List<SQLSelectOrderByItem> items = orderBy.getItems();
			for (int i = columns.length - 1; i >= 0; i--) {
				final DbColumn dbColumn = columns[i];
				final SQLExpr expr = new SQLIdentifierExpr(dbColumn.getColumnSqlName());
				SQLExpr expr2 = expr;
				final DbTable dbTable = dbColumn.getTable();
				if (dbTable != null) {
					expr2 = new SQLPropertyExpr(expr, dbTable.getName());
				}

				SQLSelectOrderByItem item = null;
				for (final SQLSelectOrderByItem o : items) {
					final SQLExpr e = o.getExpr();
					if (e.equals(expr) || e.equals(expr2)) {
						item = o;
						break;
					}
				}
				if (item == null) {
					item = new SQLSelectOrderByItem();
				} else {
					items.remove(item);
				}

				item.setExpr(expr2);
				item.setType(dbColumn.getOrder() == EOrder.asc ? SQLOrderingSpecification.ASC
						: SQLOrderingSpecification.DESC);
				items.add(0, item);
			}
			nsql = SQLUtils.toSQLString(sqlSelect, db);
		} catch (final Exception e) {
			warn(e, selectSql);
		}
		mSQL.put(key, nsql == null ? (nsql = key) : nsql);
		return nsql;
	}

	public static String addCondition(final String selectSql, final String db, final String condition) {
		if (!StringUtils.hasText(condition)) {
			return selectSql;
		}

		final String key = SqlUtils._addCondition(selectSql, condition);
		String nsql = mSQL.get(key);
		if (nsql != null) {
			return nsql;
		}
		try {
			final SQLSelect sqlSelect = SQLParserUtils.createSQLStatementParser(selectSql, db)
					.parseSelect().getSelect();
			final SQLSelectQueryBlock qBlock = (SQLSelectQueryBlock) sqlSelect.getQuery();
			final SQLExpr expr = SQLParserUtils.createExprParser(condition, db).expr();
			if (qBlock.getWhere() == null) {
				qBlock.setWhere(expr);
			} else {
				qBlock.setWhere(new SQLBinaryOpExpr(qBlock.getWhere(), SQLBinaryOperator.BooleanAnd,
						expr));
			}
			nsql = SQLUtils.toSQLString(sqlSelect, db);
		} catch (final Exception e) {
			warn(e, selectSql);
		}
		mSQL.put(key, nsql == null ? (nsql = key) : nsql);
		return nsql;
	}

	public static String format(final String sql, final String db) {
		return SQLUtils.format(sql, db);
	}
}
