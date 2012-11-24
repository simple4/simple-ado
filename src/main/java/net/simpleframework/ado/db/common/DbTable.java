package net.simpleframework.ado.db.common;

import java.io.Serializable;

import net.simpleframework.common.StringUtils;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public class DbTable implements Serializable {

	private final String name;

	private final String[] uniqueColumns;

	private boolean noCache;

	private DbColumn defaultOrder;

	public DbTable(final String name) {
		this(name, "id");
	}

	public DbTable(final String name, final boolean noCache) {
		this(name);
		this.noCache = noCache;
	}

	public DbTable(final String name, final String uniqueColumn) {
		this(name, new String[] { uniqueColumn });
	}

	public DbTable(final String name, final String[] uniqueColumns) {
		this.name = name;
		this.uniqueColumns = uniqueColumns;
	}

	public String getName() {
		return name;
	}

	public String[] getUniqueColumns() {
		return uniqueColumns;
	}

	public boolean isNoCache() {
		return noCache;
	}

	public DbTable setNoCache(final boolean noCache) {
		this.noCache = noCache;
		return this;
	}

	public DbColumn getDefaultOrder() {
		return defaultOrder;
	}

	public DbTable setDefaultOrder(final DbColumn defaultOrder) {
		this.defaultOrder = defaultOrder;
		return this;
	}

	@Override
	public String toString() {
		return name + ", unique[" + StringUtils.join(uniqueColumns, "-") + "]";
	}

	private static final long serialVersionUID = -6445073606291514860L;
}
