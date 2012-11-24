package net.simpleframework.ado.db.cache;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
public interface ITableEntityCache {

	/**
	 * 
	 * @param key
	 * @return
	 */
	Object getCache(String key);

	/**
	 * 
	 * @param key
	 * @param data
	 */
	void putCache(String key, Object data);

	/**
	 * 
	 * @param key
	 */
	void removeCache(String key);
}
