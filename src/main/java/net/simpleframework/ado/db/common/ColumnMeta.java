package net.simpleframework.ado.db.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 这是一个开源的软件，请在LGPLv3下合法使用、修改或重新发布。
 * 
 * @author 陈侃(cknet@126.com, 13910090885)
 *         http://code.google.com/p/simpleframework/
 *         http://www.simpleframework.net
 */
@Target({ ElementType.METHOD /* get */})
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnMeta {

	String columnName() default "";

	String columnText() default "";

	String columnSqlName() default "";

	boolean ignore() default false;
}
