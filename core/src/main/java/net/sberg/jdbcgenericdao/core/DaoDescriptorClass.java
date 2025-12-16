package net.sberg.jdbcgenericdao.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DaoDescriptorClass {
    public String dbTable() default DaoDescriptorHelper.unknown;
    public String primaryKey() default "id";
    public boolean transientBean() default false;
}
