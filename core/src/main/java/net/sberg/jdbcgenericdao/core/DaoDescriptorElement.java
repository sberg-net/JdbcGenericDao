package net.sberg.jdbcgenericdao.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DaoDescriptorElement {
    public String dbProperty() default DaoDescriptorHelper.unknown;
    public boolean notNull() default false;
}
