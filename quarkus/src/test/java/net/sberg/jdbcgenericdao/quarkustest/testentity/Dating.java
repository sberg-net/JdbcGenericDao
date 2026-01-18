package net.sberg.jdbcgenericdao.quarkustest.testentity;

import lombok.Data;
import net.sberg.jdbcgenericdao.core.DaoDescriptorClass;
import net.sberg.jdbcgenericdao.core.DaoDescriptorElement;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@DaoDescriptorClass(dbTable = "Dating", primaryKey = "ID")
public class Dating {

    @DaoDescriptorElement(dbProperty = "ID", notNull = true)
    private Integer id;

    @DaoDescriptorElement
    private String desc;

    @DaoDescriptorElement
    private LocalDateTime dateTime;

    @DaoDescriptorElement
    private LocalDate date;
}
