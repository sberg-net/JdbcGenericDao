package net.sberg.jdbcgenericdao.core;
import org.apache.commons.beanutils.PropertyUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcGenericDaoNativeJava {

    private final DataSource dataSource;

    public JdbcGenericDaoNativeJava(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private interface PreparedStatementFiller {
        void setValues(PreparedStatement ps) throws SQLException;
    }

    private interface RowMapperFn<T> {
        T mapRow(ResultSet rs, int rownum) throws SQLException;
    }

    private static class SelectRowMapper implements RowMapperFn<Object> {
        private final DaoDescriptorBean daoDescriptorBean;
        private final DaoProjectionBean daoProjectionBean;
        private SelectRowMapper(DaoProjectionBean daoProjectionBean, DaoDescriptorBean daoDescriptorBean) {
            this.daoDescriptorBean = daoDescriptorBean;
            this.daoProjectionBean = daoProjectionBean;
        }
        public Object mapRow(ResultSet rs, int rownum) throws SQLException {
            try {
                Object result;
                List<String> dbProperties;
                boolean resultIsMap = false;
                if (daoProjectionBean == null) {
                    result = Class.forName(daoDescriptorBean.getName(), false, Thread.currentThread().getContextClassLoader()).newInstance();
                    dbProperties = daoDescriptorBean.getAllDbProperties();
                }
                else {
                    if (!daoProjectionBean.isAtomar()) {
                        resultIsMap = daoProjectionBean.getResult().equals(Map.class);
                        result = daoProjectionBean.getResult().equals(Map.class)?new HashMap<>():daoProjectionBean.getResult().newInstance();
                        if (daoDescriptorBean != null) {
                            dbProperties = daoProjectionBean.getProperties().stream().map(o -> daoDescriptorBean.getProperties().get(o).getDbProperty()).collect(Collectors.toList());
                        }
                        else {
                            dbProperties = daoProjectionBean.getProperties();
                        }
                    }
                    else {
                        return rs.getObject(1);
                    }
                }
                String dbProperty;
                String property;
                DaoDescriptorProperty daoDescriptorProperty;
                Object value;
                for (int i = 0; i < dbProperties.size(); i++) {
                    if (daoDescriptorBean == null) {
                        value = rs.getObject(i + 1);
                        if (value != null && value.getClass().equals(Timestamp.class)) {
                            value = ((Timestamp) value).toLocalDateTime();
                        } else if (value != null && value.getClass().equals(Date.class)) {
                            value = ((Date) value).toLocalDate();
                        }
                        property = dbProperties.get(i);
                    } else {
                        dbProperty = dbProperties.get(i);
                        property = daoDescriptorBean.getDbPropertyMapping().get(dbProperty);
                        daoDescriptorProperty = daoDescriptorBean.getProperties().get(property);
                        value = rs.getObject(dbProperty);
                        if (value != null && daoDescriptorProperty.getTypeClass().getSuperclass().equals(Enum.class)) {
                            value = Enum.valueOf(daoDescriptorProperty.getTypeClass(), value.toString());
                        } else if (value != null && value.getClass().equals(Double.class) && daoDescriptorProperty.getTypeClass().equals(java.math.BigDecimal.class)) {
                            value = new java.math.BigDecimal((Double) value);
                        }
                    }

                    if (value != null) {
                        if (resultIsMap) {
                            ((Map) result).put(property, value);
                        } else {
                            PropertyUtils.setProperty(result, property, value);
                        }
                    }
                }
                return result;
            }
            catch (Exception e) {
                throw new SQLException("error on SELECT", e);
            }
        }
    }

    private static class SelectPreparedStatementSetter implements PreparedStatementFiller {
        private final DaoDescriptorBean daoDescriptorBean;
        private final List<DaoPlaceholderProperty> placeholders;
        private SelectPreparedStatementSetter(DaoDescriptorBean daoDescriptorBean, List<DaoPlaceholderProperty> placeholders) {
            this.daoDescriptorBean = daoDescriptorBean;
            this.placeholders = placeholders;
        }
        @Override
        public void setValues(PreparedStatement ps) throws SQLException {
            try {
                if (placeholders != null && !placeholders.isEmpty()) {
                    fillPreparedStatement(ps, daoDescriptorBean, null, null, placeholders);
                }
            }
            catch (Exception e) {
                throw new SQLException("error on selecting the entity: "+daoDescriptorBean.getName(), e);
            }
        }
    }

    private static class DeletePreparedStatementSetter implements PreparedStatementFiller {
        private final String sql;
        private final List<DaoPlaceholderProperty> placeholders;
        private DeletePreparedStatementSetter(String sql, List<DaoPlaceholderProperty> placeholders) {
            this.sql = sql;
            this.placeholders = placeholders;
        }
        @Override
        public void setValues(PreparedStatement ps) throws SQLException {
            try {
                if (placeholders != null && !placeholders.isEmpty()) {
                    String property;
                    for (int i = 0; i < placeholders.size(); i++) {
                        DaoPlaceholderProperty daoPlaceholderProperty = placeholders.get(i);
                        property = daoPlaceholderProperty.getProperty();
                        DaoDescriptorProperty daoDescriptorProperty = new DaoDescriptorProperty();
                        daoDescriptorProperty.setType(daoPlaceholderProperty.getValue().getClass().getName());
                        int sqlType = DaoDescriptorProperty.getSqlType(daoDescriptorProperty.getTypeClass());
                        ps.setObject(i+1, daoPlaceholderProperty.getValue(), sqlType);
                    }
                }
            }
            catch (Exception e) {
                throw new SQLException("error on deleting the entity: "+sql, e);
            }
        }
    }

    private static class InsertPreparedStatementSetter implements PreparedStatementFiller {
        private final DaoDescriptorBean daoDescriptorBean;
        private final Object entity;
        private InsertPreparedStatementSetter(DaoDescriptorBean daoDescriptorBean, Object entity) {
            this.daoDescriptorBean = daoDescriptorBean;
            this.entity = entity;
        }
        @Override
        public void setValues(PreparedStatement ps) throws SQLException {
            try {
                fillPreparedStatement(ps, daoDescriptorBean, daoDescriptorBean.getAllDbProperties(), entity, null);
            }
            catch (Exception e) {
                throw new SQLException("error on inserting the entity: "+daoDescriptorBean.getName(), e);
            }
        }
    }

    private final DaoDescriptorHelper daoDescriptorHelper = new DaoDescriptorHelper();
    private Map<String, DaoDescriptorBean> descrMap;
    private Map<String, Object> mutexMap;
    private Map<String, Integer> idMap;

    public void init(String scanPackage) throws Exception {
        descrMap = daoDescriptorHelper.createBeanMap(scanPackage);

        mutexMap = Collections.synchronizedMap(new HashMap<>());
        idMap = Collections.synchronizedMap(new HashMap<>());

        for (String beanName : descrMap.keySet()) {
            DaoDescriptorBean daoDescriptorBean = descrMap.get(beanName);
            if (!daoDescriptorBean.getTransientBean()) {
                mutexMap.put(beanName, new Object());
                idMap.put(beanName, getMaxId(daoDescriptorBean));
            }
        }
    }

    private static void fillPreparedStatement(PreparedStatement ps, DaoDescriptorBean daoDescriptorBean, List dbProperties, Object entity, List<DaoPlaceholderProperty> placeholders) throws Exception {
        Object value;
        int sqlType;
        if (placeholders != null && !placeholders.isEmpty()) {
            for (int i = 0; i < placeholders.size(); i++) {
                DaoPlaceholderProperty daoPlaceholderProperty = placeholders.get(i);
                sqlType = DaoDescriptorProperty.getSqlType(daoPlaceholderProperty.getValue().getClass());
                if (sqlType == Types.TIMESTAMP && daoPlaceholderProperty.getValue().getClass().equals(LocalDateTime.class)) {
                    value = Timestamp.valueOf((LocalDateTime)daoPlaceholderProperty.getValue());
                }
                else if (sqlType == Types.DATE && daoPlaceholderProperty.getValue().getClass().equals(LocalDate.class)) {
                    value = Date.valueOf((LocalDate)daoPlaceholderProperty.getValue());
                }
                else if (daoPlaceholderProperty.getValue().getClass().getSuperclass().equals(Enum.class)) {
                    value = daoPlaceholderProperty.getValue().toString();
                }
                else {
                    value = daoPlaceholderProperty.getValue();
                }
                ps.setObject(i+1, value, sqlType);
            }
        }
        else {
            String property;
            DaoDescriptorProperty daoDescriptorProperty;
            for (int i = 0; i < dbProperties.size(); i++) {
                property = daoDescriptorBean.getDbPropertyMapping().get(dbProperties.get(i));
                value = PropertyUtils.getProperty(entity, property);
                daoDescriptorProperty = daoDescriptorBean.getProperties().get(property);
                if (daoDescriptorProperty.isNotNull() && value == null) {
                    throw new IllegalStateException("error on inserting the entity: " + daoDescriptorBean.getName() + " property " + property + " must not null");
                }
                sqlType = DaoDescriptorProperty.getSqlType(daoDescriptorProperty.getTypeClass());
                if (value == null) {
                    ps.setNull(i + 1, sqlType);
                } else {
                    if (sqlType == Types.TIMESTAMP && daoDescriptorProperty.getTypeClass().equals(LocalDateTime.class)) {
                        value = Timestamp.valueOf((LocalDateTime) value);
                    } else if (sqlType == Types.DATE && daoDescriptorProperty.getTypeClass().equals(LocalDate.class)) {
                        value = Date.valueOf((LocalDate) value);
                    } else if (daoDescriptorProperty.getTypeClass().getSuperclass().equals(Enum.class)) {
                        value = value.toString();
                    }
                    ps.setObject(i + 1, value, sqlType);
                }
            }
        }
    }

    public Object selectOne(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        List result = select(entityName, daoProjectionBean, placeholders);
        if (result.isEmpty()) {
            return null;
        }
        return (result instanceof LinkedList)?((LinkedList)result).getFirst():result.get(0);
    }

    public Object selectOne(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        List result = select(sql, entityName, daoProjectionBean, placeholders);
        if (result.isEmpty()) {
            return null;
        }
        return (result instanceof LinkedList)?((LinkedList)result).getFirst():result.get(0);
    }

    private List select(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        String select = daoDescriptorHelper.createSelectSimpleStatement(daoProjectionBean, daoDescriptorBean, placeholders);
        return query(select, new SelectPreparedStatementSetter(daoDescriptorBean, placeholders), new SelectRowMapper(daoProjectionBean, daoDescriptorBean));
    }

    private List select(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        return query(sql, new SelectPreparedStatementSetter(daoDescriptorBean, placeholders), new SelectRowMapper(daoProjectionBean, daoDescriptorBean));
    }

    public List selectMany(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return select(entityName, daoProjectionBean, placeholders);
    }

    public List selectMany(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return select(sql, entityName, daoProjectionBean, placeholders);
    }

    public Object insert(Object entity) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entity.getClass().getName());
        String insert = daoDescriptorHelper.createInsertStatement(daoDescriptorBean);

        synchronized (mutexMap.get(daoDescriptorBean.getName())) {
            Integer id = idMap.get(daoDescriptorBean.getName());
            String idProperty = daoDescriptorBean.getDbPropertyMapping().get(daoDescriptorBean.getPrimaryKey());
            PropertyUtils.setProperty(entity, idProperty, id + 1);
            update(insert, new InsertPreparedStatementSetter(daoDescriptorBean, entity));
            idMap.put(daoDescriptorBean.getName(), id + 1);
        }

        return entity;
    }

    private int getMaxId(DaoDescriptorBean daoDescriptorBean) throws Exception {
        String select = daoDescriptorHelper.createSelectMaxIdStatement(daoDescriptorBean);
        Integer max = queryForObject(select, rs -> rs.next()?rs.getInt(1):null);
        return max == null?0:max;
    }

    public int getNextId(String entityName) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        synchronized (mutexMap.get(daoDescriptorBean.getName())) {
            return idMap.get(daoDescriptorBean.getName()) + 1;
        }
    }

    public void delete(Object entity) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entity.getClass().getName());
        String delete = daoDescriptorHelper.createDeleteStatement(daoDescriptorBean);
        Integer id = (Integer) PropertyUtils.getProperty(entity, daoDescriptorBean.getDbPropertyMapping().get(daoDescriptorBean.getPrimaryKey()));
        delete(id, daoDescriptorBean);
    }

    public void delete(int id, DaoDescriptorBean daoDescriptorBean) throws Exception {
        String delete = daoDescriptorHelper.createDeleteStatement(daoDescriptorBean);
        update(delete, ps -> ps.setInt(1, id));
    }

    public void delete(int id, String entityName) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        delete(id, daoDescriptorBean);
    }

    public void delete(String sql, List<DaoPlaceholderProperty> placeholders) throws Exception {
        update(sql, new DeletePreparedStatementSetter(sql, placeholders));
    }

    public Object update(Object entity) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entity.getClass().getName());
        String update = daoDescriptorHelper.createUpdateStatement(daoDescriptorBean);
        update(update, new PreparedStatementFiller() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                try {
                    List<String> dbProperties = new ArrayList(daoDescriptorBean.getAllDbProperties());
                    dbProperties.remove(daoDescriptorBean.getPrimaryKey());
                    fillPreparedStatement(ps, daoDescriptorBean, dbProperties, entity, null);
                    String idProperty = daoDescriptorBean.getDbPropertyMapping().get(daoDescriptorBean.getPrimaryKey());
                    int id = (Integer) PropertyUtils.getProperty(entity, idProperty);
                    ps.setObject(dbProperties.size() + 1, id, Types.INTEGER);
                }
                catch (Exception e) {
                    throw new SQLException("error on updating the entity: "+daoDescriptorBean.getName(), e);
                }
            }
        });
        return entity;
    }

    public int update(String sql, String entityName, List<DaoPlaceholderProperty> placeholders) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        return update(sql, new PreparedStatementFiller() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                try {
                    fillPreparedStatement(ps, daoDescriptorBean, null, null, placeholders);
                }
                catch (Exception e) {
                    throw new SQLException("error on updating the entity: "+daoDescriptorBean.getName(), e);
                }
            }
        });
    }

    public <T> List<T> query(String sql, PreparedStatementFiller filler, RowMapperFn<T> mapper) throws Exception {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                if (filler != null) filler.setValues(ps);
                try (ResultSet rs = ps.executeQuery()) {
                    List<T> result = new LinkedList<>();
                    int i = 0;
                    while (rs.next()) {
                        result.add(mapper.mapRow(rs, i++));
                    }
                    return result;
                }
            }
        }
    }

    public int update(String sql, PreparedStatementFiller filler) throws Exception {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                if (filler != null) filler.setValues(ps);
                return ps.executeUpdate();
            }
        }
    }

    public <T> T queryForObject(String sql, ResultSetExtractor<T> extractor) throws Exception {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    return extractor.extract(rs);
                }
            }
        }
    }

    public interface ResultSetExtractor<T> {
        T extract(ResultSet rs) throws Exception;
    }
}
