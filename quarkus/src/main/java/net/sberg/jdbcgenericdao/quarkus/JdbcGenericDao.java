package net.sberg.jdbcgenericdao.quarkus;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import net.sberg.jdbcgenericdao.core.*;
import org.apache.commons.beanutils.PropertyUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
@Default
@Slf4j
public class JdbcGenericDao {

    @ConfigProperty(name = "jdbcGenericDao.scanPackage")
    String scanPackage;

    @Inject
    DataSource dataSource;

    private interface PreparedStatementFiller {
        void setValues(PreparedStatement ps) throws SQLException;
    }

    private interface RowMapperFn<T> {
        T mapRow(ResultSet rs, int rownum) throws SQLException;
    }

    private class UpdatePreparedStatementSetter implements PreparedStatementFiller {
        private final DaoDescriptorBean daoDescriptorBean;
        private final Object entity;
        private final List<DaoPlaceholderProperty> placeholders;
        private UpdatePreparedStatementSetter(DaoDescriptorBean daoDescriptorBean, Object entity, List<DaoPlaceholderProperty> placeholders) {
            this.daoDescriptorBean = daoDescriptorBean;
            this.entity = entity;
            this.placeholders = placeholders;
        }
        @Override
        public void setValues(PreparedStatement ps) throws SQLException {
            try {
                if (placeholders != null && !placeholders.isEmpty()) {
                    fillPreparedStatement(ps, daoDescriptorBean, null, null, placeholders);
                }
                else {
                    List<String> dbProperties = new ArrayList(daoDescriptorBean.getAllDbProperties());
                    dbProperties.remove(daoDescriptorBean.getPrimaryKey());
                    fillPreparedStatement(ps, daoDescriptorBean, dbProperties, entity, null);

                    String idProperty = daoDescriptorBean.getDbPropertyMapping().get(daoDescriptorBean.getPrimaryKey());
                    int id = (Integer) PropertyUtils.getProperty(entity, idProperty);
                    ps.setObject(dbProperties.size() + 1, id, Types.INTEGER);
                }
            }
            catch (Exception e) {
                log.error("error on updating the entity: "+daoDescriptorBean.getName(), e);
                throw new SQLException("error on updating the entity: "+daoDescriptorBean.getName(), e);
            }
        }
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
                    result = Class.forName(daoDescriptorBean.getName()).newInstance();
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
                            value = ((Timestamp)value).toLocalDateTime();
                        }
                        else if (value != null && value.getClass().equals(Date.class)) {
                            value = ((Date)value).toLocalDate();
                        }
                        property = dbProperties.get(i);
                    }
                    else {
                        dbProperty = dbProperties.get(i);
                        property = daoDescriptorBean.getDbPropertyMapping().get(dbProperty);
                        daoDescriptorProperty = daoDescriptorBean.getProperties().get(property);
                        value = rs.getObject(dbProperty);
                        if (value != null && daoDescriptorProperty.getTypeClass().getSuperclass().equals(Enum.class)) {
                            value = Enum.valueOf(daoDescriptorProperty.getTypeClass(), value.toString());
                        } else if (value != null && value.getClass().equals(Double.class) && daoDescriptorProperty.getTypeClass().equals(BigDecimal.class)) {
                            value = new BigDecimal((Double) value);
                        }
                    }

                    if (value != null) {
                        if (resultIsMap) {
                            ((Map)result).put(property, value);
                        }
                        else {
                            PropertyUtils.setProperty(result, property, value);
                        }
                    }
                }
                return result;
            }
            catch (Exception e) {
                assert daoDescriptorBean != null;
                log.error("error on selecting the entity: {}", daoDescriptorBean.getName(), e);
                throw new SQLException("error on selecting the entity: "+daoDescriptorBean.getName(), e);
            }
        }
    }

    private class SelectPreparedStatementSetter implements PreparedStatementFiller {
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
                log.error("error on selecting the entity: "+daoDescriptorBean.getName(), e);
                throw new SQLException("error on selecting the entity: "+daoDescriptorBean.getName(), e);
            }
        }
    }

    private class DeletePreparedStatementSetter implements PreparedStatementFiller {
        private final List<DaoPlaceholderProperty> placeholders;
        private final String sql;
        private DeletePreparedStatementSetter(String sql, List<DaoPlaceholderProperty> placeholders) {
            this.sql = sql;
            this.placeholders = placeholders;
        }
        @Override
        public void setValues(PreparedStatement ps) throws SQLException {
            try {
                if (placeholders != null && !placeholders.isEmpty()) {
                    fillPreparedStatement(ps, null, null, null, placeholders);
                }
            }
            catch (Exception e) {
                log.error("error on deleting the entities 4 the sql: {}", sql, e);
                throw new SQLException("error on deleting the entities 4 the sql: "+sql, e);
            }
        }
    }

    private class InsertPreparedStatementSetter implements PreparedStatementFiller {
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
                log.error("error on inserting the entity: {}", daoDescriptorBean.getName(), e);
                throw new SQLException("error on inserting the entity: "+daoDescriptorBean.getName(), e);
            }
        }
    }

    private DaoDescriptorHelper daoDescriptorHelper;
    private Map<String, DaoDescriptorBean> descrMap;
    private Map<String, Object> mutexMap;
    private Map<String, Integer> idMap;

    @PostConstruct
    public void init() throws Exception {
        daoDescriptorHelper = new DaoDescriptorHelper();
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

    private void fillPreparedStatement(PreparedStatement ps, DaoDescriptorBean daoDescriptorBean, List dbProperties, Object entity, List<DaoPlaceholderProperty> placeholders) throws Exception {
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
        return result.getFirst();
    }

    public Object selectOne(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        List result = select(sql, entityName, daoProjectionBean, placeholders);
        if (result.isEmpty()) {
            return null;
        }
        return result.getFirst();
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

    @Transactional
    public Object insert(Object entity) throws Exception {
        synchronized (mutexMap.get(entity.getClass().getName())) {
            DaoDescriptorBean daoDescriptorBean = descrMap.get(entity.getClass().getName());
            String insert = daoDescriptorHelper.createInsertStatement(daoDescriptorBean);

            String idProperty = daoDescriptorBean.getDbPropertyMapping().get(daoDescriptorBean.getPrimaryKey());
            Integer id = (Integer) PropertyUtils.getProperty(entity, idProperty);
            if (id <= 0) {
                id = getMaxId(daoDescriptorBean);
                id++;
                PropertyUtils.setProperty(entity, idProperty, id);
            }

            update(insert, new InsertPreparedStatementSetter(daoDescriptorBean, entity));
            return entity;
        }
    }

    private int getMaxId(DaoDescriptorBean daoDescriptorBean) throws Exception {
        String selectMaxId = daoDescriptorHelper.createSelectMaxIdStatement(daoDescriptorBean);
        Integer id = queryForObject(selectMaxId, rs -> {
            int v = rs.getInt(1);
            return rs.wasNull() ? null : v;
        });
        if (id == null) {
            id = 0;
        }
        return id;
    }

    public int getNextId(String entityName) throws Exception {
        synchronized (mutexMap.get(entityName)) {
            Integer id = idMap.get(entityName);
            id++;
            idMap.put(entityName, id);
            return id;
        }
    }

    @Transactional
    public void delete(Object entity) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entity.getClass().getName());
        String idProperty = daoDescriptorBean.getDbPropertyMapping().get(daoDescriptorBean.getPrimaryKey());
        int id = (Integer)PropertyUtils.getProperty(entity, idProperty);
        delete(id, daoDescriptorBean);
    }

    private void delete(int id, DaoDescriptorBean daoDescriptorBean) throws Exception {
        String delete = daoDescriptorHelper.createDeleteStatement(daoDescriptorBean);
        update(delete, ps -> ps.setObject(1, id, Types.INTEGER));
    }

    @Transactional
    public void delete(int id, String entityName) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        delete(id, daoDescriptorBean);
    }

    @Transactional
    public void delete(String sql, List<DaoPlaceholderProperty> placeholders) throws Exception {
        update(sql, new DeletePreparedStatementSetter(sql, placeholders));
    }

    @Transactional
    public Object update(Object entity) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entity.getClass().getName());
        String insert = daoDescriptorHelper.createUpdateStatement(daoDescriptorBean);
        update(insert, new UpdatePreparedStatementSetter(daoDescriptorBean, entity, null));
        return entity;
    }

    @Transactional
    public int update(String sql, String entityName, List<DaoPlaceholderProperty> placeholders) throws Exception {
        DaoDescriptorBean daoDescriptorBean = descrMap.get(entityName);
        return update(sql, new UpdatePreparedStatementSetter(daoDescriptorBean, null, placeholders));
    }

    private <T> List<T> query(String sql, PreparedStatementFiller filler, RowMapperFn<T> mapper) throws Exception {
        List<T> results = new ArrayList<>();
        try (Connection con = dataSource.getConnection(); PreparedStatement ps = con.prepareStatement(sql)) {
            if (filler != null) {
                filler.setValues(ps);
            }
            try (ResultSet rs = ps.executeQuery()) {
                int rownum = 0;
                while (rs.next()) {
                    results.add(mapper.mapRow(rs, rownum++));
                }
            }
        }
        return results;
    }

    private int update(String sql, PreparedStatementFiller filler) throws Exception {
        try (Connection con = dataSource.getConnection(); PreparedStatement ps = con.prepareStatement(sql)) {
            if (filler != null) {
                filler.setValues(ps);
            }
            return ps.executeUpdate();
        }
    }

    private <T> T queryForObject(String sql, ResultSetExtractor<T> extractor) throws Exception {
        try (Connection con = dataSource.getConnection(); PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return extractor.extract(rs);
                }
                return null;
            }
        }
    }

    @FunctionalInterface
    private interface ResultSetExtractor<T> {
        T extract(ResultSet rs) throws SQLException;
    }
}
