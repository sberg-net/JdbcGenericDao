package net.sberg.jdbcgenericdao.springboot;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import net.sberg.jdbcgenericdao.core.AbstractJdbcGenericDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class JdbcGenericDao extends AbstractJdbcGenericDao {

    @Value("${jdbcGenericDao.scanPackage}")
    private String scanPackage;

    private final JdbcTemplate jdbcTemplate;

    public JdbcGenericDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void initialize() throws Exception {
        init(scanPackage);
    }

    @Transactional
    protected <T> List<T> query(String sql, PreparedStatementFiller filler, RowMapperFn<T> mapper) throws Exception {
        return jdbcTemplate.query(
            sql,
            new PreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps) throws SQLException {
                    filler.setValues(ps);
                }
            },
            new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return mapper.mapRow(rs, rowNum);
                }
            }
        );
    }

    @Transactional
    protected int manipulate(String sql, PreparedStatementFiller filler) throws Exception {
        return jdbcTemplate.update(
            sql,
            new PreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps) throws SQLException {
                    filler.setValues(ps);
                }
            }
        );
    }

    @Transactional
    protected int[] batchManipulate(String sql, PreparedBatchStatementFiller filler) throws Exception {
        return jdbcTemplate.batchUpdate(
            sql,
            new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    filler.setValues(ps, i);
                }

                @Override
                public int getBatchSize() {
                    return filler.getBatchSize();
                }
            }
        );
    }

    @Transactional
    protected <T> T queryForObject(String sql, ResultSetExtractor<T> extractor) throws Exception {
        return jdbcTemplate.query(
            sql,
            new org.springframework.jdbc.core.ResultSetExtractor<T>() {
                @Override
                public T extractData(ResultSet rs) throws SQLException, DataAccessException {
                    try {
                        return extractor.extract(rs);
                    } catch (Exception e) {
                        throw new SQLException(e);
                    }
                }
            }
        );
    }
}
