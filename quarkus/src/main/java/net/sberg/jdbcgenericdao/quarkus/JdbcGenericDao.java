package net.sberg.jdbcgenericdao.quarkus;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import net.sberg.jdbcgenericdao.core.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

@ApplicationScoped
public class JdbcGenericDao extends AbstractJdbcGenericDao {

    @ConfigProperty(name = "jdbcGenericDao.scanPackage")
    String scanPackage;

    @Inject
    DataSource dataSource;

    @PostConstruct
    public void initialize() throws Exception {
        init(scanPackage);
    }

    @Transactional
    protected <T> List<T> query(String sql, PreparedStatementFiller filler, RowMapperFn<T> mapper) throws Exception {
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

    @Transactional
    protected int manipulate(String sql, PreparedStatementFiller filler) throws Exception {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                if (filler != null) filler.setValues(ps);
                return ps.executeUpdate();
            }
        }
    }

    @Transactional
    protected int[] batchManipulate(String sql, PreparedBatchStatementFiller filler) throws Exception {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                for (int i = 0; i < filler.getBatchSize(); i++) {
                    filler.setValues(ps, i);
                }
                return ps.executeBatch();
            }
        }
    }

    @Transactional
    protected <T> T queryForObject(String sql, ResultSetExtractor<T> extractor) throws Exception {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    return extractor.extract(rs);
                }
            }
        }
    }
}
