package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.cfg.Configuration;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * JSON 매핑 회귀 테스트.
 *
 * <p>기존 스위트는 JSON DDL 타입명과 JdbcType <b>등록 여부</b>만 보고, JSON 테이블은
 * 수기 native DDL 로 만들었다. 그래서 Hibernate 가 실제로 만드는
 * {@code check (col is json)} 경로를 아무도 타지 않았고, JSON 컬럼 엔티티는
 * {@code create table} 부터 실패하는 상태였다.
 *
 * <p>여기서는 <b>엔티티 스키마 생성을 Hibernate 에 맡겨</b> 그 경로를 강제로 태운다.
 */
public class JsonMappingTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "JmEntity") @Table(name = "JM_T")
    public static class JmEntity {
        @Id public Long id;
        @JdbcTypeCode(SqlTypes.JSON) public Map<String, String> payload;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{JmEntity.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        // 부팅 시점에 Hibernate 가 직접 create table 을 실행하게 한다.
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    @Test
    public void jsonEntity_schemaIsActuallyCreatedByHibernate() {
        // hbm2ddl 은 실패해도 부팅을 막지 않으므로 카탈로그로 실체를 확인한다.
        Long cnt = inTransactionReturning(session -> session.createNativeQuery(
                "select count(*) from user_tables where table_name = 'JM_T'", Long.class).getSingleResult());
        assertEquals("JSON 컬럼 엔티티의 테이블이 실제로 만들어져야 함 "
                + "— check (… is json) 이 붙으면 JDBC-8014 로 실패한다", Long.valueOf(1L), cnt);

        String type = inTransactionReturning(session -> session.createNativeQuery(
                "select data_type from user_tab_columns where table_name='JM_T' and column_name='PAYLOAD'",
                String.class).getSingleResult());
        assertEquals("JSON", type);
    }

    // NOTE: check 제약 부재는 카탈로그로 확인하지 않는다.
    //   Tibero user_constraints.SEARCH_CONDITION 은 LONG 이라 LIKE 조회가 안 되고,
    //   NOT NULL 도 'C' 로 잡혀 개수 비교가 애매하다.
    //   DDL 문자열 단언은 render.RenderContractTest#jsonColumnDdl_hasNoIsJsonPredicate 가,
    //   실제 생성 여부는 위 jsonEntity_schemaIsActuallyCreatedByHibernate 가 담당한다.

    @Test
    public void jsonJdbcType_isTiberoSpecific_notOracleDefault() {
        JdbcTypeRegistry registry = sessionFactory().getTypeConfiguration().getJdbcTypeRegistry();
        JdbcType json = registry.getDescriptor(SqlTypes.JSON);
        assertNotNull(json);
        assertEquals("Oracle 클래스를 그대로 쓰면 is json 체크가 딸려옴 "
                        + "— TiberoJsonBlobJdbcType 으로 교체되어 있어야 함",
                "com.tmax.tibero.hibernate.type.TiberoJsonBlobJdbcType",
                json.getClass().getName());
        assertNull("체크 제약 없음이 계약",
                json.getCheckCondition("payload", null, null, sessionFactory().getJdbcServices().getDialect()));
    }
}
