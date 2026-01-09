import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TiberoDialectCascadeConstraintsTest
 *
 * 목적:
 *  - TiberoDialect.getCascadeConstraintsString() = " cascade constraints" contract 검증
 *  - FK 제약조건이 있는 상태에서 부모 테이블 drop 시:
 *      - cascade constraints 없으면 실패
 *      - cascade constraints 붙이면 FK 포함하여 drop 성공
 *  - 따라서 Dialect.dropConstraints() == false 여도 drop이 정상 동작함을 DB 레벨에서 검증
 */
public class CascadeConstraintsTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] {}; // native ddl/dml 기반
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
    }

    @Test
    public void testDropTableWithForeignKey_UsesCascadeConstraintsSuccessfully() {
        // --------------------------------------------------------------------
        // 1) contract 검증
        // --------------------------------------------------------------------
        assertEquals(" cascade constraints", dialect.getCascadeConstraintsString());
        assertFalse("dropConstraints should be false because cascade constraints will handle constraints",
                dialect.dropConstraints());

        final String parent = uniqueObjectName("CASCADE_PARENT");
        final String child  = uniqueObjectName("CASCADE_CHILD");
        final String fkName = uniqueObjectName("FK_CP"); // FK name도 짧게

        try {
            // ----------------------------------------------------------------
            // 2) parent/child table 생성 + FK 생성
            // ----------------------------------------------------------------
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + parent + " (" +
                                "id number primary key" +
                                ")"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "create table " + child + " (" +
                                "id number primary key, " +
                                "parent_id number not null" +
                                ")"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "alter table " + child +
                                " add constraint " + fkName +
                                " foreign key (parent_id) references " + parent + "(id)"
                ).executeUpdate();
            });

            inTransaction(session -> {
                        int cnt = session.createNativeQuery("select count(*) from user_constraints where constraint_name = '" + fkName + "'", Integer.class).getSingleResult();
                        assertEquals(1, cnt);
                    }
            );

            // ----------------------------------------------------------------
            // 3) cascade constraints 없이 drop 하면 실패해야 정상
            // ----------------------------------------------------------------
            try {
                inTransaction(session ->
                        session.createNativeMutationQuery("drop table " + parent).executeUpdate()
                );
                fail("Expected drop table without cascade constraints to fail due to FK reference.");
            } catch (Exception e) {
                System.out.println("[Expected failure] drop parent without cascade failed: " + e.getMessage());
            }

            // ----------------------------------------------------------------
            // 4) cascade constraints 붙이면 성공해야 정상 (FK 포함 삭제)
            // ----------------------------------------------------------------
            inTransaction(session ->
                    session.createNativeMutationQuery(
                            "drop table " + parent + dialect.getCascadeConstraintsString()
                    ).executeUpdate()
            );

            // ----------------------------------------------------------------
            // 5) parent drop 이후에도 child table은 남아있어야 함
            //    (단 FK는 자동 삭제되어야 함)
            //    -> child drop이 문제없이 수행되면 FK 제거된 것
            // ----------------------------------------------------------------
            inTransaction(session -> {
                    int cnt = session.createNativeQuery("select count(*) from user_constraints where constraint_name = '" + fkName + "'", Integer.class).getSingleResult();
                    assertEquals("FK should be removed after parent table drop", 0, cnt);
                }
            );
            inTransaction(session ->
                    session.createNativeMutationQuery("drop table " + child).executeUpdate()
            );

        } finally {
            // cleanup
            try { dropTableWithRetry(child); } catch (Exception ignored) {}
            try { dropTableWithRetry(parent); } catch (Exception ignored) {}
        }
    }
}
