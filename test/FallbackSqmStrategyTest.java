import jakarta.persistence.*;
import org.hibernate.cfg.Configuration;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FallbackSqmStrategyTest extends BaseCoreFunctionalTestCase {
    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.session_factory.statement_inspector", SqlCaptureInspector.class.getName());
        // 필요하면 켜기
        // cfg.setProperty("hibernate.show_sql", "true");
        // cfg.setProperty("hibernate.format_sql", "true");
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] { ParentEntity.class, ChildEntity.class, SourceEntity.class };
    }

    // =========================================================================
    // Mutation Strategy Tests (Bulk UPDATE/DELETE)
    // =========================================================================

    @Test
    public void testBulkDeleteJoinedInheritanceWorks() {
        inTransaction(session -> {
            session.persist(new ChildEntity("p1", "c1"));
            session.persist(new ChildEntity("p2", "c2"));
        });

        inTransaction(session -> {
            int affected = session.createMutationQuery("delete from ParentEntity").executeUpdate();
            assertTrue("Bulk delete should affect at least one row", affected > 0);
        });

        inTransaction(session -> {
            Long parentCount = session.createQuery("select count(p) from ParentEntity p", Long.class).getSingleResult();
            Long childCount = session.createQuery("select count(c) from ChildEntity c", Long.class).getSingleResult();

            assertEquals("Parent table should be empty", 0L, parentCount.longValue());
            assertEquals("Child table should be empty", 0L, childCount.longValue());
        });

        // Inspector는 assert하지 않고 출력
        SqlCaptureInspector.getSqls().forEach(sql -> System.out.println("[SQL] " + sql));
    }

    @Test
    public void testBulkUpdateJoinedInheritanceWorks() {
        inTransaction(session -> {
            session.persist(new ChildEntity("before", "c1"));
            session.persist(new ChildEntity("before", "c2"));
        });

        inTransaction(session -> {
            int affected = session.createMutationQuery("update ParentEntity p set p.pName = 'after'").executeUpdate();
            assertTrue("Bulk update should affect at least one row", affected > 0);
        });

        inTransaction(session -> {
            List<String> names = session.createQuery("select p.pName from ParentEntity p", String.class).getResultList();
            assertFalse(names.isEmpty());
            assertTrue(names.stream().allMatch(n -> "after".equals(n)));
        });

        // Inspector는 assert하지 않고 출력
        SqlCaptureInspector.getSqls().forEach(sql -> System.out.println("[SQL] " + sql));
    }

    // =========================================================================
    // Insert Strategy Test (Bulk INSERT ... SELECT)
    // =========================================================================

    /**
     * JOINED 상속 구조에서 bulk insert-select가 정상 수행되는지 검증
     *
     * 이 테스트는 Hibernate가 multi-table insert 전략(GlobalTemporaryTableInsertStrategy)을
     * 사용할 수 있는 경로를 유도한다.
     *
     * - SourceEntity에서 데이터를 읽어 ChildEntity로 bulk insert
     * - 성공 시 ParentEntity/ChildEntity 모두 row가 생성되어야 한다.
     */
    @Test
    public void testBulkInsertSelectJoinedInheritanceWorks() {
        SqlCaptureInspector.clear();

        // given: source data
        inTransaction(session -> {
            session.persist(new SourceEntity("p1", "c1"));
            session.persist(new SourceEntity("p2", "c2"));
        });

        // when: bulk insert-select into joined inheritance entity
        // NOTE:
        // - Hibernate 6.x에서 insert-select는 "insert into Entity(...) select ..." 형태로 지원됨
        // - ChildEntity는 상속 구조이므로 multi-table insert가 필요
        inTransaction(session -> {
            int inserted = session.createMutationQuery(
                    "insert into ChildEntity(pName, cName) " +
                            "select s.pName, s.cName from SourceEntity s"
            ).executeUpdate();

            assertTrue("Bulk insert should insert at least one row", inserted > 0);
        });

        // then: rows exist in parent & child
        inTransaction(session -> {
            Long parentCount = session.createQuery("select count(p) from ParentEntity p", Long.class).getSingleResult();
            Long childCount = session.createQuery("select count(c) from ChildEntity c", Long.class).getSingleResult();

            assertEquals("Child rows should match parent rows for joined inheritance",
                    parentCount.longValue(), childCount.longValue());

            assertTrue("Inserted rows should exist", childCount > 0);
        });

        // Inspector는 assert하지 않고 출력
        SqlCaptureInspector.getSqls().forEach(sql -> System.out.println("[SQL] " + sql));
    }

    // =========================================================================
    // Entities
    // =========================================================================

    @Entity(name = "ParentEntity")
    @Table(name = "PARENT_ENTITY")
    @Inheritance(strategy = InheritanceType.JOINED)
    public static class ParentEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        @Column(name = "P_NAME")
        private String pName;

        public ParentEntity() {}

        public ParentEntity(String pName) {
            this.pName = pName;
        }

        public Long getId() {
            return id;
        }

        public String getPName() {
            return pName;
        }

        public void setPName(String pName) {
            this.pName = pName;
        }
    }

    @Entity(name = "ChildEntity")
    @Table(name = "CHILD_ENTITY")
    public static class ChildEntity extends ParentEntity {

        @Column(name = "C_NAME")
        private String cName;

        public ChildEntity() {}

        public ChildEntity(String pName, String cName) {
            super(pName);
            this.cName = cName;
        }

        public String getCName() {
            return cName;
        }

        public void setCName(String cName) {
            this.cName = cName;
        }
    }

    /**
     * bulk insert-select의 source 역할
     */
    @Entity(name = "SourceEntity")
    @Table(name = "SOURCE_ENTITY")
    public static class SourceEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        @Column(name = "P_NAME")
        private String pName;

        @Column(name = "C_NAME")
        private String cName;

        public SourceEntity() {}

        public SourceEntity(String pName, String cName) {
            this.pName = pName;
            this.cName = cName;
        }

        public Long getId() {
            return id;
        }

        public String getPName() {
            return pName;
        }

        public String getCName() {
            return cName;
        }
    }
}
