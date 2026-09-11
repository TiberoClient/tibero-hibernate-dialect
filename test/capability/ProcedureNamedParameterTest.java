package capability;

import jakarta.persistence.ParameterMode;
import org.hibernate.cfg.Configuration;
import org.hibernate.procedure.ProcedureCall;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import static org.junit.Assert.*;

/**
 * 저장 프로시저 <b>이름 파라미터</b> 바인딩 회귀 테스트.
 *
 * <p>{@code TiberoCallableStatementSupport} 가 없으면 이름 파라미터도 {@code ?} 로만 나가
 * <b>등록 순서에 따른 위치 바인딩</b>이 된다. 예외가 없고 값만 뒤바뀌므로 기존
 * {@code RefCursorTest} 처럼 등록 순서를 선언 순서와 같게 쓰면 우연히 통과한다.
 * 그래서 여기서는 <b>일부러 순서를 뒤집어</b> 검증한다.
 *
 * <p>이름 표기는 Hibernate 옵트인 설정
 * {@code hibernate.query.pass_procedure_paramater_names}(Hibernate 쪽 키 오타 그대로) 가
 * 켜져 있고 드라이버가 {@code supportsNamedParameters()} 를 참으로 답할 때만 쓰인다.
 * 기본값은 false 라 전 dialect 공통으로 위치 바인딩이며, 이 테스트는 <b>옵트인 경로</b>를 고정한다.
 */
public class ProcedureNamedParameterTest extends AbstractTiberoDialectTestBase {

    private static final String PROC = "TB_NP_PROC";

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.query.pass_procedure_paramater_names", "true");
    }

    private void createProcedure() {
        inTransaction(session -> session.createNativeMutationQuery(
                "create or replace procedure " + PROC
                        + "(p_a in number, p_b in number, p_out out number) is "
                        + "begin p_out := p_a*100 + p_b; end;").executeUpdate());
    }

    private void dropProcedure() {
        try {
            inTransaction(session -> session.createNativeMutationQuery("drop procedure " + PROC).executeUpdate());
        } catch (Exception ignored) {
        }
    }

    @Test
    public void namedParameters_bindByName_notByRegistrationOrder() {
        createProcedure();
        try {
            Object out = inTransactionReturning(session -> {
                ProcedureCall call = session.createStoredProcedureCall(PROC);
                // 선언은 (p_a, p_b, p_out) 인데 일부러 p_b 를 먼저 등록한다.
                call.registerParameter("p_b", Integer.class, ParameterMode.IN);
                call.registerParameter("p_a", Integer.class, ParameterMode.IN);
                call.registerParameter("p_out", Integer.class, ParameterMode.OUT);
                call.setParameter("p_b", 7);
                call.setParameter("p_a", 3);
                call.execute();
                return call.getOutputParameterValue("p_out");
            });
            assertEquals(
                    "이름으로 바인딩되면 p_a=3, p_b=7 이라 307. 위치 바인딩이면 703 이 나온다 "
                            + "— TiberoCallableStatementSupport 가 빠졌는지 확인할 것",
                    307,
                    ((Number) out).intValue());
        } finally {
            dropProcedure();
        }
    }

    @Test
    public void tibero_acceptsOracleStyleNamedCallSyntax() {
        // dialect 수정의 전제 — Tibero 가 name => ? 표기를 실제로 받는가
        createProcedure();
        try {
            Object v = inTransactionReturning(session -> session.createNativeQuery(
                    "select 1 from dual", Integer.class).getSingleResult());
            assertEquals(Integer.valueOf(1), v);

            inTransaction(session -> session.doWork(conn -> {
                try (java.sql.CallableStatement cs =
                             conn.prepareCall("{call " + PROC + "(p_b => ?, p_a => ?, p_out => ?)}")) {
                    cs.setInt(1, 7);
                    cs.setInt(2, 3);
                    cs.registerOutParameter(3, java.sql.Types.NUMERIC);
                    cs.execute();
                    assertEquals("Tibero 가 name => ? 를 이름으로 해석해야 함", 307, cs.getInt(3));
                }
            }));
        } finally {
            dropProcedure();
        }
    }
}
