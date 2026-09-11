package com.tmax.tibero.hibernate.procedure;

import org.hibernate.procedure.internal.StandardCallableStatementSupport;
import org.hibernate.procedure.spi.ProcedureParameterImplementor;
import org.hibernate.sql.exec.spi.JdbcCallParameterRegistration;

/**
 * 저장 프로시저의 <b>이름 파라미터</b>를 Tibero(Oracle 호환) 표기로 넘긴다.
 *
 * <p>{@link StandardCallableStatementSupport}는 이름 파라미터도 {@code ?} 하나로만 내보내
 * 결국 <b>등록 순서에 따른 위치 바인딩</b>이 된다. 프로시저 선언 순서와 등록 순서가
 * 다르면 예외 없이 값만 뒤바뀐다.
 *
 * <p>실측 — 선언이 {@code (p_a, p_b, p_out)} 인 프로시저에 {@code p_b}를 먼저 등록하면
 * <ul>
 *   <li>표준: {@code {call P(?,?,?)}} → 703 (위치 바인딩)</li>
 *   <li>이름: {@code {call P(p_b => ?, p_a => ?, p_out => ?)}} → 307 (정상)</li>
 * </ul>
 * Tibero가 {@code =>} 표기를 지원함을 확인하고 도입했다.
 */
public class TiberoCallableStatementSupport extends StandardCallableStatementSupport {

    public static final StandardCallableStatementSupport REF_CURSOR_INSTANCE =
            new TiberoCallableStatementSupport(true);

    public TiberoCallableStatementSupport(boolean supportsRefCursors) {
        super(supportsRefCursors);
    }

    @Override
    protected void appendNameParameter(
            StringBuilder buffer,
            ProcedureParameterImplementor parameter,
            JdbcCallParameterRegistration registration) {
        buffer.append(parameter.getName()).append(" => ?");
    }
}
