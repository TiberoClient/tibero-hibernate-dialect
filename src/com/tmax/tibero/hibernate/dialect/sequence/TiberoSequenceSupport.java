package com.tmax.tibero.hibernate.dialect.sequence;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.sequence.NextvalSequenceSupport;
import org.hibernate.dialect.sequence.SequenceSupport;

/**
 * 시퀀스의 생성·삭제·채번 SQL 을 만든다.
 *
 * <p>{@link NextvalSequenceSupport} 를 상속해 채번을 {@code <시퀀스>.nextval} 로 내고,
 * 스칼라 조회에 {@code from dual} 이 필요하므로 {@link #getFromDual()} 로 붙여 준다.
 *
 * <p>기존 시퀀스를 읽어 오는 쪽은
 * {@link com.tmax.tibero.hibernate.tool.schema.extract.internal.SequenceInformationExtractorTiberoDatabaseImpl}
 * 이 맡는다 — {@code all_sequences} 에 {@code start_value} 컬럼이 없다.
 */
public class TiberoSequenceSupport extends NextvalSequenceSupport {

    public static SequenceSupport getInstance(final Dialect dialect) {
        return new TiberoSequenceSupport();
    }

    @Override
    public String getFromDual() {
        return " from dual";
    }

    @Override
    public String getDropSequenceString(String sequenceName) throws MappingException {
        return "drop sequence " + sequenceName;
    }

    @Override
    public String getCreateSequenceString(String sequenceName, int initialValue, int incrementSize) {
        if ((initialValue < 0) && (incrementSize > 0)) {
            return
                String.format("%s minvalue %d start with %d increment by %d", new Object[]{
                    getCreateSequenceString(sequenceName),
                        initialValue,
                        initialValue,
                        incrementSize}
                );
        }
        if ((initialValue > 0) && (incrementSize < 0)) {
            return
                String.format("%s maxvalue %d start with %d increment by %d", new Object[]{
                    getCreateSequenceString(sequenceName),
                        initialValue,
                        initialValue,
                        incrementSize}
                );
        }
        return
            String.format("%s start with %d increment by %d", new Object[]{
                getCreateSequenceString(sequenceName),
                    initialValue,
                    incrementSize}
            );
    }

    @Override
    public boolean sometimesNeedsStartingValue() {
        return true;
    }
}
