package com.tmax.tibero.hibernate.dialect.sequence;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.sequence.NextvalSequenceSupport;
import org.hibernate.dialect.sequence.SequenceSupport;

//시퀀스 생성/삭제/조회 SQL 생성 (DDL)
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
            String.format("%s start with %d increment by  %d", new Object[]{
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
