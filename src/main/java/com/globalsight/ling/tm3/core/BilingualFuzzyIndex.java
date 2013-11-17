package com.globalsight.ling.tm3.core;

import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import com.globalsight.ling.tm3.core.persistence.BatchStatementBuilder;
import com.globalsight.ling.tm3.core.persistence.SQLUtil;
import com.globalsight.ling.tm3.core.persistence.StatementBuilder;

/**
 * Fuzzy index implementation for Bilingual TMs.
 * 
 * <p>
 */
class BilingualFuzzyIndex<T extends TM3Data>  
            extends FuzzyIndex<T> {
    
    BilingualFuzzyIndex(StorageInfo<T> storage) {
        super(storage);
    }
    
    @Override
    protected void indexFingerprints(List<Long> fingerprints,
            TM3Tuv<T> tuv) throws SQLException {
        BatchStatementBuilder sb = new BatchStatementBuilder("INSERT INTO ")
            .append(getStorage().getFuzzyIndexTableName())
            .append(" (fingerprint, tuvId, tuId, tuvCount, isSource) ")
            .append("VALUES (?, ?, ?, ?, ?)");
        int tuvCount = fingerprints.size();
        for (Long fp : fingerprints) {
            sb.addBatch(fp, tuv.getId(), tuv.getTu().getId(), 
                        tuvCount, tuv.isSource());
        }
        SQLUtil.execBatch(getSession(), sb);
    }

    @Override
    public void deleteFingerprints(TM3Tuv<T> tuv) throws SQLException {
        StatementBuilder sb = new StatementBuilder();
        sb.append("DELETE FROM ")
          .append(getStorage().getFuzzyIndexTableName())
          .append(" WHERE tuvId = ?").addValue(tuv.getId());
        SQLUtil.exec(getSession(), sb);
    }

    @Override
    protected StatementBuilder getFuzzyLookupQuery(List<Long> fingerprints,
                    TM3Locale keyLocale, Set<? extends TM3Locale> matchLocales,
                    Map<TM3Attribute, Object> inlineAttrs, boolean lookupTarget) {
        StatementBuilder sb = new StatementBuilder();
        sb.append("SELECT tuvId, tuId, SUM(1) as score FROM ")
          .append(getStorage().getFuzzyIndexTableName()).append(" AS idx");
        if (! inlineAttrs.isEmpty()) {
            sb.append(", ")
              .append(getStorage().getTuTableName()).append(" AS tu");
        }
        sb.append(" WHERE ");
        sb.append("(idx.fingerprint = ?").addValue(fingerprints.get(0));
        for (int i = 1; i < fingerprints.size(); i++ ) { 
            sb.append(" OR idx.fingerprint = ?").addValue(fingerprints.get(i));
        }
        sb.append(")");
        // This is a bit redundant as BilingualTm.findMatches checks that
        // we're looking for a possible locale, but putting the full logic
        // here makes it clear.
        TM3BilingualTm<T> tm = (BilingualTm<T>) getStorage().getTm();
        if (tm.getSrcLocale().equals(keyLocale)) {
            sb.append(" AND isSource = 1");
        } else if (tm.getTgtLocale().equals(keyLocale) && lookupTarget) {
            sb.append(" AND isSource = 0");
        } else {
            throw new RuntimeException("should not happen");
        }
        if (! inlineAttrs.isEmpty()) {
            sb.append(" AND idx.tuId = tu.id");
            for (Map.Entry<TM3Attribute, Object> e : inlineAttrs.entrySet()) {
                sb.append(" AND tu." + e.getKey().getColumnName() + " = ?");
                sb.addValue(e.getValue());
            }
        }
        // Add minimum and max bounds on the data length
        int min = fingerprints.size() / 3;
        int max = fingerprints.size() * 3;
        if (min > 0) {
            sb.append(" AND idx.tuvCount > ?").addValue(min);
        }
        sb.append(" AND idx.tuvCount < ?").addValue(max);
        sb.append(" GROUP BY tuvId ORDER BY score DESC");
        if (matchLocales != null) {
            // an exists subselect seems simpler, but mysql bug 46947 causes
            // exists subselects to take locks even in repeatable read
            List<Long> targetLocaleIds = new ArrayList<Long>();
            for (TM3Locale locale : matchLocales) {
                targetLocaleIds.add(locale.getId());
            }
            sb = new StatementBuilder()
              .append("SELECT DISTINCT result.* FROM (")
              .append(sb)
              .append(") AS result, ")
              .append(getStorage().getTuvTableName() + " AS targetTuv ")
              .append("WHERE ")
              .append("targetTuv.tuId = result.tuId AND ")
              .append("targetTuv.localeId IN")
              .append(SQLUtil.longGroup(targetLocaleIds));
        }
        return sb;
    }
 
}
