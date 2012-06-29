package com.globalsight.ling.tm3.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.junit.After;
import org.junit.Test;

import com.globalsight.ling.tm3.core.persistence.HibernateConfig;

/**
 * Base class for storage testing.
 */
public abstract class TM3Tests {

    static long currentTestId = 0;
    static TM3Event currentTestEvent = null;
    static Session currentSession;
    static Transaction currentTransaction;
    static TM3Manager manager;
       
    static TestDataFactory FACTORY = new TestDataFactory();
    static SessionFactory sessionFactory;
    
    // Initialization method.  This should be called as part of a 
    // @BeforeClass method by all implementations!
    public static void init() throws SQLException {
        sessionFactory = setupHibernate();
        cleanAllDbs();
        
        EN_US = new TestLocale(1, "en", "US");
        FR_FR = new TestLocale(2, "fr", "FR");
        DE_DE = new TestLocale(3, "de", "DE");
        
        manager = DefaultManager.create();
    }
    
    // This leaves the tm in the db for later inspection.
    // All subclasses should call this in an @After method!
    public void afterTest() throws Exception {
        // If tests failed, transaction may still be active.  If we don't
        // roll it back then DB cleanup will hang in MySQL 5.5 due to 
        // metadata locking until the transaction expiration timer fires.
        if (currentTransaction != null && currentTransaction.isActive()) {
            System.out.println("Cleaning up transaction");
            currentTransaction.rollback();
        }
        if (currentSession != null && currentSession.isOpen()) {
            currentSession.close();
        }
    }
    
    static TestLocale EN_US, FR_FR, DE_DE;
    
    static Set<TM3Attribute> inlineAttrs() {
        Set<TM3Attribute> r = new HashSet<TM3Attribute>();
        r.add(new TM3Attribute("inlineString", new StringType(), "inlineString", true));
        r.add(new TM3Attribute("inlineBoolean", new BooleanType(), "inlineBoolean", true));
        r.add(new TM3Attribute("optionalString", new StringType(), "optionalString", false));
        return r;
    }

    public static class BooleanType extends TM3AttributeValueType.BooleanType {
        public BooleanType() {
            super(false);
        }
    }
    public static class StringType extends TM3AttributeValueType.StringType {
        public StringType() {
            super(10, false);
        }
    }

    @SuppressWarnings("unchecked")
    public static void cleanAllDbs() throws SQLException {
        Session session = sessionFactory.openSession();
        TM3Manager manager = DefaultManager.create();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            
            List<BaseTm<?>> tms = session.createCriteria(BaseTm.class).list();
            for (BaseTm<?> tm : tms) {
                System.out.println("Cleaning up TM " + tm.getId());
                tm.setSession(session);
                manager.removeTm(session, tm);
            }

            tx.commit();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (tx != null) {
                tx.rollback();
            }
        }
        finally {
            if (session.isOpen()) {
                session.close();
            }
        }
    }
    
    // Test TUV data class -- just a string
    static class TestData implements TM3Data {
        private String data;
        TestData(String data) {
            this.data = data;
        }
        @Override
        public String getSerializedForm() {
            return data;
        }

        @Override
        public long getFingerprint() {
            return Fingerprint.fromString(data);
        }

        @Override
        public Iterable<Long> tokenize() {
            String[] words = data.split(" ");
            ArrayList<Long> fingerprints = new ArrayList<Long>(words.length);
            for (String word: words) {
                fingerprints.add(Fingerprint.fromString(word));
            }
            return fingerprints;
        }
        @Override
        public boolean equals(Object o) {
            return (o instanceof TestData && 
                    ((TestData)o).data.equals(data));
        }
        @Override
        public String toString() {
            return "TestData(" + data + ")";
        }
    }

  
    static class TestLocale implements TM3Locale {
        private long id;
        private String language;
        private String locale;
        
        TestLocale(long id, String language, String locale) {
            this.id = id;
            this.language = language;
            this.locale = locale;
        }
        
        @Override
        public long getId() {
            return id;
        }

        @Override
        public String getLocaleCode() {
            return locale;
        }

        @Override
        public String getLanguage() {
            return language;
        }
        
        @Override
        public String toString() {
            return language + "_" + locale;
        }
    }
    
    static class TestDataFactory implements TM3DataFactory<TestData> {
        private TM3FuzzyMatchScorer<TestData> scorer = new TestScorer();
        
        @Override
        public TestData fromSerializedForm(TM3Locale locale, String value) {
            return new TestData(value);
        }

        @Override
        public TM3FuzzyMatchScorer<TestData> getFuzzyMatchScorer() {
            return scorer;
        }
        
        public void setFuzzyMatchScorer(TM3FuzzyMatchScorer<TestData> scorer) {
            this.scorer = scorer;
        }

        @Override
        public TestLocale getLocaleById(Session session, long id) {
            switch ((int)id) {
            case 1:
                return EN_US;
            case 2:
                return FR_FR;
            case 3:
                return DE_DE;
            default:
                throw new RuntimeException("Unknown locale id " + id);
            }
        }
        
        @Override
        public TestLocale getLocaleByCode(Session session, String code) {
            String[] parts = code.split("_");
            if (parts.length != 2) {
                return null;
            }
            if (parts[0].equals("en") && parts[1].equals("US")) {
                return EN_US;
            }
            else if (parts[0].equals("fr") && parts[1].equals("FR")) {
                return FR_FR;
            }
            else if (parts[0].equals("de") && parts[1].equals("DE")) {
                return DE_DE;
            }
            throw new RuntimeException("Unkown locale " + code);
        }

        @Override
        public Configuration extendConfiguration(Configuration cfg) {
            return cfg;
        }

    }
    
    // Variant that returns a specified fingerprint rather
    // than one computed from the content, and also reimplements
    // equals() to just compare the fingerprint.
    static class FixedValueTestData extends TestData {
        private long fingerprint;
        FixedValueTestData(String data, long fingerprint) {
            super(data);
            this.fingerprint = fingerprint;
        }

        @Override
        public long getFingerprint() {
            return fingerprint;
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FixedValueTestData)) {
                return false;
            }
            FixedValueTestData d = (FixedValueTestData)o;
            return getFingerprint() == d.getFingerprint();
        }
        
        @Override
        public String toString() {
            return "Fixed(" + fingerprint + ")[" + getSerializedForm() + "]";
        }
    }
    
    static class FixedValueTestDataFactory extends TestDataFactory {
        private long fingerprint;
        FixedValueTestDataFactory(long fingerprint) {
            this.fingerprint = fingerprint;
        }
        
        @Override
        public TestData fromSerializedForm(TM3Locale locale, String value) {
            return new FixedValueTestData(value, fingerprint);
        }
    }
    
    // Remove the DB used by the current test
    void cleanupTestDb(TM3Manager manager) throws Exception {
        Transaction tx = currentSession.beginTransaction();            
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        if (tm != null) {
            manager.removeTm(currentSession, tm);
        }
        tx.commit();
    }
    
    // TODO: handle attribute comparisons
    void expectResults(TM3LeverageResults<TestData> results, Expected...e) {
        List<Expected> expected = Arrays.asList(e);
        Iterator<TM3LeverageMatch<TestData>> it = results.getMatches().iterator();
        for (int i = 0; i < expected.size(); i++) {
            Expected ex = expected.get(i);
            assertTrue("No result for " + ex, it.hasNext());
            TM3LeverageMatch<TestData> match = it.next();
            ex.check(match, "Mismatch for result " + i);
        }
        assertFalse("More results than expected", it.hasNext());
    }
    
    Expected expected(TestData data, boolean exact) {
        return new Expected(data, exact, new HashMap<TM3Attribute, Object>());
    }
    Expected expected(TestData data, boolean exact, Map<TM3Attribute, Object> attributes) {
        return new Expected(data, exact, attributes);
    }
    static class Expected {
        boolean exact;
        TestData data;
        Map<TM3Attribute, Object> attributes;
        Expected(TestData data, boolean exact, Map<TM3Attribute, Object> attributes) {
            this.exact = exact;
            this.data = data;
            this.attributes = attributes;
        }
        @Override
        public String toString() {
            return "Expected(exact=" + exact + ", " + data + ")";
        }
        
        public void check(TM3LeverageMatch<TestData> match, String message) {
            assertEquals(message, exact, match.isExact());
            assertEquals(message, data, match.getTuv().getContent());
        }
    }
    
    static class TestScorer implements TM3FuzzyMatchScorer<TestData> {

        @Override
        public float score(TestData matchKey, TestData candidate,
                TM3Locale locale) {
            
            // use list instead of set so we count duplicate terms..,
            List<String> w1 = Arrays.asList(matchKey.data.split(" "));
            List<String> w2 = Arrays.asList(candidate.data.split(" "));
            
            int total = 0;
            for (String s : w1) {
                if (w2.contains(s)) {
                    total++;
                }
            }
            return ((float)total * 2f) / (float)(w1.size() + w2.size());
        }
        
    }

    // 
    // Test methods
    // These just call into the implementations below
    //
    @Test
    public void testExactMatch() throws Exception {
        testExactMatch(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testUnicodeContent() throws Exception {
        testUnicodeContent(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }

    
    @Test
    public void testExactMatchingWithAttrs() throws Exception {
        testExactMatchingWithAttrs(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testExactMachingWithNonIdentityAttributes() throws Exception {
        testExactMachingWithNonIdentityAttributes(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testExactMatchingWithInlineAttrs() throws Exception {
        testExactMatchingWithInlineAttrs(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testExactMatchingWithNoResults() throws Exception {
        testExactMatchingWithNoResults(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testExactMatchWithTargetLocales() throws Exception {
        testExactMatchWithTargetLocales(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR, DE_DE);
    }
    
    @Test
    public void testFuzzyMatching() throws Exception {
        testFuzzyMatching(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testFuzzyTargetMatching() throws Exception {
        testFuzzyTargetMatching(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testFuzzyMatchingWithAttributes() throws Exception {
        testFuzzyMatchingWithAttributes(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testFuzzyMatchingWithInlineAttributes() throws Exception {
        testFuzzyMatchingWithInlineAttributes(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testFuzzyMatchingWithNoResults() throws Exception {
        testFuzzyMatchingWithNoResults(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR);
    }
    
    @Test
    public void testFuzzyMatchingWithTargetLocales() throws Exception {
        testFuzzyMatchingWithTargetLocales(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR, DE_DE);
    }
    
    @Test
    public void testTuIdentityWithAttributes() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testTuIdentityWithAttributes(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testModifyTuv() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testModifyTuv(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testAddDeleteTuv() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testAddDeleteTuv(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testUpdateTuAttrs() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testUpdateTuAttrs(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testUpdateSourceTuv() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testUpdateSourceTuv(tm, EN_US, FR_FR);
    }

    @Test
    public void testDontReturnRedundantResults() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testDontReturnRedundantResults(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testTuvEvents() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testTuvEvents(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testIdempotentMergeMode() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testIdempotentMergeMode(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testMatchType() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testMatchType(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testFuzzyMatchThreshold() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testFuzzyMatchThresholdAndLimit(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testEmptyFuzzyQuery() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testEmptyFuzzyQuery(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testIdenticalScores() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testIdenticalScores(tm, EN_US, FR_FR);
    }
      
    @Test
    public void testMaxResults() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testMaxResultsFilterOrdering(tm, EN_US, FR_FR);
    }

    @Test
    public void testGetAllTuData() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testGetAllTuData(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testGetAllTuDataWithDateRange() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testGetAllTuDataWithDateRange(tm, EN_US, FR_FR);
    }

    @Test
    public void testGetTuDataByLocale() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testGetTuDataByLocale(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testGetTuDataByLocaleWithDateRange() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testGetAllTuDataWithDateRange(tm, EN_US, FR_FR);
    }

    @Test
    public void testGetTuDataById() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testGetTuDataById(tm, EN_US, FR_FR);
    }

    @Test
    public void testLockOnSave() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testLockOnSave(tm, EN_US, FR_FR);
    }

    @Test
    public void testLockOnModify() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testLockOnModify(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testMultipleTargets() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testMultipleTargets(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testIdenticalTargets() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testIdenticalTargets(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testMerge() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testMerge(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testMergeIdentical() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testMergeIdentical(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testMergeWithIdenticalTargets() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testMergeWithIdenticalTargets(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testOverwrite() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testOverwrite(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testOverwriteOfMultipleTargets() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testOverwriteOfMultipleTargets(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testOverwriteOfMultipleTargetsWithIdentical() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testOverwriteOfMultipleTargetsWithIdentical(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testOverwriteWithMultipleTargets() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testOverwriteWithMultipleTargets(tm, EN_US, FR_FR);
    }
    
    @Test
    public void testOverwriteWithIdenticalTargets() throws Exception {
        TM3Tm<TestData> tm = manager.getTm(currentSession, FACTORY, currentTestId);
        testOverwriteWithIdenticalTargets(tm, EN_US, FR_FR);
    }
  
    @Test
    public void testFixedFingerprintTuvs() throws Exception {
        testFixedFingerprintTuvs(EN_US, FR_FR);
    }

    @Test
    public void testDataByLocaleOrdering() throws Exception {
        testDataByLocaleOrdering(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR, DE_DE);
    }
    
    @Test
    public void testFuzzyLookupTargetOnlySearchesTargets() throws Exception {
        testFuzzyLookupTargetOnlySearchesTargets(
                manager.getTm(currentSession, FACTORY, currentTestId), EN_US, FR_FR, DE_DE);
    }
    
    // 
    // Test implementations
    //
    
    private void verifyExact(TM3Tm<TestData> tm, TestData src, TM3Locale srcLocale,
                        TestData tgt, TM3Locale tgtLocale, boolean lookupTarget) {
        TM3LeverageResults<TestData> results = 
            tm.findMatches(src, srcLocale, null, null, TM3MatchType.EXACT, lookupTarget);
        expectResults(results, expected(src, true));
        TM3LeverageMatch<TestData> match = results.getMatches().first();
        assertEquals(100, match.getScore());
        assertEquals(tgt, getLocaleTuv(match.getTu(), tgtLocale).getContent());
        
        // Now do the query again and make sure that the same result isn't 
        // returned as both an exact and a fuzzy match.
        results = tm.findMatches(src, srcLocale, null, null, TM3MatchType.ALL, lookupTarget);
        Iterator<TM3LeverageMatch<TestData>> it = results.getMatches().iterator();
        expected(src, true).check(it.next(), "exact match was not the first result for ALL search");
        while (it.hasNext()) {
            match = it.next();
            assertFalse("same result returned twice", match.getTuv().getContent().equals(src));
        }
    }
    
    public void testExactMatch(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData srcData2 = new TestData("This is source 2");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            tm.save(srcLocale, srcData1, null, 
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, null, 
                    tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Now let's do an exact match query
            currentTransaction = currentSession.beginTransaction();
            verifyExact(tm, srcData1, srcLocale, tgtData1, tgtLocale, false);
            verifyExact(tm, srcData2, srcLocale, tgtData2, tgtLocale, false);

            // exact target match
            verifyExact(tm, tgtData1, tgtLocale, srcData1, srcLocale, true);
            verifyExact(tm, tgtData2, tgtLocale, srcData2, srcLocale, true);

            // but not if we don't ask for it
            TM3LeverageResults<TestData> results =
                tm.findMatches(tgtData1, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results);
            
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    // Ensure the TUVs can handle unicode (ie, we are creating storage correctly)
    public void testUnicodeContent(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("\u306b\u307b\u3093"); // Hiragana "Nihon"
            tm.save(srcLocale, srcData1, null, 
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Now let's do an exact match query
            currentTransaction = currentSession.beginTransaction();
            verifyExact(tm, srcData1, srcLocale, tgtData1, tgtLocale, false);

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testExactMatchingWithAttrs(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            final TM3Attribute attr1 = tm.getAttributeByName("test");
            assertNotNull(attr1);
            
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            tm.save(srcLocale, srcData1, TM3Attributes.one(attr1, "bad"),
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData1, TM3Attributes.one(attr1, "good"), 
                    tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Now let's do an exact match query
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("This is source 1");

            // First, search without attributes -- this should catch both
            TM3LeverageResults<TestData> results = 
                tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(2, results.getMatches().size());
            
            // Next, search by an attr value -- this should only find 1
            results = tm.findMatches(key, srcLocale, null,
                    TM3Attributes.one(attr1, "good"), TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());
            assertEquals(key, match.getTuv().getContent());
            assertEquals(new TestData("This is target 2"),
                         getLocaleTuv(match.getTu(), FR_FR).getContent());
            currentTransaction.commit();

            cleanupTestDb(manager);          
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testExactMachingWithNonIdentityAttributes(TM3Tm<TestData> tm, 
            TestLocale srcLocale, final TestLocale tgtLocale) throws Exception {

        try {
            currentTransaction = currentSession.beginTransaction();

            final TM3Attribute attr1 = tm.getAttributeByName("inlineString");
            final TM3Attribute attr2 = tm.getAttributeByName("optionalString");

            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            
            // Save two segments that are identical in text and identity-determining
            // attributes, but different in a non-identity-affecting attribute.
            // The result of this should be a single TU in the DB, with two 
            // target tuvs.
            tm.save(srcLocale, srcData1, TM3Attributes.many(
                    TM3Attributes.entry(attr1, "required"),
                    TM3Attributes.entry(attr2, "optional1")),
                tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData1, TM3Attributes.many(
                    TM3Attributes.entry(attr1, "required"),
                    TM3Attributes.entry(attr2, "optional2")),
                tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();

            TM3LeverageResults<TestData> results = 
                tm.findMatches(srcData1, srcLocale, null, 
                               TM3Attributes.one(attr1, "required"),
                               TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targets = tu.getTargetTuvs();
            assertEquals(2, targets.size());
            assertEquals(tgtData1, targets.get(0).getContent());
            assertEquals(tgtLocale, targets.get(0).getLocale());
            assertEquals(tgtData2, targets.get(1).getContent());
            assertEquals(tgtLocale, targets.get(1).getLocale());
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testExactMatchingWithInlineAttrs(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            final TM3Attribute attr1 = tm.getAttributeByName("test");
            final TM3Attribute attr2 = tm.getAttributeByName("inlineString");
            final TM3Attribute attr3 = tm.getAttributeByName("inlineBoolean");
            assertNotNull(attr1);
            
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            tm.save(srcLocale, srcData1, TM3Attributes.many(
                        TM3Attributes.entry(attr1, "bad"),
                        TM3Attributes.entry(attr2, "BAD"),
                        TM3Attributes.entry(attr3, false)),
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData1, TM3Attributes.many(
                        TM3Attributes.entry(attr1, "good"),
                        TM3Attributes.entry(attr2, "GOOD"),
                        TM3Attributes.entry(attr3, true)),
                    tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Now let's do an exact match query
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("This is source 1");

            // First, search without attributes -- this should catch both
            TM3LeverageResults<TestData> results = 
                tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(2, results.getMatches().size());
            
            // Next, search by attrs -- this should only find 1
            results = tm.findMatches(key, srcLocale, null,
                    TM3Attributes.many(
                        TM3Attributes.entry(attr1, "good"),
                        TM3Attributes.entry(attr2, "GOOD"),
                        TM3Attributes.entry(attr3, true)),
                    TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());
            assertEquals(key, match.getTuv().getContent());
            assertEquals(new TestData("This is target 2"),
                         getLocaleTuv(match.getTu(), FR_FR).getContent());
            currentTransaction.commit();

            cleanupTestDb(manager);          
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testExactMatchingWithNoResults(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            final TM3Attribute attr1 = tm.getAttributeByName("test");
            assertNotNull(attr1);
            
            TM3LeverageResults<TestData> results = tm.findMatches(new TestData("blah blah"), 
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.EXACT, false);
            assertEquals(0, results.getMatches().size());
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testExactMatchWithTargetLocales(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale, final TestLocale altLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            tm.save(srcLocale, srcData1, null, 
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();

            // exact match with the correct target locale
            TM3LeverageResults<TestData> results = 
                tm.findMatches(srcData1, srcLocale, Collections.singleton(tgtLocale), null, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));

            // exact match with the correct target locale plus another
            Set<TestLocale> tgtPlusAltLocales = new HashSet<TestLocale>();
            Collections.addAll(tgtPlusAltLocales, tgtLocale, altLocale);
            results = 
                tm.findMatches(srcData1, srcLocale, tgtPlusAltLocales, null, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));

            // exact match with the wrong target locale
            results = 
                tm.findMatches(srcData1, srcLocale, Collections.singleton(altLocale), null, TM3MatchType.EXACT, false);
            expectResults(results);

            // exact target match with the correct source locale
            results = 
                tm.findMatches(tgtData1, tgtLocale, Collections.singleton(srcLocale), null, TM3MatchType.EXACT, true);
            expectResults(results, expected(tgtData1, true));

            // exact target match with the correct source locale plus another
            Set<TestLocale> srcPlusAltLocales = new HashSet<TestLocale>();
            Collections.addAll(srcPlusAltLocales, srcLocale, altLocale);
            results = 
                tm.findMatches(tgtData1, tgtLocale, srcPlusAltLocales, null, TM3MatchType.EXACT, true);
            expectResults(results, expected(tgtData1, true));

            // exact target match with the wrong source locale
            results = 
                tm.findMatches(tgtData1, tgtLocale, Collections.singleton(altLocale), null, TM3MatchType.EXACT, true);
            expectResults(results);

            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testFuzzyMatching(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            // Create some segments.
            TestData srcData1 = new TestData("The quick brown fox ran up the stairs.");
            TestData srcData2 = new TestData("foo bar baz");
            TestData srcData3 = new TestData("The quick brown fox jumped over the lazy cats.");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, TM3Attributes.NONE, tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData3, TM3Attributes.NONE, tgtLocale, tgtData3, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Do a fuzzy query
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("The quick brown fox jumped over the lazy dogs.");

            TM3LeverageResults<TestData> results = tm.findMatches(key, 
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results,
                    expected(srcData3, false),
                    expected(srcData1, false)
            );

            // Make sure target leveraging doesn't find anything
            key = new TestData("This is target");
            results = tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results);
            
            currentTransaction.commit();

            cleanupTestDb(manager); 
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }

    }
    
    public void testFuzzyTargetMatching(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            // Create some segments.
            TestData srcData1 = new TestData("The quick brown fox ran up the stairs.");
            TestData srcData2 = new TestData("foo bar baz");
            TestData srcData3 = new TestData("The quick brown fox jumped over the lazy cats.");
            TestData tgtData1 = new TestData("fee fie foe");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("fee fie foe fum");
            tm.setIndexTarget(true);
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, TM3Attributes.NONE, tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData3, TM3Attributes.NONE, tgtLocale, tgtData3, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Do a fuzzy query
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("The quick brown fox jumped over the lazy dogs.");

            TM3LeverageResults<TestData> results = tm.findMatches(key, 
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results,
                    expected(srcData3, false),
                    expected(srcData1, false)
            );

            // Do a target fuzzy query
            key = new TestData("fee fie foe fum plonk");

            results = tm.findMatches(key, 
                    tgtLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results,
                    expected(tgtData3, false),
                    expected(tgtData1, false)
            );
            
            // Make sure we don't find targets if we don't ask for them
            key = new TestData("fee fie foe fum plonk");
            results = tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results);

            currentTransaction.commit();

            cleanupTestDb(manager); 
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            tm.setIndexTarget(false);
        }

    }
    
    public void testFuzzyMatchingWithAttributes(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            final TM3Attribute attr1 = tm.getAttributeByName("test");
            assertNotNull(attr1);
            
            // Create some segments.
            TestData srcData1 = new TestData("The quick brown fox ran up the stairs.");
            TestData srcData2 = new TestData("foo bar baz");
            TestData srcData3 = new TestData("The quick brown fox jumped over the lazy cats.");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");
            tm.save(srcLocale, srcData1, TM3Attributes.one(attr1, "yes"), 
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, TM3Attributes.one(attr1, "yes"), 
                    tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData3, TM3Attributes.one(attr1, "no"), 
                    tgtLocale, tgtData3, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Do a fuzzy query matching on the attribute value 'yes'.  This should match
            // src1 (attr + fuzzy), but not src2 (attr, but no fuzzy) or src3 (no attr)
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("The quick brown fox jumped over the lazy dogs.");

            // First, search with attributes -- this should catch both
            TM3LeverageResults<TestData> results = tm.findMatches(key, 
                    srcLocale, null, TM3Attributes.one(attr1, "yes"), TM3MatchType.ALL, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> first = results.getMatches().first();
            TM3Tu<TestData> match = first.getTu();
            System.out.println("Matched [" + first.getTuv().getContent() + 
                               "] with score " + first.getScore());
            assertEquals(srcData1, first.getTuv().getContent());
            assertEquals(1, match.getAttributes().size());
            assertNotNull(match.getAttribute(attr1));
            assertEquals("yes", match.getAttribute(attr1));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testFuzzyMatchingWithInlineAttributes(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            final TM3Attribute attr1 = tm.getAttributeByName("test");
            final TM3Attribute attr2 = tm.getAttributeByName("inlineString");
            final TM3Attribute attr3 = tm.getAttributeByName("inlineBoolean");
            assertNotNull(attr1);
            
            // Create some segments.
            TestData srcData1 = new TestData("The quick brown fox ran up the stairs.");
            TestData srcData2 = new TestData("foo bar baz");
            TestData srcData3 = new TestData("The quick brown fox jumped over the lazy cats.");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");
            tm.save(srcLocale, srcData1, TM3Attributes.many(
                        TM3Attributes.entry(attr1, "yes"),
                        TM3Attributes.entry(attr2, "YES"),
                        TM3Attributes.entry(attr3, true)),
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, TM3Attributes.many(
                        TM3Attributes.entry(attr1, "yes"),
                        TM3Attributes.entry(attr2, "YES"),
                        TM3Attributes.entry(attr3, true)),
                    tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData3, TM3Attributes.many(
                        TM3Attributes.entry(attr1, "no"),
                        TM3Attributes.entry(attr2, "NO"),
                        TM3Attributes.entry(attr3, false)),
                    tgtLocale, tgtData3, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Do a fuzzy query matching on the attribute value 'yes'.  This should match
            // src1 (attr + fuzzy), but not src2 (attr, but no fuzzy) or src3 (no attr)
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("The quick brown fox jumped over the lazy dogs.");

            // First, search with attributes -- this should catch both
            TM3LeverageResults<TestData> results = tm.findMatches(key, 
                    srcLocale, null, TM3Attributes.many(
                        TM3Attributes.entry(attr1, "yes"),
                        TM3Attributes.entry(attr2, "YES"),
                        TM3Attributes.entry(attr3, true)),
                    TM3MatchType.ALL, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> first = results.getMatches().first();
            TM3Tu<TestData> match = first.getTu();
            System.out.println("Matched [" + first.getTuv().getContent() + 
                               "] with score " + first.getScore());
            assertEquals(srcData1, first.getTuv().getContent());
            assertEquals(3, match.getAttributes().size());
            assertNotNull(match.getAttribute(attr1));
            assertEquals("yes", match.getAttribute(attr1));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testFuzzyMatchingWithNoResults(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            final TM3Attribute attr1 = tm.getAttributeByName("test");
            assertNotNull(attr1);
            
            TM3LeverageResults<TestData> results = tm.findMatches(new TestData("blah blah"),
                    srcLocale, null,  TM3Attributes.one(attr1, "yes"), TM3MatchType.ALL, false);
            assertEquals(0, results.getMatches().size());
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testFuzzyMatchingWithTargetLocales(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale, final TestLocale altLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.addAttribute("test");
            currentSession.flush();

            // Create some segments.
            TestData srcData1 = new TestData("The quick brown fox ran up the stairs.");
            TestData tgtData1 = new TestData("This is target 1");
            tm.setIndexTarget(true);
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("The quick brown fox jumped over the lazy dogs.");

            // fuzzy query with the correct target locale
            TM3LeverageResults<TestData> results = tm.findMatches(key, 
                    srcLocale, Collections.singleton(tgtLocale), TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results, expected(srcData1, false));

            // fuzzy query with the correct target locale plus another
            Set<TestLocale> tgtPlusAltLocales = new HashSet<TestLocale>();
            Collections.addAll(tgtPlusAltLocales, tgtLocale, altLocale);
            results = tm.findMatches(key, 
                    srcLocale, tgtPlusAltLocales, TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results, expected(srcData1, false));

            // fuzzy query with the wrong target locale
            results = tm.findMatches(key, 
                    srcLocale, Collections.singleton(altLocale), TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results);

            key = new TestData("This is target");

            // fuzzy target query with the correct source locale
            results = tm.findMatches(key, 
                    tgtLocale, Collections.singleton(srcLocale), TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results, expected(tgtData1, false));

            // fuzzy target query with the correct source locale plus another
            Set<TestLocale> srcPlusAltLocales = new HashSet<TestLocale>();
            Collections.addAll(srcPlusAltLocales, srcLocale, altLocale);
            results = tm.findMatches(key, 
                    tgtLocale, tgtPlusAltLocales, TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results, expected(tgtData1, false));

            // fuzzy target query with the wrong source locale
            results = tm.findMatches(key, 
                    tgtLocale, Collections.singleton(altLocale), TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results);

            currentTransaction.commit();

            cleanupTestDb(manager); 
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            tm.setIndexTarget(false);
        }

    }
    
    // Make sure the different values for TM3MatchType do what they're supposed to.
    public void testMatchType(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();

            // Create some segments.
            TestData srcData1 = new TestData("I like to eat flying fish");
            TestData srcData2 = new TestData("I like to eat soup");
            TestData srcData3 = new TestData("I swam across the Thames");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, TM3Attributes.NONE, tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData3, TM3Attributes.NONE, tgtLocale, tgtData3, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            TM3LeverageResults<TestData> results;
            
            // Test EXACT mode
            results = tm.findMatches(
                    new TestData("I like to eat flying fish"),
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results,
                    expected(srcData1, true)
            );
            results = tm.findMatches(
                    new TestData("I like to eat soup"),
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results,
                    expected(srcData2, true)
            );
            
            // Test ALL
            results = tm.findMatches(
                    new TestData("I like to eat flying fish"),
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results,
                    expected(srcData1, true),
                    expected(srcData2, false)
            );
            
            // Test fallback
            results = tm.findMatches(
                    new TestData("I like to eat flying toast"),
                    srcLocale, null, TM3Attributes.NONE, TM3MatchType.FALLBACK, false);
            expectResults(results,
                    expected(srcData1, false),
                    expected(srcData2, false)
            );
            
            currentTransaction.commit();

            cleanupTestDb(manager); 
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }

    }
    
    public void testTuIdentityWithAttributes(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            final TM3Attribute attr1 = tm.addAttribute("test1");
            final TM3Attribute attr2 = tm.addAttribute("inlineString");
            final TM3Attribute attr3 = tm.addAttribute("inlineBoolean");
            currentSession.flush();
            
            TestData srcData1 = new TestData("source data 1"),
                     tgtData1 = new TestData("tgt data 1"),
                     tgtData2 = new TestData("tgt data 2"),
                     tgtData3 = new TestData("tgt data 3"),
                     tgtData4 = new TestData("tgt data 4");
            
            Map<TM3Attribute, Object> attrSet1 = TM3Attributes.one(attr1, "yes");
            Map<TM3Attribute, Object> attrSet2 = TM3Attributes.many(
                TM3Attributes.entry(attr1, "yes"),
                TM3Attributes.entry(attr2, "YES"),
                TM3Attributes.entry(attr3, true));
            
            // Note: these have the same source, but different attribute sets => different identities
            tm.save(srcLocale, srcData1, attrSet1, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData1, attrSet2, tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);

            // These should add to the version with the matching attribute set
            tm.save(srcLocale, srcData1, attrSet1, tgtLocale, tgtData3, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData1, attrSet2, tgtLocale, tgtData4, TM3SaveMode.MERGE, currentTestEvent);
            
            TM3LeverageResults<TestData> results = tm.findMatches(srcData1,
                    srcLocale, null, attrSet1, TM3MatchType.EXACT, false);            
            assertEquals(2, results.getMatches().size());
            for (TM3LeverageMatch<TestData> match : results.getMatches()) {
                TM3Tu<TestData> tu = match.getTu();
                if (tu.getAttributes().equals(attrSet1)) {
                    List<TM3Tuv<TestData>> targets = tu.getTargetTuvs();
                    assertEquals(2, targets.size());
                    TestData foundData1 = targets.get(0).getContent(); 
                    assertTrue(foundData1.equals(tgtData1) || foundData1.equals(tgtData3));
                    TestData foundData2 = targets.get(1).getContent(); 
                    assertTrue(foundData2.equals(tgtData1) || foundData2.equals(tgtData3));
                    assertNotSame(foundData1, foundData2);
                }
                else if (tu.getAttributes().equals(attrSet2)) {
                    List<TM3Tuv<TestData>> targets = tu.getTargetTuvs();
                    assertEquals(2, targets.size());
                    TestData foundData1 = targets.get(0).getContent(); 
                    assertTrue(foundData1.equals(tgtData2) || foundData1.equals(tgtData4));
                    TestData foundData2 = targets.get(1).getContent(); 
                    assertTrue(foundData2.equals(tgtData2) || foundData2.equals(tgtData4));
                    assertNotSame(foundData1, foundData2);                    
                }
                else {
                    fail("Unexpected attribute set for tu " + tu + ": " + tu.getAttributes());
                }
            }
                       
            currentTransaction.commit();
            
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testModifyTuv(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();

            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("a b c");
            TestData tgtData2 = new TestData("c b a");
            tm.setIndexTarget(true);
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Make sure the data is all intact
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("This is source 1");
            TM3LeverageResults<TestData> results = 
                tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());            
            TM3Tu<TestData> tu = match.getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData1, getLocaleTuv(tu, tgtLocale).getContent());
            
            // Now update one of the tuvs, but not the other
            getLocaleTuv(tu, tgtLocale).setContent(tgtData2);
            tm.modifyTu(tu, currentTestEvent);
            currentTransaction.commit();
            
            // Now make sure it persisted
            currentTransaction = currentSession.beginTransaction();
            results = tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            tu = results.getMatches().first().getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData2, getLocaleTuv(tu, tgtLocale).getContent());

            // make sure the indexes were updated
            // note that tgtData1 and tgtData2, while similar according to
            // TestScorer, have no trigrams in common, in order to isolate the
            // trigram indexes from the scoring

            // fuzzy matches on the new target
            key = new TestData("d c b a");
            results =
                tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results, expected(tgtData2, false));

            // no fuzzy matches on the old target
            key = new TestData("a b c d");
            results =
                tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results);
            
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            tm.setIndexTarget(false);
        }
    }
    
    public void testAddDeleteTuv(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();

            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("a b c");
            TestData tgtData2 = new TestData("c b a");
            tm.setIndexTarget(true);
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Make sure the data is all intact
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("This is source 1");
            TM3LeverageResults<TestData> results = 
                tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());            
            TM3Tu<TestData> tu = match.getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData1, getLocaleTuv(tu, tgtLocale).getContent());
            
            // Delete the old TUV.  Add another.
            tu.removeTargetTuvs();
            tu.addTargetTuv(FR_FR, tgtData2, currentTestEvent);
            tm.modifyTu(tu, currentTestEvent);
            currentTransaction.commit();
            
            // Now make sure it persisted
            currentTransaction = currentSession.beginTransaction();
            results = tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            tu = results.getMatches().first().getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData2, getLocaleTuv(tu, tgtLocale).getContent());

            // make sure the indexes were updated
            // note that tgtData1 and tgtData2, while similar according to
            // TestScorer, have no trigrams in common, in order to isolate the
            // trigram indexes from the scoring

            // fuzzy matches on the new target
            key = new TestData("d c b a");
            results =
                tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results, expected(tgtData2, false));

            // no fuzzy matches on the old target
            key = new TestData("a b c d");
            results =
                tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results);
            
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            tm.setIndexTarget(false);
        }
    }
    
    public void testUpdateTuAttrs(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            TM3Attribute attr1 = tm.addAttribute("attr1");
            TM3Attribute attr2 = tm.addAttribute("inlineString");

            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            tm.save(srcLocale, srcData1, TM3Attributes.many(TM3Attributes.entry(attr1, "attr1_val1"),
                                         TM3Attributes.entry(attr2, "attr2_val1")), 
                    tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Make sure the data is all intact
            currentTransaction = currentSession.beginTransaction();
            TestData key = new TestData("This is source 1");
            TM3LeverageResults<TestData> results = 
                tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());            
            TM3Tu<TestData> tu = match.getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData1, getLocaleTuv(tu, tgtLocale).getContent());
            Map<TM3Attribute, Object> attrs = tu.getAttributes();
            assertEquals(2, attrs.entrySet().size());
            assertEquals("attr1_val1", attrs.get(attr1));
            assertEquals("attr2_val1", attrs.get(attr2));
            
            // Now update the attributes
            tu.setAttribute(attr1, "attr1_val2");
            attrs.put(attr2, "attr2_val2");
            tm.modifyTu(tu, currentTestEvent);
            currentTransaction.commit();
            
            // Now make sure it persisted
            currentTransaction = currentSession.beginTransaction();
            results = tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            tu = results.getMatches().first().getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData1, getLocaleTuv(tu, tgtLocale).getContent());
            attrs = tu.getAttributes();
            assertEquals(2, attrs.entrySet().size());
            assertEquals("attr1_val2", attrs.get(attr1));
            assertEquals("attr2_val2", attrs.get(attr2));
            currentTransaction.commit();
            
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testUpdateSourceTuv(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();

            // Now let's create some segments.
            TestData srcData1 = new TestData("a b c");
            TestData srcData2 = new TestData("c b a");
            TestData tgtData1 = new TestData("This is target 1");
            tm.setIndexTarget(true);
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Make sure the data is all intact
            currentTransaction = currentSession.beginTransaction();
            TM3LeverageResults<TestData> results = 
                tm.findMatches(srcData1, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());            
            TM3Tu<TestData> tu = match.getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData1, getLocaleTuv(tu, tgtLocale).getContent());
            
            // Update the source
            tu.getSourceTuv().setContent(srcData2);
            tm.modifyTu(tu, currentTestEvent);
            currentTransaction.commit();
            
            // Now make sure it persisted
            currentTransaction = currentSession.beginTransaction();
            results = tm.findMatches(srcData2, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            tu = results.getMatches().first().getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            assertEquals(tgtData1, getLocaleTuv(tu, tgtLocale).getContent());

            // make sure the indexes were updated
            // note that srcData1 and srcData2, while similar according to
            // TestScorer, have no trigrams in common, in order to isolate the
            // trigram indexes from the scoring

            // fuzzy matches on the new source
            TestData key = new TestData("d c b a");
            results =
                tm.findMatches(key, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results, expected(srcData2, false));

            // no fuzzy matches on the old source
            key = new TestData("a b c d");
            results =
                tm.findMatches(key, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, false);
            expectResults(results);
            
            // fuzzy matches on the new target
            key = new TestData("This is target");
            results =
                tm.findMatches(key, tgtLocale, null,  TM3Attributes.NONE, TM3MatchType.ALL, true);
            expectResults(results, expected(tgtData1, false));

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            tm.setIndexTarget(false);
        }
    }
    
    public void testDontReturnRedundantResults(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            tm.save(srcLocale, srcData1, null, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            // Now let's do an exact match query
            currentTransaction = currentSession.beginTransaction();
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null, null, TM3MatchType.ALL, false);
            assertEquals(1, results.getMatches().size());
            expectResults(results, expected(srcData1, true));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    // Make sure that we don't store identical target TUVs for a single TU
    @SuppressWarnings("serial")
    public void testIdempotentMergeMode(TM3Tm<TestData> tm, TestLocale srcLocale, 
                              final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            TestData tgtData2 = new TestData("This is target 1"); // identical to tgtData1
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            TM3LeverageResults<TestData> results = 
                tm.findMatches(srcData1, srcLocale, null, TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            TM3Tu<TestData> tu = match.getTu();
            assertNotNull(tu);
            List<TM3Tuv<TestData>> tuvs = tu.getTargetTuvs();
            assertEquals(1, tuvs.size());
            assertEquals(tgtLocale, tuvs.get(0).getLocale());
            assertEquals(tgtData1, tuvs.get(0).getContent());
            currentTransaction.commit();
            
            // Now do the same thing again, but using the multi-target save signature.
            currentTransaction = currentSession.beginTransaction();
            TestData srcData2 = new TestData("This is source 2");
            final TestData target2 = new TestData("Target 2");
            tm.save(srcLocale, srcData2, TM3Attributes.NONE, tgtLocale, target2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            // This should fail
            tm.save(srcLocale, srcData2, TM3Attributes.NONE, 
                    new HashMap<TM3Locale, TestData>() {{
                       put(tgtLocale, target2); 
                    }}, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            results = 
                tm.findMatches(srcData2, srcLocale, null, TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData2, true));
            match = results.getMatches().first();
            tu = match.getTu();
            assertNotNull(tu);
            tuvs = tu.getTargetTuvs();
            assertEquals(1, tuvs.size());
            assertEquals(tgtLocale, tuvs.get(0).getLocale());
            assertEquals(target2, tuvs.get(0).getContent());
            currentTransaction.commit();
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testTuvEvents(TM3Tm<TestData> tm, TestLocale srcLocale, 
                              TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            final TM3Event event1 = tm.addEvent(0, "tester", "Initial creation");
            
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, event1);
            currentTransaction.commit();
            
            // Make sure the data is all intact
            currentTransaction = currentSession.beginTransaction();
            TM3Event loadedEvent = tm.getEvent(event1.getId());
            assertNotNull(loadedEvent);
            assertEquals("tester", loadedEvent.getUsername());
            assertEquals("Initial creation", loadedEvent.getArgument());
            TestData key = new TestData("This is source 1");
            TM3LeverageResults<TestData> results = 
                tm.findMatches(key, srcLocale, null, TM3Attributes.NONE, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            TM3LeverageMatch<TestData> match = results.getMatches().first();
            assertTrue(match.isExact());            
            TM3Tu<TestData> tu = match.getTu();
            assertNotNull(getLocaleTuv(tu, tgtLocale));
            TM3Tuv<TestData> targetTuv = getLocaleTuv(tu, tgtLocale);
            assertEquals(tgtData1, targetTuv.getContent());
            
            assertEquals(event1, targetTuv.getLatestEvent());
            assertEquals(event1, targetTuv.getFirstEvent());
            
            // Sleep a little bit to bump timestamps
            Thread.sleep(1000);
            
            // Now update one of the tuvs, but not the other
            final TM3Event event2 = tm.addEvent(0, "tester", "TUV modification");
            getLocaleTuv(tu, tgtLocale).setContent(tgtData2);
            tm.modifyTu(tu, event2);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            results = tm.findMatches(key, srcLocale, null, null, TM3MatchType.EXACT, false);
            assertEquals(1, results.getMatches().size());
            tu = results.getMatches().first().getTu();
            targetTuv = getLocaleTuv(tu, tgtLocale);
            assertNotNull(targetTuv);
            assertEquals(tgtData2, targetTuv.getContent());

            assertEquals(event2, targetTuv.getLatestEvent());
            assertEquals(event1, targetTuv.getFirstEvent());
            
            currentTransaction.commit();
            
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    @SuppressWarnings("serial")
    public void testFuzzyMatchThresholdAndLimit(TM3Tm<TestData> tm, TestLocale srcLocale, 
                              TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            
            TestData srcData1 = new TestData("A B E F");
            TestData tgtData1 = new TestData("This is target 1");
            TestData srcData2 = new TestData("A B C G");
            TestData tgtData2 = new TestData("This is target 2");
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, srcData2, TM3Attributes.NONE, tgtLocale, tgtData2, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            
            // Search with threshold 50 -- this should pick up both
            TM3LeverageResults<TestData> results = 
                tm.findMatches(new TestData("A B C D"), srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false, 1000, 50);
            assertEquals(2, results.getMatches().size());
            expectResults(results, expected(srcData2, false), expected(srcData1, false));

            // Now seach with threshold 75 -- this will only pick up the better match
            results = tm.findMatches(new TestData("A B C D"), srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false, 1000, 75);
            assertEquals(1, results.getMatches().size());
            expectResults(results, expected(srcData2, false));
            
            // Now search with the generous threshold, but only allow one result
            results = tm.findMatches(new TestData("A B C D"), srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false, 1, 50);
            assertEquals(1, results.getMatches().size());
            expectResults(results, expected(srcData2, false));
            
            
            currentTransaction.commit();
                        
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testEmptyFuzzyQuery(TM3Tm<TestData> tm, TestLocale srcLocale, 
                            TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            
            TestData srcData1 = new TestData("A B C D E F");
            TestData tgtData1 = new TestData("This is target 1");
            tm.save(srcLocale, srcData1, TM3Attributes.NONE, tgtLocale, tgtData1, TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            
            TM3LeverageResults<TestData> results = 
                tm.findMatches(new TestData("   "), srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false, 1000, 50);
            assertEquals(0, results.getMatches().size());
            currentTransaction.commit();
                        
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testIdenticalScores(TM3Tm<TestData> tm, TestLocale srcLocale, 
            TestLocale tgtLocale) throws Exception {
        TM3FuzzyMatchScorer<TestData> oldScorer = FACTORY.getFuzzyMatchScorer();
        try {
            currentTransaction = currentSession.beginTransaction();
            
            // Use a new scorer that only returns one result
            FACTORY.setFuzzyMatchScorer(new TM3FuzzyMatchScorer<TestData>() {                
                @Override
                public float score(TestData matchKey, TestData candidate, TM3Locale locale) {
                    return 0.78f;
                }
            });
            
            tm.save(srcLocale, new TestData("A B C D E"), TM3Attributes.NONE, tgtLocale, 
                    new TestData("This is target 1"), TM3SaveMode.MERGE, currentTestEvent);
            tm.save(srcLocale, new TestData("A B C D F"), TM3Attributes.NONE, tgtLocale, 
                    new TestData("This is target 2"), TM3SaveMode.MERGE, currentTestEvent);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            
            // Search with threshold 50 -- this should pick up both
            TM3LeverageResults<TestData> results = 
                tm.findMatches(new TestData("A B C"), srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false, 1000, 50);
            assertEquals(2, results.getMatches().size());
            for (TM3LeverageMatch<TestData> match : results.getMatches()) {
                assertFalse(match.isExact());
                assertEquals(78, match.getScore());
            }
            currentTransaction.commit();
                    
            cleanupTestDb(manager);
            }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            FACTORY.setFuzzyMatchScorer(oldScorer);
        }
    }
    
    public void testMaxResultsFilterOrdering(TM3Tm<TestData> tm, TestLocale srcLocale, 
            TestLocale tgtLocale) throws Exception {
        TM3FuzzyMatchScorer<TestData> oldScorer = FACTORY.getFuzzyMatchScorer();
        try {
            currentTransaction = currentSession.beginTransaction();
            
            // Use a new scorer that only returns one result
            FACTORY.setFuzzyMatchScorer(new TM3FuzzyMatchScorer<TestData>() {
                private float next = 0.99f;
                @Override
                public float score(TestData matchKey, TestData candidate, TM3Locale locale) {
                    float f = next;
                    next -= 0.01f;
                    return f;
                }
            });
            
            for (int i = 0; i < 10; i++) {
                tm.save(srcLocale, new TestData("A B C " + i), TM3Attributes.NONE, tgtLocale, 
                        new TestData("Target " + i), TM3SaveMode.MERGE, currentTestEvent);
            }
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            
            // Search with threshold 50 -- this should pick up both
            TM3LeverageResults<TestData> results = 
                tm.findMatches(new TestData("A B C"), srcLocale, null, TM3Attributes.NONE, TM3MatchType.ALL, false, 5, 50);
            assertEquals(5, results.getMatches().size());
            Iterator<TM3LeverageMatch<TestData>> it = results.getMatches().iterator();
            assertEquals(99, it.next().getScore());
            assertEquals(98, it.next().getScore());
            assertEquals(97, it.next().getScore());
            assertEquals(96, it.next().getScore());
            assertEquals(95, it.next().getScore());
            currentTransaction.commit();
                    
            cleanupTestDb(manager);
            }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
        finally {
            FACTORY.setFuzzyMatchScorer(oldScorer);
        }
    }

    public void testGetAllTuData(TM3Tm<TestData> tm, TestLocale srcLocale, TestLocale tgtLocale) 
                        throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Populate the TM with a bunch of stuff
            for (int i = 0; i < 1000; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                        new TestData(Integer.toString(i)), TM3SaveMode.MERGE, currentTestEvent);
            }
            currentTransaction.commit();
            currentTransaction = currentSession.beginTransaction();
            
            // Verify the data without range restriction
            TM3Handle<TestData> handle = tm.getAllData(null, null);
            assertEquals("unexpected tu count", 1000, handle.getCount());
            Iterator<TM3Tu<TestData>> it = handle.iterator();
            for (int i = 0; i < 1000; i++) {
                assertTrue("No tu " + i, it.hasNext());
                assertEquals("Tu value mismatch", Integer.toString(i), it.next().getSourceTuv().getContent().data);
            }
            assertFalse("Too many TU returned", it.hasNext());
            handle.purge();
            assertEquals("purge failed", 0, handle.getCount());
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testGetAllTuDataWithDateRange(TM3Tm<TestData> tm, TestLocale srcLocale, TestLocale tgtLocale) 
            throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            
            // Make a couple fake events
            Date now = new Date();
            TM3Event first = tm.addEvent(0, "test", "first event");
            TM3Event second = tm.addEvent(0, "test", "second event");
            TM3Event third = tm.addEvent(0, "test", "third event");
            
            // Set the second and third timestamps in the future
            second.setTimestamp(new Date(second.getTimestamp().getTime() + 60 * 1000)); 
            third.setTimestamp(new Date(third.getTimestamp().getTime() + 120 * 1000));
            
            currentSession.flush();
            
            // Populate the TM with a bunch of stuff
            for (int i = 0; i < 350; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, first);
            }
            for (int i = 350; i < 710; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, second);
            }
            for (int i = 710; i < 1000; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, third);
            }
            currentTransaction.commit();
            currentTransaction = currentSession.beginTransaction();
            
            // The date range should encompass only the 'second' event
            Date start = new Date(now.getTime() + 30 * 1000);
            Date end = new Date(now.getTime() + 90 * 1000);
            
            TM3Handle<TestData> handle = tm.getAllData(start, end);
            assertEquals("unexpected tu count", 360, handle.getCount());
            Iterator<TM3Tu<TestData>> it = handle.iterator();
            for (int i = 350; i < 710; i++) {
                assertTrue("No tu " + i, it.hasNext());
                assertEquals("Tu value mismatch", Integer.toString(i), it.next().getSourceTuv().getContent().data);
            }
            assertFalse("Too many TU returned", it.hasNext());
            
            handle.purge();
            assertEquals("purge failed", 0, handle.getCount());
            
            // Now get a handle to everything
            
            handle = tm.getAllData(null, null);
            assertEquals("unexpected post-purge tu count", 640, handle.getCount());
            
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testGetTuDataByLocale(TM3Tm<TestData> tm, TestLocale srcLocale, TestLocale tgtLocale) 
                throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Populate the TM with a bunch of stuff
            for (int i = 0; i < 1000; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, currentTestEvent);
            }
            currentTransaction.commit();
            currentTransaction = currentSession.beginTransaction();
            
            // Verify the data without range restriction
            TM3Handle<TestData> handle = tm.getDataByLocale(tgtLocale, null, null);
            assertEquals("unexpected tu count", 1000, handle.getCount());
            Iterator<TM3Tu<TestData>> it = handle.iterator();
            for (int i = 0; i < 1000; i++) {
                assertTrue("No tu " + i, it.hasNext());
                assertEquals("Tu value mismatch", Integer.toString(i), it.next().getSourceTuv().getContent().data);
            }
            assertFalse("Too many TU returned", it.hasNext());
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    public void testGetTuDataByLocaleWithDateRange(TM3Tm<TestData> tm, TestLocale srcLocale, TestLocale tgtLocale) 
            throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            
            // Make a couple fake events
            Date now = new Date();
            TM3Event first = tm.addEvent(0, "test", "first event");
            TM3Event second = tm.addEvent(0, "test", "second event");
            TM3Event third = tm.addEvent(0, "test", "third event");
            
            // Set the second and third timestamps in the future
            second.setTimestamp(new Date(second.getTimestamp().getTime() + 60 * 1000)); 
            third.setTimestamp(new Date(third.getTimestamp().getTime() + 120 * 1000));
            
            currentSession.flush();
            
            // Populate the TM with a bunch of stuff
            for (int i = 0; i < 350; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, first);
            }
            for (int i = 350; i < 710; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, second);
            }
            for (int i = 710; i < 1000; i++) {
                tm.save(srcLocale, new TestData(Integer.toString(i)), TM3Attributes.NONE, tgtLocale, 
                    new TestData(Integer.toString(i)), TM3SaveMode.MERGE, third);
            }
            currentTransaction.commit();
            currentTransaction = currentSession.beginTransaction();
            
            // The date range should encompass only the 'second' event
            Date start = new Date(now.getTime() + 30 * 1000);
            Date end = new Date(now.getTime() + 90 * 1000);
            
            TM3Handle<TestData> handle = tm.getDataByLocale(tgtLocale, start, end);
            assertEquals("unexpected tu count", 360, handle.getCount());
            Iterator<TM3Tu<TestData>> it = handle.iterator();
            for (int i = 350; i < 710; i++) {
                assertTrue("No tu " + i, it.hasNext());
                assertEquals("Tu value mismatch", Integer.toString(i), it.next().getSourceTuv().getContent().data);
            }
            assertFalse("Too many TU returned", it.hasNext());
            
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testGetTuDataById(TM3Tm<TestData> tm, TestLocale srcLocale, TestLocale tgtLocale) 
                        throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            
            TM3Event event = tm.addEvent(0, "test", "event");
            currentSession.flush();
            
            // Populate the TM with a bunch of stuff
            List<Long> ids = new ArrayList<Long>();
            for (int i = 0; i < 20; i++) {
                TM3Tu<TestData> tu = tm.save(srcLocale, new TestData(Integer.toString(i)), 
                    TM3Attributes.NONE, tgtLocale, new TestData(Integer.toString(i)), 
                    TM3SaveMode.MERGE, event);
                if (tu.getId() % 2 == 0) {
                    ids.add(tu.getId());
                }
            }
            currentTransaction.commit();
            currentTransaction = currentSession.beginTransaction();
            
            TM3Handle<TestData> handle = tm.getDataById(ids);
            
            assertEquals("unexpected tu count", ids.size(), handle.getCount());

            for (TM3Tu<TestData> tu : handle) {
                assertTrue("unexpected tu id " + tu.getId(), tu.getId() % 2 == 0);
                assertTrue("unexpected tu id " + tu.getId(), ids.contains(tu.getId()));
            }
            
            handle.purge();
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            
            int actualCount = 0;
            for (TM3Tu<TestData> tu : tm.getDataById(ids)) {
                actualCount++;
            }
            
            assertEquals("failed to delete " + actualCount + " tu", 0, actualCount);
            
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    // Engineer a 2-thread race to insert the same source TUV with separate target TUVs.
    // Ensure that this only produes a single TU.
    public void testLockOnSave(final TM3Tm<TestData> testTm, final TestLocale srcLocale, final TestLocale tgtLocale)
                throws Exception {

        final TM3Event event = testTm.addEvent(1, "test", null);
        final TM3Attribute attr = testTm.addAttribute("test");
        try {
            currentTransaction = currentSession.beginTransaction();
            currentSession.flush();
            currentTransaction.commit();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (currentTransaction != null) {
                if (currentTransaction.isActive()) {
                    currentTransaction.rollback();
                }
            }
        }
        
        class Saver extends TesterThread {
            Saver(int id) {
                super(id);
            }
            @Override
            public void test(Session session, Transaction currentTransaction) throws Exception {
                TM3Tm<TestData> tm = manager.getTm(session, FACTORY, testTm.getId());
                tm.createSaver()
                  .tu(new TestData("src1"), srcLocale, event)
                  .attr(attr, "value")
                  .target(new TestData("tgt" + getId()), tgtLocale, event)
                  .save(TM3SaveMode.MERGE);
                // Pause so whoever got here first will wait for the other
                // thread to also start trying to save
                Thread.sleep(250);
            }
        }
        
        Thread t1 = new Thread(new Saver(1));
        Thread t2 = new Thread(new Saver(2));
        t1.start();
        t2.start();
        Thread.sleep(2000);
        
        List<TM3Tu<TestData>> alltu = collect(testTm.getAllData(null, null).iterator());
        assertEquals(1, alltu.size());
        List<TM3Tuv<TestData>> alltuv = alltu.get(0).getAllTuv();
        assertEquals(3, alltuv.size());
    }
    
    // Create a TU with a single translation.  Then race to add a second translation while 
    // also changing the original translation to have the save content as the proposed second one.
    public void testLockOnModify(final TM3Tm<TestData> testTm, final TestLocale srcLocale, final TestLocale tgtLocale)
            throws Exception {
        
        final TM3Event event = testTm.addEvent(1, "test", null);
        final TestData srcData = new TestData("src1");
        final TestData origTgtData = new TestData("dst1");
        final TestData newTgtData = new TestData("dst2");
        
        try {
            currentTransaction = currentSession.beginTransaction();
            testTm.createSaver()
                .tu(srcData, srcLocale, event)
                .target(origTgtData, tgtLocale, event)
                .save(TM3SaveMode.MERGE);
            currentSession.flush();
            currentTransaction.commit();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (currentTransaction != null) {
                if (currentTransaction.isActive()) {
                    currentTransaction.rollback();
                }
            }
        }
        
        class Saver extends TesterThread {
            Saver(int id) {
                super(id);
            }
            @Override
            public void test(Session session, Transaction tx) throws Exception {
                TM3Tm<TestData> tm = manager.getTm(session, FACTORY, testTm.getId());
                tm.createSaver()
                  .tu(srcData, srcLocale, event)
                  .target(newTgtData, tgtLocale, event)
                  .save(TM3SaveMode.MERGE);
                // Pause so whoever got here first will wait for the other
                // thread to also start trying to save
                Thread.sleep(250);
            }
        }
        
        class Modifier extends TesterThread {
            Modifier(int id) { 
                super(id);
            }
            @Override
            public void test(Session session, Transaction tx) throws Exception {
                TM3Tm<TestData> tm = manager.getTm(session, FACTORY, testTm.getId());
                TM3LeverageResults<TestData> results = tm.findMatches(srcData,
                        srcLocale, Collections.singleton(tgtLocale), 
                        TM3Attributes.NONE, TM3MatchType.EXACT, false);
                assertEquals(1, results.getMatches().size());
                TM3Tu<TestData> tu = results.getMatches().first().getTu();
                assertEquals(srcData, tu.getSourceTuv().getContent());
                getLocaleTuv(tu, tgtLocale).setContent(newTgtData);
                tm.modifyTu(tu, event);
                // Pause so whoever got here first will wait for the other
                // thread to also start trying to save
                Thread.sleep(250);
            }
        }
        
        Thread t1 = new Thread(new Modifier(1));
        Thread t2 = new Thread(new Saver(2));
        t2.start();
        t1.start();
        Thread.sleep(2000);
        
        List<TM3Tu<TestData>> alltu = collect(testTm.getAllData(null, null).iterator());
        assertEquals(1, alltu.size());
        List<TM3Tuv<TestData>> alltuv = alltu.get(0).getAllTuv();
        assertEquals(2, alltuv.size());
        assertEquals(newTgtData, getLocaleTuv(alltu.get(0), tgtLocale).getContent());
    }

    
    /**
     * Runnable to perform an action inside a session transaction.
     */
    abstract class TesterThread implements Runnable {
        private int id;
        TesterThread(int id) {
            this.id = id;
        }
        @Override
        public void run() {
            Session session = sessionFactory.openSession();
            Transaction tx = null;
            try {
                tx = session.beginTransaction();
                test(session, tx);
                tx.commit();
            }
            catch (Exception e) { 
                throw new RuntimeException(e);
            }
            finally {
                if (tx != null) {
                    if (tx.isActive()) {
                        tx.rollback();
                    }
                }
            }
        }
        public int getId() {
            return id;
        }
        abstract void test(Session session, Transaction tx) throws Exception;
    }
    
    private <T> List<T> collect(Iterator<T> it) {
        List<T> l = new ArrayList<T>();
        while (it.hasNext()) {
            l.add(it.next());
        }
        return l;
    }

    public void testMultipleTargets(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            // make sure both target tuvs are there
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(2, targetTuvs.size());
            assertTrue(
                (targetTuvs.get(0).getContent().equals(tgtData1) &&
                 targetTuvs.get(1).getContent().equals(tgtData2)) ||
                (targetTuvs.get(0).getContent().equals(tgtData2) &&
                 targetTuvs.get(1).getContent().equals(tgtData1)));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testIdenticalTargets(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            // make sure there is only one target tuv
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(1, targetTuvs.size());
            assertTrue(targetTuvs.get(0).getContent().equals(tgtData1));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testMerge(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure both target tuvs are there
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(2, targetTuvs.size());
            assertTrue(
                (targetTuvs.get(0).getContent().equals(tgtData1) &&
                 targetTuvs.get(1).getContent().equals(tgtData2)) ||
                (targetTuvs.get(0).getContent().equals(tgtData2) &&
                 targetTuvs.get(1).getContent().equals(tgtData1)));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testMergeIdentical(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure there is only one target tuv
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(1, targetTuvs.size());
            assertTrue(targetTuvs.get(0).getContent().equals(tgtData1));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testMergeWithIdenticalTargets(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure there are just two target tuvs
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(2, targetTuvs.size());
            assertTrue(
                (targetTuvs.get(0).getContent().equals(tgtData1) &&
                 targetTuvs.get(1).getContent().equals(tgtData2)) ||
                (targetTuvs.get(0).getContent().equals(tgtData2) &&
                 targetTuvs.get(1).getContent().equals(tgtData1)));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    public void testOverwrite(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 1");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.OVERWRITE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure there is only one target tuv
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(1, targetTuvs.size());
            assertTrue(targetTuvs.get(0).getContent().equals(tgtData2));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    // not all old targets were being removed due to a bug in
    // TM3Tu.removeTargetTuvByLocale
    public void testOverwriteOfMultipleTargets(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData3, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.OVERWRITE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure there is only one target tuv
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(1, targetTuvs.size());
            assertTrue(targetTuvs.get(0).getContent().equals(tgtData3));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    // not all old targets were being removed due to a bug in
    // TM3Tu.removeTargetTuvByLocale
    public void testOverwriteOfMultipleTargetsWithIdentical(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.OVERWRITE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure both target tuvs are there
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(2, targetTuvs.size());
            assertTrue(
                (targetTuvs.get(0).getContent().equals(tgtData1) &&
                 targetTuvs.get(1).getContent().equals(tgtData2)) ||
                (targetTuvs.get(0).getContent().equals(tgtData2) &&
                 targetTuvs.get(1).getContent().equals(tgtData1)));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    // this used to fail because with every new target, we removed all existing
    // targets in that locale, even if they were also new
    public void testOverwriteWithMultipleTargets(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .target(tgtData3, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.OVERWRITE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure both target tuvs are there
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(2, targetTuvs.size());
            assertTrue(
                (targetTuvs.get(0).getContent().equals(tgtData2) &&
                 targetTuvs.get(1).getContent().equals(tgtData3)) ||
                (targetTuvs.get(0).getContent().equals(tgtData3) &&
                 targetTuvs.get(1).getContent().equals(tgtData2)));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    // This used to crash because we didn't check for null when adding the
    // second identical target, so tried to save a null tuv
    public void testOverwriteWithIdenticalTargets(TM3Tm<TestData> tm, TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = new TestData("This is source 1");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.OVERWRITE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure there is only one target tuv
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(1, targetTuvs.size());
            assertTrue(targetTuvs.get(0).getContent().equals(tgtData2));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
    // In GlobalSight, it's possible to have logically identical TUVs with content
    // that is not string-identical (because of optional inline XML attributes).  
    // We simulate with a test here.
    public void testFixedFingerprintTuvs(TestLocale srcLocale, 
            final TestLocale tgtLocale) throws Exception {
        FixedValueTestDataFactory factory = new FixedValueTestDataFactory(2L);
        TM3Tm<TestData> tm = manager.getTm(currentSession, factory, currentTestId);
        try {
            currentTransaction = currentSession.beginTransaction();
            // Now let's create some segments.
            TestData srcData1 = 
                new TestData("This is source 1");
            // These two targets are "different", but they are logically the same
            // in the opinion of the TM3Data implementation
            TestData tgtData1 = new FixedValueTestData("This is target 1", 2L);
            TestData tgtData2 = new FixedValueTestData("This is target 2", 2L);
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();
            
            currentTransaction = currentSession.beginTransaction();
            saver = tm.createSaver();
            saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            // make sure there is only one target tuv
            TM3LeverageResults<TestData> results =
                tm.findMatches(srcData1, srcLocale, null,  TM3Attributes.NONE, TM3MatchType.EXACT, false);
            expectResults(results, expected(srcData1, true));
            TM3Tu<TestData> tu = results.getMatches().first().getTu();
            List<TM3Tuv<TestData>> targetTuvs = tu.getLocaleTuvs(tgtLocale);
            assertEquals(1, targetTuvs.size());
            assertTrue(targetTuvs.get(0).getContent().equals(tgtData1));
            currentTransaction.commit();

            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }

    /**
     * Make sure that we don't skip TUVs or return them in the wrong order
     * when using getDataByLocale().  (GBS-2328).
     */
     public void testDataByLocaleOrdering(TM3Tm<TestData> tm, 
            TestLocale srcLocale, TestLocale tgtLocale1, 
            TestLocale tgtLocale2) throws Exception {

        try {
            currentTransaction = currentSession.beginTransaction();
            // Create three segments in such a way that the TUVs for 
            // French are out of order relative to the TUs.
            TestData srcData1 = 
                new TestData("This is source 1");
            TestData srcData2 = 
                new TestData("This is source 2");
            TestData srcData3 = 
                new TestData("This is source 3");
            TestData tgtData1 = new TestData("This is target 1");
            TestData tgtData2 = new TestData("This is target 2");
            TestData tgtData3 = new TestData("This is target 3");

            TM3Saver<TestData> saver = tm.createSaver();
            // TU 1, en -> fr tuv
            TM3Tu<TestData> tu1 = 
                saver.tu(srcData1, srcLocale, currentTestEvent)
                 .target(tgtData1, tgtLocale1, currentTestEvent)
                 .save(TM3SaveMode.MERGE)
                 .get(0);
            currentSession.flush();
            // TU 2, en -> de tuv
            saver = tm.createSaver();
            TM3Tu<TestData> tu2 = 
                saver.tu(srcData2, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale2, currentTestEvent)
                 .save(TM3SaveMode.MERGE)
                 .get(0);
            currentSession.flush();
            // TU 3, en -> fr tuv
            saver = tm.createSaver();
            TM3Tu<TestData> tu3 = 
                saver.tu(srcData3, srcLocale, currentTestEvent)
                 .target(tgtData3, tgtLocale1, currentTestEvent)
                 .save(TM3SaveMode.MERGE)
                 .get(0);
            currentSession.flush();
            // now go back to tu 2 and add an en -> fr tuv
            saver.tu(srcData2, srcLocale, currentTestEvent)
                 .target(tgtData2, tgtLocale1, currentTestEvent)
                 .save(TM3SaveMode.MERGE);

            currentSession.flush();
            currentTransaction.commit();
            currentTransaction = currentSession.beginTransaction();

            // now when we ask for all fr data, we should get tuvs from 
            // tus 1, 2, and 3 in that order.
            TM3Handle<TestData> handle = 
                        tm.getDataByLocale(tgtLocale1, null, null);
            assertEquals("unexpected tu count", 3, handle.getCount());
            ((LocaleDataHandle)handle).setIncrement(1);
            Iterator<TM3Tu<TestData>> it = handle.iterator();
            TM3Tu<TestData> tu = it.next();
            assertNotNull(tu);
            assertEquals(tu1.getId(), tu.getId());
            assertEquals(srcData1, tu.getSourceTuv().getContent());
            assertTrue(it.hasNext());
            tu = it.next();
            assertNotNull(tu);
            assertEquals(tu2.getId(), tu.getId());
            assertEquals(srcData2, tu.getSourceTuv().getContent());
            assertTrue(it.hasNext());
            tu = it.next();
            assertNotNull(tu);
            assertEquals(tu3.getId(), tu.getId());
            assertEquals(srcData3, tu.getSourceTuv().getContent());
            assertFalse("Too many TU returned", it.hasNext());
            currentTransaction.commit();
            cleanupTestDb(manager);
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
    
     public void testFuzzyLookupTargetOnlySearchesTargets(TM3Tm<TestData> tm, 
             TestLocale srcLocale, TestLocale tgtLocale1, 
             TestLocale tgtLocale2) throws Exception {

         try {
             currentTransaction = currentSession.beginTransaction();
             
             // Create a pair of TUs:
             //   A --> B
             //   C --> A
             // Then do a reverse (target) fuzzy lookup for "A" and make sure
             // that only C is found, not B.
             // 
             TestData dataA = 
                 new TestData("A B C D E F G");
             TestData dataB = 
                 new TestData("H I J K L M");
             TestData dataC = 
                 new TestData("N O P Q R S");
             tm.setIndexTarget(true);
             TM3Saver<TestData> saver = tm.createSaver();
             saver.tu(dataA, srcLocale, currentTestEvent)
                  .target(dataB, tgtLocale1, currentTestEvent);
             saver.tu(dataC, srcLocale, currentTestEvent)
                  .target(dataA, tgtLocale1, currentTestEvent);
             saver.save(TM3SaveMode.MERGE);
             currentSession.flush();
             currentTransaction.commit();
             currentTransaction = currentSession.beginTransaction();

             TestData fuzzyKey = new TestData("A B C D E F Z");
             TM3LeverageResults<TestData> results = 
                     tm.findMatches(fuzzyKey, tgtLocale1, Collections.singleton(srcLocale), 
                     null, TM3MatchType.ALL, true);
             assertEquals(1, results.getMatches().size());
             TM3LeverageMatch<TestData> m = results.getMatches().first();
             assertEquals(dataA, m.getTuv().getContent());
             assertEquals(dataC, m.getTu().getSourceTuv().getContent());
             currentTransaction.commit();
             cleanupTestDb(manager);
         }
         catch (Exception e) {
             currentTransaction.rollback();
             throw e;
         }
     }

     
    // when it's safe to assume at most one tuv per locale
    private static <T extends TM3Data> TM3Tuv<T> getLocaleTuv(TM3Tu<T> tu, TM3Locale locale) {
        List<TM3Tuv<T>> tuvs = tu.getLocaleTuvs(locale);
        if (tuvs.size() == 0) {
            return null;
        } else if (tuvs.size() == 1) {
            return tuvs.get(0);
        } else {
            throw new RuntimeException("unexpected multiple target tuvs");
        }
    }
    
    public static SessionFactory setupHibernate() {
        long start = System.currentTimeMillis();
        Properties props = new Properties();
        try {
            props.load(TM3Tests.class.getResourceAsStream("/test.properties"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Configuration cfg = new Configuration().addProperties(props);
        cfg = HibernateConfig.extendConfiguration(cfg);
        cfg = new TestDataFactory().extendConfiguration(cfg);
        SessionFactory sessionFactory = cfg.buildSessionFactory();
        System.out.println("Hibernate initialization took " + 
                           (System.currentTimeMillis() - start) + "ms");
        return sessionFactory;
    }


}
