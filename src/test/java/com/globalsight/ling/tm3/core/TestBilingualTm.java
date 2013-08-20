package com.globalsight.ling.tm3.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBilingualTm extends TM3Tests {

    @BeforeClass
    public static void setup() throws Exception {
        init();
    }
    
    // Set up a bilingual TM for each test, start a fresh hibernate session, etc
    @Before
    @Override
    public void beforeTest() throws Exception {
        super.beforeTest();
        Transaction tx = null;
        try {
            tx = currentSession.beginTransaction();
            System.out.println("Creating TM id " + currentTestId);
            TM3Tm<TestData> tm = manager.createBilingualTm(FACTORY, inlineAttrs(), EN_US, FR_FR);
            currentSession.flush();
            currentTestId = tm.getId();
            currentTestEvent = tm.addEvent(0, "test", "test " + currentTestId);
            tx.commit();

        }
        catch (Exception e) {
            if (tx != null) {
                tx.rollback();
            }
            throw e;
        }
    }
  
    @After
    public void afterTest() throws Exception {
        super.afterTest();
    }
        
    @Test
    public void testCreateBilingualTm() throws Exception {
        Transaction tx = null;
        try {
            TM3Tm<TestData> tm2 = manager.getTm(FACTORY, currentTestId);
            assertNotNull(tm2);
            assertTrue(tm2 instanceof TM3BilingualTm);
            cleanupTestDb(manager);
            
            TM3Tm<TestData> tm3 = manager.getTm(FACTORY, currentTestId);
            assertNull(tm3);
        }
        catch (Exception e) {
            tx.rollback();
            throw e;
        }
    }

    @Test(expected=TM3Exception.class)
    public void testBadLocale() throws Exception {
        testBadLocale(
                manager.getTm(FACTORY, currentTestId), EN_US, FR_FR, DE_DE);
    }

    /**
     * An EN_US -> FR_FR bilingual TM should not accept DE_DE data.
     */
    public void testBadLocale(TM3Tm<TestData> tm,
           TestLocale srcLocale, TestLocale goodTgtLocale,
           TestLocale badTgtLocale) throws Exception {
        try {
            currentTransaction = currentSession.beginTransaction();
            tm.setIndexTarget(true);
            TM3Saver<TestData> saver = tm.createSaver();
            saver.tu(fuzzyData1, srcLocale, currentTestEvent)
                 .target(fuzzyData2, badTgtLocale, currentTestEvent)
                 .save(TM3SaveMode.MERGE);
            currentTransaction.commit();

            currentTransaction = currentSession.beginTransaction();
            TM3LeverageResults<TestData> results;

            // exact match
            verifyExact(tm, fuzzyData1, srcLocale,
                            fuzzyData2, badTgtLocale, true);

            // exact target match
            // this would fail
            //verifyExact(tm, fuzzyData2, badTgtLocale,
            //                fuzzyData1, srcLocale, true);

            // fuzzy match
            results = tm.findMatches(
               fuzzyKey1, srcLocale, null, null, TM3MatchType.ALL, true);
            expectResults(results, expected(fuzzyData1, false));

            // fuzzy target match
            // If we lie about the keyLocale, this succeeds!
            results = tm.findMatches(
               fuzzyKey2, goodTgtLocale, null, null, TM3MatchType.ALL, true);
            expectResults(results, expected(fuzzyData2, false));

            throw new TM3Exception("What, we made it through all that???");
        }
        catch (Exception e) {
            currentTransaction.rollback();
            throw e;
        }
    }
}
